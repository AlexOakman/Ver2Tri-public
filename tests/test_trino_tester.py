from datetime import date
import json
import re
from types import SimpleNamespace

import pytest

from config import settings
from core.state_manager import StateManager
from core.trino_tester import (
    HeaderParser,
    ParameterResolver,
    RawRepairLLMClient,
    RuntimeExecutionState,
    SQLLineEdit,
    SQLPatchApplier,
    TrinoRuntimeTester,
    TrinoRuntimeError,
    TrinoSQLPreparer,
)
from core.migration_knowledge import RepairPatchGuard


class FakeRawRepairClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.prompts = []

    def complete(self, prompt):
        self.prompts.append(prompt)
        if not self.responses:
            raise AssertionError("No fake raw repair responses left")
        return self.responses.pop(0)


def _state_with_trino_part(tmp_path, query_name="dma_demo", part_num=0, sql="SELECT 1"):
    workflow_root = tmp_path / "workflow"
    state_manager = StateManager(query_name, base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(part_num + 1)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.trino_parts_path / f"{query_name}_part_{part_num}_trino.sql").write_text(sql, encoding="utf-8")
    return state_manager


def test_sql_patch_applier_replace_line_succeeds():
    old_sql = "SELECT\n    CAST(date_create AS DATE),\n    id\n"

    patched = SQLPatchApplier.apply(
        old_sql,
        [
            SQLLineEdit(
                op="replace_line",
                line=2,
                old="    CAST(date_create AS DATE),",
                new="    CAST(date_create AS DATE) AS date_create,",
            )
        ],
    )

    assert patched == "SELECT\n    CAST(date_create AS DATE) AS date_create,\n    id\n"


def test_sql_patch_applier_rejects_stale_replace_line():
    with pytest.raises(TrinoRuntimeError, match="old mismatch"):
        SQLPatchApplier.apply(
            "SELECT id\n",
            [SQLLineEdit(op="replace_line", line=1, old="SELECT missing", new="SELECT id")],
        )


def test_sql_patch_applier_replace_range_is_inclusive():
    patched = SQLPatchApplier.apply(
        "SELECT\n    a,\n    b,\n    c\n",
        [
            SQLLineEdit(
                op="replace_range",
                start_line=2,
                end_line=3,
                old_lines=["    a,", "    b,"],
                new_lines=["    a AS a,", "    b AS b,"],
            )
        ],
    )

    assert patched == "SELECT\n    a AS a,\n    b AS b,\n    c\n"


def test_sql_patch_applier_insert_after_line_supports_file_start():
    patched = SQLPatchApplier.apply(
        "SELECT id",
        [SQLLineEdit(op="insert_after_line", after_line=0, new_lines=["-- generated fix"])],
    )

    assert patched == "-- generated fix\nSELECT id"


def test_sql_patch_applier_multiple_edits_are_stable_against_line_shifts():
    patched = SQLPatchApplier.apply(
        "line1\nline2\nline3\n",
        [
            SQLLineEdit(op="replace_line", line=3, old="line3", new="line3 fixed"),
            SQLLineEdit(op="insert_after_line", after_line=1, new_lines=["inserted"]),
        ],
    )

    assert patched == "line1\ninserted\nline2\nline3 fixed\n"


def test_parse_final_fix_accepts_line_edits_and_preserves_journal_fields(tmp_path):
    state_manager = _state_with_trino_part(
        tmp_path,
        sql="SELECT\n    CAST(date_create AS DATE),\n    id\n",
    )
    tester = TrinoRuntimeTester(state_manager)

    result = tester._parse_final_fix(
        json.dumps(
            {
                "target_part": 0,
                "change_type": "line_patch",
                "edits": [
                    {
                        "op": "replace_line",
                        "line": 2,
                        "old": "    CAST(date_create AS DATE),",
                        "new": "    CAST(date_create AS DATE) AS date_create,",
                    }
                ],
                "summary": "Added CTAS alias",
                "confidence": 0.9,
                "used_evidence": ["MISSING_COLUMN_NAME points to line 2"],
            }
        ),
        default_part=0,
    )

    assert result.change_type == "line_patch"
    assert result.summary == "Added CTAS alias"
    assert result.confidence == 0.9
    assert result.used_evidence == ["MISSING_COLUMN_NAME points to line 2"]
    assert result.edits[0].op == "replace_line"
    assert "AS date_create" in result.fixed_sql


def test_line_edit_repair_save_keeps_guard_and_journal_metadata(tmp_path):
    state_manager = _state_with_trino_part(
        tmp_path,
        sql="CREATE TABLE demo AS\nSELECT\n    CAST(date_create AS DATE),\n    id\nFROM source\n",
    )
    tester = TrinoRuntimeTester(state_manager)
    final_result = tester._parse_final_fix(
        json.dumps(
            {
                "target_part": 0,
                "change_type": "line_patch",
                "edits": [
                    {
                        "op": "replace_line",
                        "line": 3,
                        "old": "    CAST(date_create AS DATE),",
                        "new": "    CAST(date_create AS DATE) AS date_create,",
                    }
                ],
                "summary": "Added CTAS alias",
                "confidence": 0.9,
                "used_evidence": ["Trino MISSING_COLUMN_NAME points to CAST expression without alias"],
            }
        ),
        default_part=0,
    )
    report = {"event_log": [{"session_id": "session-1"}], "error": "MISSING_COLUMN_NAME"}

    saved_path, guard_result = tester._save_guarded_fix(
        0,
        final_result.fixed_sql,
        report=report,
        source_stage="repair_agent",
        root_failed_part=0,
        attempt_num=1,
        final_result=final_result,
    )

    assert guard_result["accepted"]
    assert saved_path.read_text(encoding="utf-8").count("AS date_create") == 1
    event_details = report["event_log"][-1]["details"]
    assert event_details["change_type"] == "line_patch"
    assert event_details["edit_summary"] == [{"op": "replace_line", "line": 3, "changed_lines": 1}]
    journal = (state_manager.load_state()["knowledge"]["fix_attempt_journal"])[-1]
    assert journal["change_type"] == "line_patch"
    assert journal["confidence"] == 0.9
    assert journal["used_evidence"] == ["Trino MISSING_COLUMN_NAME points to CAST expression without alias"]


def test_parse_final_fix_accepts_legacy_full_rewrite(tmp_path):
    state_manager = _state_with_trino_part(tmp_path, sql="SELECT bad")
    tester = TrinoRuntimeTester(state_manager)

    result = tester._parse_final_fix(
        json.dumps({"target_part": 0, "fixed_sql": "SELECT 1", "summary": "rewrote broken part"}),
        default_part=0,
    )

    assert result.change_type == "full_rewrite"
    assert result.fixed_sql == "SELECT 1"
    assert result.edits == []


def test_parse_final_fix_rejects_ambiguous_edits_and_fixed_sql(tmp_path):
    state_manager = _state_with_trino_part(tmp_path, sql="SELECT bad")
    tester = TrinoRuntimeTester(state_manager)

    with pytest.raises(TrinoRuntimeError, match="exactly one"):
        tester._parse_final_fix(
            json.dumps(
                {
                    "target_part": 0,
                    "edits": [{"op": "replace_line", "line": 1, "old": "SELECT bad", "new": "SELECT 1"}],
                    "fixed_sql": "SELECT 1",
                }
            ),
            default_part=0,
        )


def test_parse_final_fix_rejects_unknown_edit_op(tmp_path):
    state_manager = _state_with_trino_part(tmp_path, sql="SELECT bad")
    tester = TrinoRuntimeTester(state_manager)

    with pytest.raises(TrinoRuntimeError, match="Unknown SQL line edit op"):
        tester._parse_final_fix(
            json.dumps({"target_part": 0, "edits": [{"op": "delete_file"}]}),
            default_part=0,
        )


def test_read_trino_part_lines_returns_one_based_numbered_content(tmp_path):
    state_manager = _state_with_trino_part(tmp_path, sql="line1\nline2\nline3\n")
    tester = TrinoRuntimeTester(state_manager)

    result = tester._read_trino_part_lines(0, start_line=2, end_line=3)

    assert result["lines"] == [{"line": 2, "text": "line2"}, {"line": 3, "text": "line3"}]
    assert result["content"] == "2 | line2\n3 | line3"


def test_read_trino_part_lines_clamps_end_line_to_file_length(tmp_path):
    state_manager = _state_with_trino_part(tmp_path, sql="line1\nline2\nline3\n")
    tester = TrinoRuntimeTester(state_manager)

    result = tester._read_trino_part_lines(0, start_line=1, end_line=30)

    assert result["start_line"] == 1
    assert result["end_line"] == 3
    assert result["content"] == "1 | line1\n2 | line2\n3 | line3"


def test_runtime_fix_budget_resets_for_new_error_in_same_part(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "trino_schema", "runtime_schema")
    monkeypatch.setattr(settings, "trino_test_max_fix_iterations", 1)

    workflow_root = tmp_path / "workflow"
    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.trino_parts_path / "dma_demo_part_0_trino.sql").write_text("SELECT 1", encoding="utf-8")

    tester = TrinoRuntimeTester(state_manager, connection=SimpleNamespace())

    monkeypatch.setattr(tester, "_reassemble_artifacts", lambda: None)
    monkeypatch.setattr(tester, "_load_header", lambda: HeaderParser.parse("/* @header\ndatamart: analytics_src.demo\n*/"))
    monkeypatch.setattr(tester, "_load_latest_parts", lambda: {0: "SELECT 1"})
    monkeypatch.setattr(tester, "_build_runtime_execution_state", lambda header, parts: RuntimeExecutionState())
    monkeypatch.setattr(tester, "_build_preparer", lambda header, parts: SimpleNamespace())
    monkeypatch.setattr(tester, "_drop_runtime_tables", lambda connection, tables: None)
    monkeypatch.setattr(tester, "_write_report", lambda report: None)
    monkeypatch.setattr(tester, "_update_runtime_status", lambda **kwargs: None)
    monkeypatch.setattr(tester, "_invalidate_from_part", lambda **kwargs: 0)

    execute_results = iter(
        [
            {"success": False, "error": "TrinoUserError(name=MISSING_COLUMN_NAME, query_id=abc123)"},
            {"success": False, "error": "TrinoUserError(name=TABLE_NOT_FOUND, query_id=def456)"},
            {"success": True},
        ]
    )
    monkeypatch.setattr(tester, "_execute_single_part", lambda **kwargs: next(execute_results))

    fixed_path = tmp_path / "fixed.sql"
    fixed_path.write_text("SELECT 1", encoding="utf-8")
    seen_attempts = []

    def fake_fix_part(
        connection,
        part_num,
        error_text,
        header,
        part_sql_by_num,
        runtime_state,
        preparer,
        resolver,
        report,
        attempt_num,
    ):
        seen_attempts.append((error_text, attempt_num))
        return {"path": fixed_path, "target_part": 0, "summary": error_text}

    monkeypatch.setattr(tester, "_fix_part", fake_fix_part)

    success, report = tester.run()

    assert success is True
    assert seen_attempts == [
        ("TrinoUserError(name=MISSING_COLUMN_NAME, query_id=abc123)", 1),
        ("TrinoUserError(name=TABLE_NOT_FOUND, query_id=def456)", 1),
    ]
    assert [item["attempt"] for item in report["runtime_fix_attempts"]] == [1, 1]


def test_runtime_state_maps_insert_target_to_create_part_only(tmp_path):
    state_manager = StateManager("dma_demo", base_path=tmp_path / "workflow" / "in_progress")
    state_manager.initialize()
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester.state_manager = state_manager
    tester.runtime_schema = "runtime_schema"
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.foo\nkeys:\n  - id\n*/")
    parts = {
        0: "CREATE TABLE sandbox.foo_trino (id BIGINT);",
        21: "INSERT INTO sandbox.foo_trino SELECT id FROM tmp_stage;",
    }

    runtime_state = tester._build_runtime_execution_state(header, parts)

    assert runtime_state.expected_tables_by_part[0] == {"foo_trino"}
    assert runtime_state.expected_tables_by_part[21] == set()
    assert runtime_state.table_to_creator_part["foo_trino"] == 0


def test_runtime_state_ignores_execution_order_hint_without_create(tmp_path):
    state_manager = StateManager("dma_demo", base_path=tmp_path / "workflow" / "in_progress")
    state_manager.initialize()
    state_manager.update_section(
        "parts",
        {
            "dependencies": {
                "part_4": {
                    "interpolate_dependency": [],
                    "table_dependency": [],
                    "execution_order_hint": ["gos_client"],
                }
            }
        },
    )
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester.state_manager = state_manager
    tester.runtime_schema = "runtime_schema"
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.foo\nkeys:\n  - id\n*/")
    parts = {
        4: "WITH gos_client AS (SELECT 1 AS id) SELECT * FROM gos_client;",
    }

    runtime_state = tester._build_runtime_execution_state(header, parts)

    assert runtime_state.expected_tables_by_part[4] == set()
    assert runtime_state.runtime_tables_created == set()
    assert "gos_client" not in runtime_state.table_to_creator_part


def test_header_parser_extracts_datamart_keys_params_and_defaults():
    sql = """/* @header
datamart: source.analytics_user_rollup
type: FULL_REFRESH
keys:
  - user_id
  - ab_test_name
  - test_date
params:
  - name: launch_id
  - name: actual_date
    default: $TODAY[-1]
scheduled: true
engine: Vertica
unit: Public Analytics
*/
CREATE TABLE sandbox.analytics_user_rollup(user_id int);
"""

    header = HeaderParser.parse(sql)

    assert header.datamart == "source.analytics_user_rollup"
    assert header.datamart_schema == "source"
    assert header.datamart_table == "analytics_user_rollup"
    assert header.target_table == "analytics_user_rollup_trino"
    assert header.keys == ["user_id", "ab_test_name", "test_date"]
    assert set(header.params) == {"launch_id", "actual_date"}
    assert header.params["actual_date"]["default"] == "$TODAY[-1]"


def test_schema_rewriter_preserves_final_datamart_target_schema():
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.foo\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    sql = """
CREATE TABLE sandbox.foo_trino AS
SELECT * FROM analytics_src.foo
JOIN dict.bar ON true;
INSERT INTO sandbox.foo_trino
SELECT * FROM analytics_src.foo;
"""

    preparer.discover_runtime_targets([sql])
    rewritten = preparer.rewrite_part_sql(sql)

    assert "CREATE TABLE sandbox.foo_trino AS" in rewritten
    assert "INSERT INTO sandbox.foo_trino" in rewritten
    assert "FROM analytics_src.foo" in rewritten
    assert "JOIN dict.bar" in rewritten
    assert "runtime_schema.foo_trino" not in rewritten


def test_schema_rewriter_moves_intermediate_write_targets_to_runtime_schema():
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.final_table\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    sql = """
CREATE TABLE sandbox.tmp_stage AS
SELECT * FROM analytics_src.foo;
INSERT INTO sandbox.tmp_stage
SELECT * FROM analytics_src.foo;
"""

    preparer.discover_runtime_targets([sql])
    rewritten = preparer.rewrite_part_sql(sql)

    assert "CREATE TABLE runtime_schema.tmp_stage AS" in rewritten
    assert "INSERT INTO runtime_schema.tmp_stage" in rewritten
    assert "sandbox.tmp_stage" not in rewritten


def test_schema_rewriter_strips_header_before_execution():
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.foo\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    sql = """/* @header
datamart: sandbox.foo_trino
*/
CREATE TABLE sandbox.foo_trino(id bigint);
"""

    rewritten = preparer.rewrite_part_sql(sql)

    assert "@header" not in rewritten
    assert rewritten.startswith("CREATE TABLE sandbox.foo_trino")


def test_parameter_substitution_skips_comments_and_handles_header_values():
    header = HeaderParser.parse(
        """/* @header
params:
  - name: launch_id
  - name: actual_date
    default: $TODAY[-1]
*/"""
    )
    resolver = ParameterResolver(header)
    sql = """
SELECT :launch_id AS launch_id, CAST(:actual_date AS date) AS dt
-- keep :actual_date in comment
/* keep ${actual_date} in block */
"""

    result = resolver.substitute(sql)

    assert "SELECT 1 AS launch_id" in result
    assert "CAST('" in result
    assert "-- keep :actual_date in comment" in result
    assert "/* keep ${actual_date} in block */" in result


def test_compare_strategy_threshold():
    assert TrinoRuntimeTester.choose_compare_strategy(100_000_000, 100_000_000) == "keys_metrics"
    assert TrinoRuntimeTester.choose_compare_strategy(100_000_001, 100_000_000) == "aggregates"


def test_compare_uses_final_target_schema_from_part_zero(tmp_path, monkeypatch):
    state_manager = _state_with_trino_part(
        tmp_path,
        query_name="dma_demo",
        part_num=0,
        sql="CREATE TABLE sandbox.dma_demo_trino AS SELECT 1",
    )
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester.state_manager = state_manager
    tester.runtime_schema = "runtime_schema"
    tester.query_name = state_manager.query_name

    header = HeaderParser.parse(
        "/* @header\n"
        "datamart: source.dma_demo\n"
        "keys:\n"
        "  - event_month\n"
        "*/\n"
    )

    captured_sql = []

    def fake_scalar(connection, sql):
        captured_sql.append(sql)
        return 0

    monkeypatch.setattr(tester, "_scalar", fake_scalar)
    monkeypatch.setattr(tester, "_common_columns", lambda *args, **kwargs: {"metric": "double"})
    monkeypatch.setattr(tester, "_pick_period_column", lambda header: None)
    monkeypatch.setattr(
        tester,
        "_compare_keys_metrics",
        lambda *args, **kwargs: {"missing_keys": 0, "extra_keys": 0, "metric_mismatches": {}, "success": True},
    )

    result = tester._compare(connection=object(), header=header, resolver=SimpleNamespace())

    assert result["target_table"] == "sandbox.dma_demo_trino"
    assert any("FROM sandbox.dma_demo_trino" in sql for sql in captured_sql)


def test_runtime_sample_limit_uses_configured_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "trino_test_sample_limit", 1000)

    state_manager = StateManager("sample_limit_case", base_path=tmp_path / "workflow" / "in_progress")
    state_manager.initialize()
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester.state_manager = state_manager

    sql = "CREATE TABLE sandbox.foo AS SELECT * FROM analytics_src.bar;"

    limited = tester._apply_runtime_sample_limit(sql)

    assert "LIMIT 1000" in limited
    assert not re.search(r"\bLIMIT\s+10\b", limited)


def test_runtime_context_does_not_read_legacy_root_reports(tmp_path):
    state_manager = StateManager("legacy_report_case", base_path=tmp_path / "workflow" / "in_progress")
    state_manager.initialize()
    (state_manager.work_dir / "trino_test_report.json").write_text(
        json.dumps({"repair_sessions": [{"session_id": "legacy"}]}),
        encoding="utf-8",
    )
    (state_manager.work_dir / "fix_attempt_journal.json").write_text(
        json.dumps([{"stage": "runtime_test", "target_part": 1}]),
        encoding="utf-8",
    )
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester.state_manager = state_manager

    assert tester._recent_repair_sessions(3) == []
    assert tester._recent_fix_attempts(part_num=1, limit=3) == []


def test_suggest_column_candidates_handles_underscore_rename():
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)

    result = tester._suggest_column_candidates("isfraud", ["user_id", "is_fraud", "created_at"])

    assert result["candidates"][0]["column"] == "is_fraud"


def test_raw_repair_client_strips_litellm_openai_prefix(monkeypatch):
    monkeypatch.setattr(settings, "llm_model", "openai/google/gemma-4-26b-a4b")
    monkeypatch.setattr(settings, "llm_repair_model", None)
    client = RawRepairLLMClient.__new__(RawRepairLLMClient)

    assert client._model_name() == "google/gemma-4-26b-a4b"


def test_raw_repair_client_prefers_explicit_repair_model(monkeypatch):
    monkeypatch.setattr(settings, "llm_model", "openai/google/gemma-4-26b-a4b")
    monkeypatch.setattr(settings, "llm_repair_model", "qwen/qwen3.6-27b@q6_k")
    monkeypatch.setattr(settings, "llm_profile_repair_think_model", None)
    client = RawRepairLLMClient.__new__(RawRepairLLMClient)

    assert client._model_name() == "qwen/qwen3.6-27b@q6_k"


def test_extract_json_payload_escapes_raw_control_chars_inside_fixed_sql():
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)

    raw_payload = (
        '{\n'
        '  "target_part": 3,\n'
        '  "change_type": "patch",\n'
        '  "fixed_sql": "SELECT\\n'
        '\tCAST(date_create AS DATE) AS date_create\\n'
        'FROM analytics_src.deals",\n'
        '  "summary": "add alias",\n'
        '  "confidence": 0.87,\n'
        '  "used_evidence": ["ctas requires named columns"]\n'
        '}'
    )

    payload = tester._extract_json_payload(raw_payload)

    assert payload["target_part"] == 3
    assert "AS date_create" in payload["fixed_sql"]
    assert "\tCAST(date_create AS DATE)" in payload["fixed_sql"]


def test_resource_error_context_includes_cpu_optimization_guidance(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.setattr(settings, "trino_schema", "runtime_schema")

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.trino_parts_path / "dma_demo_part_0_trino.sql").write_text("SELECT * FROM big_table", encoding="utf-8")

    tester = TrinoRuntimeTester(state_manager)
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.demo\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    resolver = ParameterResolver(header)

    context = tester._build_planner_context(
        part_num=0,
        error_text="TrinoQueryError(type=INSUFFICIENT_RESOURCES, name=EXCEEDED_CPU_LIMIT)",
        runtime_state=RuntimeExecutionState(),
        preparer=preparer,
        resolver=resolver,
    )

    assert "CPU / RESOURCE ERROR GUIDANCE" in context


def test_repair_patch_guard_rejects_unrelated_rewrite(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()

    old_sql = """
CREATE TABLE sandbox.demo_trino (
    user_id BIGINT,
    amount DECIMAL(18, 2),
    event_date DATE
);
"""
    new_sql = "CREATE TABLE sandbox.other_table (x BIGINT);"

    result = RepairPatchGuard(state_manager).validate(part_num=1, old_sql=old_sql, new_sql=new_sql)

    assert not result.accepted
    assert any("created table changed" in reason for reason in result.reasons)


def test_repair_patch_guard_accepts_vertica_aligned_rename_with_downstream_usage(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(3)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)

    (state_manager.vertica_parts_path / "dma_demo_part_1.sql").write_text(
        "create local temp table sf_user_balance_net on commit preserve rows as select 1;",
        encoding="utf-8",
    )
    (state_manager.vertica_parts_path / "dma_demo_part_2.sql").write_text(
        "select * from sf_user_balance_net;",
        encoding="utf-8",
    )
    (state_manager.trino_parts_path / "dma_demo_part_2_trino.sql").write_text(
        "SELECT * FROM sf_user_balance_net;",
        encoding="utf-8",
    )

    old_sql = "CREATE TABLE sf_user_balance AS SELECT 1;"
    new_sql = "CREATE TABLE sf_user_balance_net AS SELECT 1;"

    result = RepairPatchGuard(state_manager).validate(part_num=1, old_sql=old_sql, new_sql=new_sql)

    assert result.accepted
    assert not result.reasons


def test_repair_patch_guard_rejects_vertica_aligned_rename_without_downstream_usage(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(2)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)

    (state_manager.vertica_parts_path / "dma_demo_part_1.sql").write_text(
        "create local temp table sf_user_balance_net on commit preserve rows as select 1;",
        encoding="utf-8",
    )
    (state_manager.vertica_parts_path / "dma_demo_part_2.sql").write_text(
        "select * from another_table;",
        encoding="utf-8",
    )

    old_sql = "CREATE TABLE sf_user_balance AS SELECT 1;"
    new_sql = "CREATE TABLE sf_user_balance_net AS SELECT 1;"

    result = RepairPatchGuard(state_manager).validate(part_num=1, old_sql=old_sql, new_sql=new_sql)

    assert not result.accepted
    assert any("created table changed" in reason for reason in result.reasons)


def test_repair_patch_guard_accepts_sandbox_trino_suffix_rename(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()

    old_sql = "CREATE TABLE sandbox.foo AS SELECT 1;"
    new_sql = "CREATE TABLE sandbox.foo_trino AS SELECT 1;"

    result = RepairPatchGuard(state_manager).validate(part_num=1, old_sql=old_sql, new_sql=new_sql)

    assert result.accepted
    assert not result.reasons


def test_repair_patch_guard_rejects_arbitrary_rename_without_vertica_confirmation(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(2)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)

    (state_manager.vertica_parts_path / "dma_demo_part_1.sql").write_text(
        "create local temp table sf_user_balance on commit preserve rows as select 1;",
        encoding="utf-8",
    )

    old_sql = "CREATE TABLE sf_user_balance AS SELECT 1;"
    new_sql = "CREATE TABLE bar_baz AS SELECT 1;"

    result = RepairPatchGuard(state_manager).validate(part_num=1, old_sql=old_sql, new_sql=new_sql)

    assert not result.accepted
    assert any("created table changed" in reason for reason in result.reasons)


def test_trino_runtime_fix_saves_new_latest_version(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "trino_schema", "runtime_schema")

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.vertica_parts_path / "dma_demo_part_0.sql").write_text("SELECT 1", encoding="utf-8")
    (state_manager.trino_parts_path / "dma_demo_part_0_trino.sql").write_text("SELECT bad", encoding="utf-8")

    tester = TrinoRuntimeTester(state_manager)
    tester.raw_repair_client = FakeRawRepairClient(
        [
            json.dumps(
                {
                    "hypothesis": "local bad column",
                    "target_part_candidate": 0,
                    "actions": [],
                    "stop_and_fix_now": True,
                    "why": "simple local repair",
                }
            ),
            json.dumps(
                {
                    "target_part": 0,
                    "fixed_sql": "SELECT 1",
                    "summary": "fix bad select",
                    "confidence": 0.9,
                    "used_evidence": ["runtime error"],
                }
            ),
        ]
    )
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.demo\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    resolver = ParameterResolver(header)
    runtime_state = RuntimeExecutionState()

    class FakeConnection:
        def cursor(self):
            raise AssertionError("part_0 fix test must not need live introspection")

    fix_result = tester._fix_part(
        FakeConnection(),
        0,
        "Column bad cannot be resolved",
        header,
        {},
        runtime_state,
        preparer,
        resolver,
        {"diagnostic_queries": [], "introspection": [], "repair_sessions": [], "event_log": []},
        1,
    )
    new_path = fix_result["path"]

    assert new_path.name == "dma_demo_part_0_trino_v1.sql"
    assert new_path.read_text(encoding="utf-8") == "SELECT 1"
    assert state_manager.get_latest_version_path(0) == new_path
    assert fix_result["target_part"] == 0


def test_execute_sql_strips_trailing_semicolons():
    executed = []

    class FakeCursor:
        def execute(self, sql):
            executed.append(sql)

        def fetchall(self):
            return []

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester._execute_sql(FakeConnection(), "SELECT 1; SET SESSION x = 'y';")

    assert executed == ["SELECT 1", "SET SESSION x = 'y'"]


def test_execute_sql_retries_transient_empty_document_error(tmp_path):
    workflow_root = tmp_path / "workflow"
    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()

    class FakeCursor:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, sql):
            self.connection.executed.append(sql)
            if sql.startswith("DROP TABLE"):
                return
            if self.connection.failures_left:
                self.connection.failures_left -= 1
                raise Exception("Input is a zero-length, empty document: line 1 column 1 (char 0)")

        def fetchall(self):
            return []

    class FakeConnection:
        def __init__(self):
            self.executed = []
            self.failures_left = 1

        def cursor(self):
            return FakeCursor(self)

    tester = TrinoRuntimeTester(state_manager)
    tester.runtime_schema = "runtime_schema"
    runtime_state = RuntimeExecutionState(expected_tables_by_part={11: {"user_balance"}})
    report = {"event_log": []}
    connection = FakeConnection()

    tester._execute_sql_with_transient_retries(
        connection=connection,
        sql="CREATE TABLE runtime_schema.user_balance AS SELECT 1",
        part_num=11,
        total_parts=41,
        version=1,
        runtime_state=runtime_state,
        report=report,
    )

    assert connection.executed == [
        "CREATE TABLE runtime_schema.user_balance AS SELECT 1",
        "DROP TABLE IF EXISTS runtime_schema.user_balance",
        "CREATE TABLE runtime_schema.user_balance AS SELECT 1",
    ]
    assert report["event_log"][0]["event"] == "transient_execution_retry"


def test_store_part_saves_runtime_params():
    class FakeCursor:
        description = [("first_period_date",)]

        def execute(self, sql):
            assert "date_trunc" in sql

        def fetchall(self):
            return [(date(2026, 4, 1),)]

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    resolver = ParameterResolver(HeaderParser.parse("/* @header\n*/"))
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)

    result = tester._execute_store_part(
        FakeConnection(),
        "SELECT date_trunc('month', cast('2026-04-20' AS DATE)) AS first_period_date;",
        resolver,
    )

    assert result["status"] == "store_ok"
    assert resolver.values["first_period_date"] == "'2026-04-01'"


def test_version_id_store_is_ignored_for_trino():
    resolver = ParameterResolver(HeaderParser.parse("/* @header\n*/"))
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)

    result = tester._execute_store_part(
        object(),
        "SELECT COALESCE(MAX(version_id), 0) + 1 AS version_id FROM sandbox.foo;",
        resolver,
    )

    assert result["status"] == "ignored_version_id_store"
    assert resolver.values["version_id"] == "1"


def test_forbidden_patterns_include_version_id_rule():
    patterns = json.loads(open("golden_dataset/forbidden_patterns.json", encoding="utf-8").read())
    assert any(pattern["id"] == "version_id_forbidden" for pattern in patterns["patterns"])


def test_part0_columns_are_extracted_from_latest_version(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.trino_parts_path / "dma_demo_part_0_trino.sql").write_text(
        """/* @header
datamart: analytics_src.demo
*/
CREATE TABLE sandbox.demo_trino (
    launch_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(18, 2),
    test_date DATE
)
WITH (
    partitioned_by = ARRAY['test_date']
);
""",
        encoding="utf-8",
    )

    tester = TrinoRuntimeTester(state_manager)

    assert tester._extract_part0_columns() == [
        {"name": "launch_id", "type": "BIGINT"},
        {"name": "user_id", "type": "BIGINT"},
        {"name": "amount", "type": "DECIMAL(18, 2)"},
        {"name": "test_date", "type": "DATE"},
    ]


def test_runtime_introspection_uses_select_limit_and_skips_sandbox(monkeypatch):
    monkeypatch.setattr(settings, "trino_schema", "runtime_schema")
    executed = []

    class FakeCursor:
        description = [("id",), ("name",)]

        def execute(self, sql):
            executed.append(sql)

        def fetchall(self):
            return [(1, "demo")]

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)

    ok = tester._inspect_table(FakeConnection(), "tmp_part_0", ["tmp_part_0"])
    skipped = tester._inspect_table(FakeConnection(), "sandbox.prod_table", ["tmp_part_0"])
    skipped_catalog = tester._inspect_table(FakeConnection(), "catalog_a.target_schema.prod_table", ["tmp_part_0"])

    assert executed == ["SELECT * FROM runtime_schema.tmp_part_0 LIMIT 1"]
    assert ok["columns"] == ["id", "name"]
    assert ok["sample_row"] == ["1", "demo"]
    assert skipped["status"] == "skipped"
    assert skipped_catalog["status"] == "skipped"


def test_table_refs_include_catalog_schema_table_names():
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.demo\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)

    refs = tester._table_refs_for_introspection(
        "SELECT * FROM catalog_a.analytics_src.source_table JOIN tmp_part_0 ON true",
        "SELECT * FROM runtime_schema.tmp_part_0 JOIN catalog_a.target_schema.forbidden ON true",
        preparer,
    )

    assert "catalog_a.analytics_src.source_table" in refs
    assert "catalog_a.target_schema.forbidden" in refs


def test_runtime_fix_formats_new_non_part0_version(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(2)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.trino_parts_path / "dma_demo_part_1_trino.sql").write_text("SELECT bad", encoding="utf-8")
    formatted_files = []

    def fake_format_file(self, file_path):
        formatted_files.append(file_path.name)
        file_path.write_text("SELECT 1\n", encoding="utf-8")
        return True, None

    monkeypatch.setattr("core.formatter.TrinoFormatter.format_file", fake_format_file)
    tester = TrinoRuntimeTester(state_manager)

    new_path = tester._save_fixed_version(1, "select 1")

    assert formatted_files == ["dma_demo_part_1_trino_v1.sql"]
    assert new_path.read_text(encoding="utf-8") == "SELECT 1\n"
    metadata = state_manager.get_part_metadata(1)
    assert metadata["runtime_format"]["status"] == "formatted"


def test_repair_agent_can_request_runtime_table_introspection(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "trino_schema", "runtime_schema")

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(2)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.vertica_parts_path / "dma_demo_part_1.sql").write_text(
        "SELECT * FROM tmp_part_0",
        encoding="utf-8",
    )
    (state_manager.trino_parts_path / "dma_demo_part_0_trino.sql").write_text(
        "CREATE TABLE tmp_part_0 (id BIGINT, name VARCHAR);",
        encoding="utf-8",
    )
    (state_manager.trino_parts_path / "dma_demo_part_1_trino.sql").write_text(
        "SELECT missing_col FROM tmp_part_0",
        encoding="utf-8",
    )

    class FakeCursor:
        description = [("id",), ("name",)]

        def __init__(self, executed):
            self.executed = executed

        def execute(self, sql):
            self.executed.append(sql)

        def fetchall(self):
            return [(1, "demo")]

    class FakeConnection:
        def __init__(self):
            self.executed = []

        def cursor(self):
            return FakeCursor(self.executed)

    fake_connection = FakeConnection()
    raw_client = FakeRawRepairClient(
        [
            json.dumps(
                {
                    "hypothesis": "Need to inspect the runtime table columns before fixing the query.",
                    "target_part_candidate": 1,
                    "actions": [
                        {
                            "tool": "inspect_runtime_table",
                            "args": {"table_name": "tmp_part_0"},
                            "purpose": "see available columns",
                        }
                    ],
                    "stop_and_fix_now": False,
                    "why": "Need runtime table structure",
                }
            ),
            json.dumps(
                {
                    "target_part": 1,
                    "fixed_sql": "SELECT id, name FROM tmp_part_0",
                    "summary": "Selected available columns from runtime table",
                    "confidence": 0.83,
                    "used_evidence": ["tmp_part_0 contains id and name"],
                }
            ),
        ]
    )
    tester = TrinoRuntimeTester(state_manager)
    tester.raw_repair_client = raw_client
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.demo\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    preparer.discover_runtime_targets(
        [
            (state_manager.trino_parts_path / "dma_demo_part_0_trino.sql").read_text(encoding="utf-8"),
            (state_manager.trino_parts_path / "dma_demo_part_1_trino.sql").read_text(encoding="utf-8"),
        ]
    )
    resolver = ParameterResolver(header)
    runtime_state = RuntimeExecutionState(runtime_tables_created={"tmp_part_0"})

    fix_result = tester._fix_part(
        fake_connection,
        1,
        "Column missing_col cannot be resolved",
        header,
        {},
        runtime_state,
        preparer,
        resolver,
        {"diagnostic_queries": [], "introspection": [], "repair_sessions": [], "event_log": []},
        1,
    )
    new_path = fix_result["path"]

    assert fake_connection.executed == ["SELECT * FROM runtime_schema.tmp_part_0 LIMIT 1"]
    assert len(raw_client.prompts) == 2
    assert "Investigation results:" in raw_client.prompts[1]
    assert '"columns"' in raw_client.prompts[1]
    fixed_sql = new_path.read_text(encoding="utf-8")
    assert "SELECT" in fixed_sql
    assert "id" in fixed_sql
    assert "name" in fixed_sql
    assert "FROM tmp_part_0" in fixed_sql


def test_introspection_request_parser_accepts_only_allowed_items():
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)

    requested = tester._parse_introspection_request(
        "-- VER2TRI_INTROSPECT: part0_columns, table:tmp_1, table:catalog_a.analytics_src.foo, drop table x"
    )

    assert requested == ["part0_columns", "table:tmp_1", "table:catalog_a.analytics_src.foo"]


def test_repair_agent_can_use_diagnostic_query_without_saving_it(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "trino_schema", "runtime_schema")

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(2)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.vertica_parts_path / "dma_demo_part_1.sql").write_text(
        "SELECT * FROM analytics_src.source_table",
        encoding="utf-8",
    )
    (state_manager.trino_parts_path / "dma_demo_part_1_trino.sql").write_text(
        "SELECT bad_col FROM analytics_src.source_table",
        encoding="utf-8",
    )

    class FakeCursor:
        description = [("id",), ("value",)]

        def __init__(self, executed):
            self.executed = executed

        def execute(self, sql):
            self.executed.append(sql)

        def fetchall(self):
            return [(1, "x")]

    class FakeConnection:
        def __init__(self):
            self.executed = []

        def cursor(self):
            return FakeCursor(self.executed)

    fake_connection = FakeConnection()
    raw_client = FakeRawRepairClient(
        [
            json.dumps(
                {
                    "hypothesis": "Need to inspect source rows before choosing the right column.",
                    "target_part_candidate": 1,
                    "actions": [
                        {
                            "tool": "run_diagnostic_query",
                            "args": {"sql": "SELECT id, value FROM analytics_src.source_table LIMIT 5"},
                            "purpose": "see available source columns",
                        }
                    ],
                    "stop_and_fix_now": False,
                    "why": "Need evidence from source table",
                }
            ),
            json.dumps(
                {
                    "target_part": 1,
                    "fixed_sql": "SELECT id FROM analytics_src.source_table",
                    "summary": "Use available id column",
                    "confidence": 0.81,
                    "used_evidence": ["Diagnostic query showed id column"],
                }
            ),
        ]
    )
    tester = TrinoRuntimeTester(state_manager)
    tester.raw_repair_client = raw_client
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.demo\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    resolver = ParameterResolver(header)
    report = {"diagnostic_queries": [], "introspection": [], "repair_sessions": [], "event_log": []}
    runtime_state = RuntimeExecutionState()

    fix_result = tester._fix_part(
        fake_connection,
        1,
        "Column bad_col cannot be resolved",
        header,
        {},
        runtime_state,
        preparer,
        resolver,
        report,
        1,
    )
    new_path = fix_result["path"]

    assert fake_connection.executed == ["SELECT id, value FROM analytics_src.source_table LIMIT 5"]
    assert report["diagnostic_queries"]
    assert "Investigation results:" in raw_client.prompts[1]
    assert '"sample_rows"' in raw_client.prompts[1]
    assert new_path.read_text(encoding="utf-8").strip() == "SELECT id FROM analytics_src.source_table"


def test_repair_contract_failure_falls_back_to_full_rewrite(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "trino_schema", "runtime_schema")

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.vertica_parts_path / "dma_demo_part_0.sql").write_text("SELECT fw_close_dt FROM src", encoding="utf-8")
    (state_manager.trino_parts_path / "dma_demo_part_0_trino.sql").write_text(
        "SELECT\n"
        "    1 AS fw_close_dt,\n"
        "    fw_close_dt + 1 AS window_length\n"
        "FROM src\n",
        encoding="utf-8",
    )

    raw_client = FakeRawRepairClient(
        [
            json.dumps(
                {
                    "hypothesis": "fw_close_dt is referenced in the same SELECT where it is defined",
                    "target_part_candidate": 0,
                    "actions": [],
                    "stop_and_fix_now": True,
                    "why": "local alias scope bug",
                }
            ),
            json.dumps(
                {
                    "target_part": 0,
                    "change_type": "line_patch",
                    "edits": [
                        {
                            "op": "replace_line",
                            "line": 3,
                            "old": "    fw_close_dt + 1 AS window_length",
                            "new": "    (\n        1 + 1\n    ) AS window_length",
                        }
                    ],
                    "summary": "Expand alias reference",
                    "confidence": 0.8,
                    "used_evidence": ["Alias is referenced in the same SELECT scope"],
                }
            ),
            json.dumps(
                {
                    "target_part": 0,
                    "change_type": "full_rewrite",
                    "fixed_sql": "SELECT\n    1 AS fw_close_dt,\n    1 + 1 AS window_length\nFROM src\n",
                    "summary": "Rewrote the part to remove same-select alias reuse",
                    "confidence": 0.88,
                    "used_evidence": ["Alias from the current SELECT scope cannot be reused in Trino"],
                }
            ),
        ]
    )

    tester = TrinoRuntimeTester(state_manager)
    tester.raw_repair_client = raw_client
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.demo\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    resolver = ParameterResolver(header)
    runtime_state = RuntimeExecutionState()
    report = {"diagnostic_queries": [], "introspection": [], "repair_sessions": [], "event_log": []}

    class FakeConnection:
        def cursor(self):
            raise AssertionError("full rewrite fallback test must not need live introspection")

    fix_result = tester._fix_part(
        FakeConnection(),
        0,
        "Column 'fw_close_dt' cannot be resolved",
        header,
        {},
        runtime_state,
        preparer,
        resolver,
        report,
        1,
    )

    fixed_sql = fix_result["path"].read_text(encoding="utf-8")

    assert len(raw_client.prompts) == 3
    assert "CONTRACT FAILURE FALLBACK" in raw_client.prompts[2]
    assert "Do not return edits." in raw_client.prompts[2]
    assert "fw_close_dt + 1 AS window_length" not in fixed_sql
    assert "1 + 1 AS window_length" in fixed_sql
    assert any(event["event"] == "repair_contract_fallback_requested" for event in report["event_log"])


def test_runtime_log_event_updates_report_and_runtime_state(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    tester = TrinoRuntimeTester(state_manager)
    report = {"event_log": []}

    tester._log_event(
        report,
        "repair_session_started",
        part=5,
        total_parts=10,
        fix_attempt=2,
        error="boom",
    )

    state = state_manager.load_state()
    assert report["event_log"]
    assert report["event_log"][0]["event"] == "repair_session_started"
    assert state["test_runtime"]["current_part"] == 5
    assert state["test_runtime"]["current_fix_attempt"] == 2
    assert state["test_runtime"]["last_error_text"] == "boom"
    assert state["test_runtime"]["last_event"]["event"] == "repair_session_started"


def test_dependency_fix_context_includes_direct_upstream_parts(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "trino_schema", "runtime_schema")

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(6)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.vertica_parts_path / "dma_demo_part_4.sql").write_text("create local temp table gos_client as select 1;", encoding="utf-8")
    (state_manager.trino_parts_path / "dma_demo_part_4_trino.sql").write_text("SELECT 1 AS broken_gos_client", encoding="utf-8")
    (state_manager.vertica_parts_path / "dma_demo_part_5.sql").write_text("select * from gos_client", encoding="utf-8")
    (state_manager.trino_parts_path / "dma_demo_part_5_trino.sql").write_text("SELECT * FROM gos_client", encoding="utf-8")
    state_manager.update_section(
        "parts",
        {
            "dependencies": {
                "part_4": {"interpolate_dependency": [], "table_dependency": [], "execution_order_hint": ["gos_client"]},
                "part_5": {
                    "interpolate_dependency": [],
                    "table_dependency": [{"table": "gos_client", "created_in_part": 4}],
                    "execution_order_hint": ["users"],
                },
            }
        },
    )

    tester = TrinoRuntimeTester(state_manager)
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.demo\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    resolver = ParameterResolver(header)

    context = tester._build_fix_context(
        part_num=5,
        error_text="Table 'catalog_a.runtime_schema.gos_client' does not exist",
        trino_sql="SELECT * FROM gos_client",
        prepared_sql="SELECT * FROM gos_client",
        preparer=preparer,
        resolver=resolver,
        introspection=None,
        diagnostic_results=None,
    )

    assert "Direct upstream dependency context:" in context
    assert '"part_num": 4' in context
    assert '"relation": "table_dependency"' in context
    assert "broken_gos_client" in context
    assert "create local temp table gos_client" in context
    assert "Available runtime tools you can use:" in context
    assert "VER2TRI_APPLY_TO_PART" in context
    assert "VER2TRI_INTROSPECT:" in context
    assert "VER2TRI_DIAGNOSTIC_QUERY_START" in context


def test_resolve_missing_table_producer_uses_runtime_expected_tables():
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    runtime_state = SimpleNamespace(
        table_to_creator_part={"gos_client": 4, "users": 5},
    )

    producer = tester._resolve_missing_table_producer(
        'TrinoUserError(type=USER_ERROR, name=TABLE_NOT_FOUND, message="line 6:25: Table \'catalog_a.runtime_schema.gos_client\' does not exist")',
        5,
        runtime_state,
    )

    assert producer == 4


def test_repair_agent_can_choose_upstream_part_to_fix(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "trino_schema", "runtime_schema")

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(6)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.vertica_parts_path / "dma_demo_part_4.sql").write_text("create local temp table gos_client as select 1;", encoding="utf-8")
    (state_manager.trino_parts_path / "dma_demo_part_4_trino.sql").write_text("SELECT 1 AS broken_gos_client", encoding="utf-8")
    (state_manager.vertica_parts_path / "dma_demo_part_5.sql").write_text("select * from gos_client", encoding="utf-8")
    (state_manager.trino_parts_path / "dma_demo_part_5_trino.sql").write_text("SELECT * FROM gos_client", encoding="utf-8")
    state_manager.update_section(
        "parts",
        {
            "dependencies": {
                "part_4": {"interpolate_dependency": [], "table_dependency": [], "execution_order_hint": ["gos_client"]},
                "part_5": {
                    "interpolate_dependency": [],
                    "table_dependency": [{"table": "gos_client", "created_in_part": 4}],
                    "execution_order_hint": ["users"],
                },
            }
        },
    )

    class FakeConnection:
        def cursor(self):
            raise AssertionError("upstream target fix test must not need live introspection")

    raw_client = FakeRawRepairClient(
        [
            json.dumps(
                {
                    "hypothesis": "Producer part likely did not create gos_client correctly.",
                    "target_part_candidate": 4,
                    "actions": [
                        {
                            "tool": "read_trino_part",
                            "args": {"part_num": 4},
                            "purpose": "inspect current producer SQL",
                        },
                        {
                            "tool": "read_vertica_part",
                            "args": {"part_num": 4},
                            "purpose": "compare original producer logic",
                        },
                    ],
                    "stop_and_fix_now": False,
                    "why": "The missing object is produced upstream by part 4",
                }
            ),
            json.dumps(
                {
                    "target_part": 4,
                    "fixed_sql": "CREATE TABLE gos_client AS SELECT 1",
                    "summary": "Fix producer to create gos_client",
                    "confidence": 0.77,
                    "used_evidence": ["part 4 should create gos_client"],
                }
            ),
        ]
    )
    tester = TrinoRuntimeTester(state_manager)
    tester.raw_repair_client = raw_client
    header = HeaderParser.parse("/* @header\ndatamart: analytics_src.demo\nkeys:\n  - id\n*/")
    preparer = TrinoSQLPreparer("runtime_schema", header)
    resolver = ParameterResolver(header)
    runtime_state = RuntimeExecutionState(table_to_creator_part={"gos_client": 4})

    result = tester._fix_part(
        FakeConnection(),
        5,
        "Table 'catalog_a.runtime_schema.gos_client' does not exist",
        header,
        {},
        runtime_state,
        preparer,
        resolver,
        {"diagnostic_queries": [], "introspection": [], "repair_sessions": [], "event_log": []},
        1,
    )

    assert result["target_part"] == 4
    assert "create local temp table gos_client" in raw_client.prompts[1].lower()
    assert '"target_part_candidate": 4' in raw_client.prompts[1]


def test_parse_repair_plan_rejects_unknown_tools():
    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester.REPAIR_AGENT_ALLOWED_TOOLS = TrinoRuntimeTester.REPAIR_AGENT_ALLOWED_TOOLS

    try:
        tester._parse_repair_plan(
            json.dumps(
                {
                    "hypothesis": "bad tool",
                    "target_part_candidate": 1,
                    "actions": [{"tool": "drop_all_tables", "args": {}, "purpose": "nope"}],
                    "stop_and_fix_now": False,
                    "why": "invalid",
                }
            ),
            default_part=1,
        )
        assert False, "expected TrinoRuntimeError for unknown tool"
    except Exception as exc:
        assert "Unknown repair tool" in str(exc)


def test_inspect_information_schema_returns_tables_and_columns():
    executed = []

    class FakeCursor:
        def execute(self, sql):
            executed.append(sql)

        def fetchall(self):
            if len(executed) == 1:
                return [("runtime_schema", "users")]
            return [("runtime_schema", "users", "user_id", "bigint")]

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester.runtime_schema = "runtime_schema"
    result = tester._inspect_information_schema(FakeConnection(), "users")

    assert len(executed) == 2
    assert "lower(table_schema) IN ('runtime_schema')" in executed[0]
    assert "lower(table_schema) IN ('runtime_schema')" in executed[1]
    assert result["tables"] == [["runtime_schema", "users"]]
    assert result["columns"] == [["runtime_schema", "users", "user_id", "bigint"]]
    assert result["searched_schemas"] == ["runtime_schema"]


def test_inspect_information_schema_uses_explicit_schema():
    executed = []

    class FakeCursor:
        def execute(self, sql):
            executed.append(sql)

        def fetchall(self):
            return []

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester.runtime_schema = "runtime_schema"
    result = tester._inspect_information_schema(FakeConnection(), "analytics_src.users")

    assert "lower(table_schema) IN ('analytics_src')" in executed[0]
    assert "LIKE '%users%'" in executed[0]
    assert result["searched_schemas"] == ["analytics_src"]


def test_runtime_table_introspection_returns_expected_creator_on_missing_table():
    class FakeCursor:
        description = []

        def execute(self, sql):
            raise Exception("Table 'runtime_schema.gos_client' does not exist")

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    tester = TrinoRuntimeTester.__new__(TrinoRuntimeTester)
    tester.runtime_schema = "runtime_schema"
    runtime_state = SimpleNamespace(
        runtime_tables_created={"gos_client"},
        expected_tables_by_part={4: {"gos_client"}},
        table_to_creator_part={"gos_client": 4},
    )
    action = SimpleNamespace(tool="inspect_runtime_table", args={"table_name": "gos_client"}, purpose="inspect")

    result = tester._execute_repair_action(
        connection=FakeConnection(),
        action=action,
        runtime_state=runtime_state,
        preparer=None,
    )

    payload = result["result"]
    assert payload["status"] == "error"
    assert payload["expected_creator_part"] == 4
    assert "Producer part 4 is expected" in payload["producer_contract"]
    assert "Inspect the producer part" in payload["guidance"]
