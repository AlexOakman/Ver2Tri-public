import re

from core.assembler import Assembler
from core.api_validation_fixer import APIValidationFixer
from core.api_validation_fixer import FixStrategy
from core.formatter import TrinoFormatter
from core.pattern_guard import PatternGuard
from core.state_manager import StateManager


def test_latest_version_path_supports_versions_above_v3(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)

    parts_dir = state_manager.trino_parts_path
    parts_dir.mkdir(parents=True, exist_ok=True)

    (parts_dir / "sample_scorecard_part_0_trino.sql").write_text("SELECT 0", encoding="utf-8")
    (parts_dir / "sample_scorecard_part_0_trino_v3.sql").write_text("SELECT 3", encoding="utf-8")
    latest_path = parts_dir / "sample_scorecard_part_0_trino_v4.sql"
    latest_path.write_text("SELECT 4", encoding="utf-8")

    assert state_manager.get_latest_version_path(0) == latest_path

    assembler = Assembler(state_manager)
    assert assembler._get_latest_version_content(0) == "SELECT 4"

    pattern_guard = PatternGuard.__new__(PatternGuard)
    pattern_guard.state_manager = state_manager
    assert pattern_guard.get_latest_valid_version(0) == latest_path


def test_api_reassemble_updates_final_and_test_artifacts_from_latest_version(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(2)

    parts_dir = state_manager.trino_parts_path
    parts_dir.mkdir(parents=True, exist_ok=True)

    (parts_dir / "sample_scorecard_part_0_trino.sql").write_text("CREATE TABLE x(id bigint)", encoding="utf-8")
    (parts_dir / "sample_scorecard_part_1_trino.sql").write_text("SELECT 1", encoding="utf-8")
    latest_path = parts_dir / "sample_scorecard_part_1_trino_v1.sql"
    latest_path.write_text("SELECT 11", encoding="utf-8")

    fixer = APIValidationFixer.__new__(APIValidationFixer)
    fixer.state_manager = state_manager
    fixer.query_name = state_manager.query_name

    fixer._reassemble_final()

    final_sql = (state_manager.work_dir / "final" / "sample_scorecard_final.sql").read_text(encoding="utf-8")

    assert "SELECT 11;" in final_sql
    assert not (state_manager.work_dir / "test" / "sample_scorecard_trino_test.sql").exists()
    assert not (state_manager.work_dir / "validations" / "validate_sample_scorecard_test.py").exists()
    assert not hasattr(Assembler, "generate_test_script")
    assert not hasattr(Assembler, "generate_test_sql_only")


def test_assembler_skips_legacy_noop_semicolon_part(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(2)

    parts_dir = state_manager.trino_parts_path
    parts_dir.mkdir(parents=True, exist_ok=True)
    (parts_dir / "sample_scorecard_part_0_trino.sql").write_text("SELECT 42", encoding="utf-8")
    (parts_dir / "sample_scorecard_part_1_trino.sql").write_text(";", encoding="utf-8")

    final_path = Assembler(state_manager).assemble_final()
    final_sql = final_path.read_text(encoding="utf-8")

    assert "SELECT 42;" in final_sql
    assert "-- Part 01" not in final_sql


def test_finalize_review_does_not_leave_trino_artifact_in_done(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)

    parts_dir = state_manager.trino_parts_path
    parts_dir.mkdir(parents=True, exist_ok=True)
    (parts_dir / "sample_scorecard_part_0_trino.sql").write_text("SELECT 42", encoding="utf-8")

    assembler = Assembler(state_manager)
    local_final = assembler.assemble_final()
    assert local_final == state_manager.work_dir / "final" / "sample_scorecard_final.sql"

    assembler.finalize_workflow(move_to="review")

    review_sql = workflow_root / "review" / "trino" / "sample_scorecard_trino.sql"
    done_sql = workflow_root / "done" / "trino" / "sample_scorecard_trino.sql"

    assert review_sql.exists()
    assert not done_sql.exists()


def test_set_part_status_resets_stale_flags(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)

    state_manager.set_part_status(0, "validated", {"fix_version": 3})
    state_manager.set_part_status(0, "pattern_fixed", {"fix_version": 4})

    status = state_manager.get_part_status(0)
    assert status["translated"] is True
    assert status["validated"] is False
    assert status["pattern_error"] is False
    assert status["fix_version"] == 4


def test_pattern_context_is_persisted_in_part_metadata(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)

    guard = PatternGuard.__new__(PatternGuard)
    guard.state_manager = state_manager

    source_path = state_manager.trino_parts_path / "sample_scorecard_part_0_trino.sql"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("SELECT 1", encoding="utf-8")

    found_patterns = [{
        "id": "uncasted_parameter",
        "description": "Needs cast",
        "fix_hint": "Wrap into CAST",
    }]

    guard._persist_pattern_context(
        part_num=0,
        source_path=source_path,
        source_version=0,
        found_patterns=found_patterns,
        context_rules="rule text",
        resolved=False,
        result_version=1,
        error="still broken",
    )

    metadata = state_manager.get_part_metadata(0)
    context = metadata["pattern_context"]
    assert context["resolved"] is False
    assert context["active_patterns"][0]["id"] == "uncasted_parameter"
    assert context["history"][-1]["context_rules"] == "rule text"


def test_pattern_guard_builds_repair_mode_context(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)

    source_path = state_manager.trino_parts_path / "sample_scorecard_part_0_trino.sql"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("CREATE TABLE x WITH (temporary = true) AS SELECT 1", encoding="utf-8")

    guard = PatternGuard.__new__(PatternGuard)
    guard.state_manager = state_manager

    context = guard._build_fix_context([{
        "id": "temporary_flag_true",
        "description": "Detects temporary flag",
        "fix_hint": "Remove temporary = true",
        "matches": ["temporary = true"],
    }], part_num=0)

    assert "forbidden-pattern repair assistant" in context
    assert "=== CURRENT TRINO ===" in context
    assert "temporary = true" in context
    assert "=== ALLOWED CHANGE SCOPE ===" in context
    assert "=== FORBIDDEN CHANGE SCOPE ===" in context
    assert "do not modify JOIN/ON clauses" in context


def test_pattern_guard_uses_raw_repair_client_after_deterministic_path(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)

    trino_path = state_manager.trino_parts_path / "dma_demo_part_0_trino.sql"
    trino_path.parent.mkdir(parents=True, exist_ok=True)
    trino_path.write_text("SELECT bad_column\n", encoding="utf-8")

    class FakeRepairClient:
        def __init__(self):
            self.prompts = []

        def complete(self, prompt: str) -> str:
            self.prompts.append(prompt)
            return "SELECT fixed_column\n"

    repair_client = FakeRepairClient()
    guard = PatternGuard.__new__(PatternGuard)
    guard.state_manager = state_manager
    guard.repair_client = repair_client
    monkeypatch.setattr(guard, "_fix_version_id_store_part", lambda **kwargs: None)
    monkeypatch.setattr(guard, "_get_original_vertica", lambda part_num: "SELECT source_column")
    monkeypatch.setattr(guard, "_persist_pattern_context", lambda **kwargs: None)
    monkeypatch.setattr(guard, "_update_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(guard, "check_part_content", lambda fixed_sql: (True, []))

    success, fixed_path = guard.fix_patterns(
        0,
        [{"id": "some_pattern", "description": "broken pattern", "matches": ["bad_column"]}],
    )

    assert success is True
    assert fixed_path is not None
    assert repair_client.prompts
    assert "CURRENT TRINO" in repair_client.prompts[0]
    assert "ALLOWED CHANGE SCOPE" in repair_client.prompts[0]


def test_pattern_guard_replaces_version_id_store_part_with_noop(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)

    source_path = state_manager.trino_parts_path / "dma_demo_part_0_trino.sql"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(
        "-- @store\n"
        "SELECT COALESCE(MAX(version_id), 0) + 1 AS next_version_id\n"
        "FROM sandbox.dma_demo;",
        encoding="utf-8",
    )

    guard = PatternGuard.__new__(PatternGuard)
    guard.state_manager = state_manager
    guard.patterns = [
        {
            "id": "version_id_forbidden",
            "pattern": r"\bversion_id\b",
            "type": "regex",
            "description": "version_id must not be present in Trino SQL",
            "severity": "error",
            "fix_hint": "Remove version_id",
        }
    ]
    guard._compiled_patterns = {
        "version_id_forbidden": re.compile(r"\bversion_id\b", re.IGNORECASE)
    }

    success, fixed_path = guard.fix_patterns(
        0,
        [
            {
                "id": "version_id_forbidden",
                "description": "version_id must not be present in Trino SQL",
                "fix_hint": "Remove version_id",
            }
        ],
    )

    assert success is True
    assert fixed_path is not None
    assert fixed_path.name == "dma_demo_part_0_trino_v1.sql"
    assert fixed_path.read_text(encoding="utf-8") == "SELECT 1;\n"

    status = state_manager.get_part_status(0)
    assert status["status"] == "pattern_fixed"
    assert status["fix_version"] == 1


def test_api_fix_context_includes_previous_pattern_context(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)
    state_manager.update_part_metadata(0, {
        "pattern_context": {
            "history": [{
                "source_version": 1,
                "resolved": False,
                "result_version": 2,
                "error": "Fix iterations exhausted",
                "found_patterns": [{
                    "id": "temporary_flag_true",
                    "description": "temporary=true must be removed",
                    "fix_hint": "remove temporary flag",
                }],
                "context_rules": "- temporary_flag_true: remove temporary flag",
            }]
        }
    })

    fixer = APIValidationFixer.__new__(APIValidationFixer)
    fixer.state_manager = state_manager
    fixer.query_name = state_manager.query_name

    strategy = FixStrategy(
        error_type="sql_table_n",
        target_part=0,
        context_parts=[],
        auto_fix=False,
        description="test",
    )
    context = fixer._build_fix_context(
        strategy=strategy,
        error_details={"title": "SQL error", "comment": "broken query", "metadata": {}},
        vertica_sql="SELECT 1",
        trino_sql="SELECT 2",
        part_num=0,
    )

    assert "PREVIOUS PATTERN GUARD CONTEXT" in context
    assert "temporary_flag_true" in context
    assert "remove temporary flag" in context


def test_api_auto_replace_uses_only_latest_part_version(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)

    parts_dir = state_manager.trino_parts_path
    parts_dir.mkdir(parents=True, exist_ok=True)

    base_path = parts_dir / "sample_scorecard_part_0_trino.sql"
    v1_path = parts_dir / "sample_scorecard_part_0_trino_v1.sql"
    base_path.write_text("SELECT * FROM analytics_src.foo", encoding="utf-8")
    v1_path.write_text("SELECT * FROM analytics_src.foo WHERE fixed = 1", encoding="utf-8")

    fixer = APIValidationFixer.__new__(APIValidationFixer)
    fixer.state_manager = state_manager
    fixer.query_name = state_manager.query_name

    replacements = fixer._apply_trino_replacements("Replace tables to _trino suffix: ['analytics_src.foo']")

    v2_path = parts_dir / "sample_scorecard_part_0_trino_v2.sql"
    assert replacements == 1
    assert v2_path.exists()
    assert base_path.read_text(encoding="utf-8") == "SELECT * FROM analytics_src.foo"
    assert v1_path.read_text(encoding="utf-8") == "SELECT * FROM analytics_src.foo WHERE fixed = 1"
    assert "analytics_src.foo_trino" in v2_path.read_text(encoding="utf-8")
    assert "fixed = 1" in v2_path.read_text(encoding="utf-8")


def test_formatter_formats_only_latest_versions_and_skips_part_0(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("sample_scorecard", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(3)

    parts_dir = state_manager.trino_parts_path
    parts_dir.mkdir(parents=True, exist_ok=True)

    part_0 = parts_dir / "sample_scorecard_part_0_trino.sql"
    part_1_base = parts_dir / "sample_scorecard_part_1_trino.sql"
    part_1_latest = parts_dir / "sample_scorecard_part_1_trino_v1.sql"
    part_2 = parts_dir / "sample_scorecard_part_2_trino.sql"

    for path in (part_0, part_1_base, part_1_latest, part_2):
        path.write_text(f"-- {path.name}", encoding="utf-8")

    formatter = TrinoFormatter()
    formatter.enabled = True

    formatted_files = []

    def fake_format_file(file_path):
        formatted_files.append(file_path.name)
        return True, None

    formatter.format_file = fake_format_file

    formatted_count, error_count = formatter.format_parts(
        parts_dir=parts_dir,
        query_name="sample_scorecard",
        total_parts=3,
    )

    assert formatted_count == 2
    assert error_count == 0
    assert formatted_files == [
        "sample_scorecard_part_1_trino_v1.sql",
        "sample_scorecard_part_2_trino.sql",
    ]


def test_formatter_rejects_unnest_column_alias_removal():
    formatter = TrinoFormatter()
    original_sql = """
SELECT dt
FROM date_range
CROSS JOIN UNNEST(SEQUENCE(min_dt, max_dt, INTERVAL '1' DAY)) AS t(dt)
"""
    formatted_sql = """
SELECT dt
FROM date_range
CROSS JOIN UNNEST(SEQUENCE(min_dt, max_dt, INTERVAL '1' DAY))
"""

    assert formatter._unsafe_format_change(original_sql, formatted_sql) == (
        "sqlfluff unsafe change: removed UNNEST column alias"
    )
