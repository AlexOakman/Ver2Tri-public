import json
from pathlib import Path

from config import settings
from main import determine_retry_start_stage, reset_in_progress_states
from core.pipeline import PipelineConfig, PipelineRunner, StageContext, StageResult
from core.pipeline_stages import PatternGuardStage, ReportStage, TranslateStage
from core.state_manager import StateManager
from core.trino_tester import HeaderParser, ParameterResolver, QualifiedTable, TrinoRuntimeTester
from core.translator import PartTranslator


class DummyStage:
    def __init__(self, stage_id, *, blocking, status="success", updates=None, calls=None):
        self.stage_id = stage_id
        self.enabled_by_default = True
        self.blocking = blocking
        self.can_retry_from_here = True
        self.produced_artifacts = []
        self.consumed_context_keys = []
        self.status = status
        self.updates = updates
        self.calls = calls

    def run(self, context):
        if self.calls is not None:
            self.calls.append(self.stage_id)
        return StageResult(
            status=self.status,
            blocking=self.blocking,
            issues=[{"message": f"{self.stage_id} failed"}] if self.status == "failed" else [],
            updates=self.updates
            if self.updates is not None
            else {"status": "review" if self.status == "failed" and self.blocking else "initialized"},
        )


def test_pipeline_runner_marks_non_blocking_warning_as_done_with_warnings(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()

    runner = PipelineRunner(
        {"api_validate": DummyStage("api_validate", blocking=False, status="warning")},
        config=PipelineConfig(
            enabled_stages=["api_validate"],
            stage_order=["api_validate"],
            enable_api_validation=True,
            enable_trino_test=True,
            api_validation_blocking=False,
            trino_test_blocking=True,
        ),
    )

    result = runner.run_stage(
        "api_validate",
        {
            "query_name": "demo",
            "current_part": 0,
            "total_parts": 0,
            "status": "initialized",
            "error_msg": None,
            "metadata_path": str(state_manager.metadata_path),
            "dspy_lm_initialized": False,
        },
    )

    assert result["status"] == "initialized"
    saved_state = state_manager.load_state()
    assert saved_state["pipeline"]["final_decision"] == "done_with_warnings"


def test_pipeline_runner_persists_stage_updates_to_metadata(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()

    class ProgressStage(DummyStage):
        def run(self, context):
            return StageResult(
                status="success",
                blocking=True,
                updates={"current_part": 14, "status": "translating"},
            )

    runner = PipelineRunner(
        {"translate": ProgressStage("translate", blocking=True)},
        config=PipelineConfig(
            enabled_stages=["translate"],
            stage_order=["translate"],
            enable_api_validation=True,
            enable_trino_test=True,
            api_validation_blocking=False,
            trino_test_blocking=True,
        ),
    )

    result = runner.run_stage(
        "translate",
        {
            "query_name": "demo",
            "current_part": 0,
            "total_parts": 41,
            "status": "initialized",
            "error_msg": None,
            "metadata_path": str(state_manager.metadata_path),
            "dspy_lm_initialized": False,
        },
    )

    assert result["current_part"] == 14
    saved_state = state_manager.load_state()
    assert saved_state["current_part"] == 14
    assert saved_state["status"] == "translating"


def test_pipeline_runner_run_pipeline_loops_translate_until_all_parts(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()

    class LoopingTranslateStage(DummyStage):
        def run(self, context):
            if self.calls is not None:
                self.calls.append(self.stage_id)
            current_part = context.state.get("current_part", 0) + 1
            return StageResult(
                status="success",
                blocking=True,
                updates={"current_part": current_part, "status": "translating"},
            )

    calls = []
    runner = PipelineRunner(
        {
            "translate": LoopingTranslateStage("translate", blocking=True, calls=calls),
            "pattern_guard": DummyStage("pattern_guard", blocking=True, updates={}, calls=calls),
        },
        config=PipelineConfig(
            enabled_stages=["translate", "pattern_guard"],
            stage_order=["translate", "pattern_guard"],
            enable_api_validation=True,
            enable_trino_test=True,
            api_validation_blocking=False,
            trino_test_blocking=True,
        ),
    )

    result = runner.run_pipeline(
        "demo",
        {
            "query_name": "demo",
            "current_part": 0,
            "total_parts": 3,
            "status": "initialized",
            "error_msg": None,
            "metadata_path": str(state_manager.metadata_path),
            "dspy_lm_initialized": False,
        },
        start_stage="translate",
    )

    assert result["current_part"] == 3
    assert calls == ["translate", "translate", "translate", "pattern_guard"]


def test_pipeline_runner_run_pipeline_starts_from_requested_stage(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    calls = []

    runner = PipelineRunner(
        {
            "split": DummyStage("split", blocking=True, calls=calls),
            "translate": DummyStage("translate", blocking=True, calls=calls),
            "pattern_guard": DummyStage("pattern_guard", blocking=True, updates={}, calls=calls),
            "format": DummyStage("format", blocking=False, updates={}, calls=calls),
        },
        config=PipelineConfig(
            enabled_stages=["split", "translate", "pattern_guard", "format"],
            stage_order=["split", "translate", "pattern_guard", "format"],
            enable_api_validation=True,
            enable_trino_test=True,
            api_validation_blocking=False,
            trino_test_blocking=True,
        ),
    )

    runner.run_pipeline(
        "demo",
        {
            "query_name": "demo",
            "current_part": 0,
            "total_parts": 0,
            "status": "initialized",
            "error_msg": None,
            "metadata_path": str(state_manager.metadata_path),
            "dspy_lm_initialized": False,
        },
        start_stage="pattern_guard",
    )

    assert calls == ["pattern_guard", "format"]


def test_translate_stage_skips_when_current_part_reaches_total(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(3)

    context = StageContext(
        query_name="demo",
        state_manager=state_manager,
        state={
            "query_name": "demo",
            "current_part": 3,
            "total_parts": 3,
            "status": "review",
        },
        config=PipelineConfig(
            enabled_stages=["translate"],
            stage_order=["translate"],
            enable_api_validation=True,
            enable_trino_test=True,
        ),
    )

    result = TranslateStage().run(context)

    assert result.status == "success"
    assert result.updates["current_part"] == 3
    assert result.details["skipped"] == "current_part_at_or_after_total_parts"


def test_pattern_guard_stage_does_not_require_global_llm_init(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)

    calls = []

    class FakeGuard:
        def __init__(self, state_manager):
            self.state_manager = state_manager
            calls.append("guard_init")

        def check_patterns(self, part_num):
            calls.append(("check_patterns", part_num))
            return True, []

    monkeypatch.setattr("core.pipeline_stages.PatternGuard", FakeGuard)

    context = StageContext(
        query_name="demo",
        state_manager=state_manager,
        state={
            "query_name": "demo",
            "current_part": 0,
            "total_parts": 1,
            "status": "review",
        },
        config=PipelineConfig(
            enabled_stages=["pattern_guard"],
            stage_order=["pattern_guard"],
            enable_api_validation=True,
            enable_trino_test=True,
        ),
    )

    result = PatternGuardStage().run(context)

    assert result.status == "success"
    assert calls[0] == "guard_init"
    assert calls[1] == ("check_patterns", 0)


def test_pipeline_runner_records_disabled_stage_as_skipped(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()

    runner = PipelineRunner(
        {"api_validate": DummyStage("api_validate", blocking=False)},
        config=PipelineConfig(
            enabled_stages=[],
            stage_order=["api_validate"],
            enable_api_validation=False,
            enable_trino_test=True,
            api_validation_blocking=False,
            trino_test_blocking=True,
        ),
    )

    runner.run_pipeline(
        "demo",
        {
            "query_name": "demo",
            "current_part": 0,
            "total_parts": 0,
            "status": "initialized",
            "error_msg": None,
            "metadata_path": str(state_manager.metadata_path),
            "dspy_lm_initialized": False,
        },
        start_stage="api_validate",
    )

    saved_state = state_manager.load_state()
    assert saved_state["diagnostics"]["api_validation"]["status"] == "skipped"
    assert saved_state["pipeline"]["final_decision"] == "done_with_warnings"


def test_auto_retry_prefers_trino_test_when_runtime_report_exists(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo")
    state_manager.initialize()
    state_manager.set_total_parts(2)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.trino_parts_path / "demo_part_0_trino.sql").write_text("SELECT 1", encoding="utf-8")
    (state_manager.trino_parts_path / "demo_part_1_trino.sql").write_text("SELECT 2", encoding="utf-8")
    state_manager.update_state({"current_part": 2, "status": "review"})
    state_manager.register_report(
        "trino_test_report",
        "workflow/in_progress/demo/reports/trino_test_report.json",
    )
    state_manager.update_section(
        "test_runtime",
        {"last_failed_part": 1, "last_error_text": "PAGE_TRANSPORT_TIMEOUT"},
    )

    assert determine_retry_start_stage("demo") == "trino_test"


def test_reset_in_progress_states_clears_stale_reports_and_restores_total_parts(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "workflow_base_path", workflow_root)

    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    state_manager.trino_parts_path.mkdir(parents=True, exist_ok=True)
    state_manager.metadata_path.parent.joinpath("reports").mkdir(parents=True, exist_ok=True)
    state_manager.metadata_path.parent.joinpath("logs").mkdir(parents=True, exist_ok=True)
    (state_manager.vertica_parts_path / "demo_part_0.sql").write_text("SELECT 1", encoding="utf-8")
    (state_manager.vertica_parts_path / "demo_part_1.sql").write_text("SELECT 2", encoding="utf-8")
    (state_manager.trino_parts_path / "demo_part_0_trino.sql").write_text("SELECT 1", encoding="utf-8")
    (state_manager.metadata_path.parent / "reports" / "trino_test_report.json").write_text("{}", encoding="utf-8")
    (state_manager.metadata_path.parent / "reports" / "compare_report.json").write_text("{}", encoding="utf-8")
    (state_manager.metadata_path.parent / "logs" / "fix_attempt_journal.json").write_text("[]", encoding="utf-8")

    reset_count = reset_in_progress_states()
    reset_state = state_manager.load_state()

    assert reset_count == 1
    assert reset_state["status"] == "initialized"
    assert reset_state["total_parts"] == 2
    assert not (state_manager.metadata_path.parent / "reports" / "trino_test_report.json").exists()
    assert not (state_manager.metadata_path.parent / "reports" / "compare_report.json").exists()
    assert not (state_manager.metadata_path.parent / "logs" / "fix_attempt_journal.json").exists()


def test_pipeline_runner_runs_report_after_runtime_review(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    calls = []

    runner = PipelineRunner(
        {
            "trino_test": DummyStage("trino_test", blocking=True, status="failed", updates={}, calls=calls),
            "compare": DummyStage("compare", blocking=True, updates={}, calls=calls),
            "report": DummyStage("report", blocking=False, updates={}, calls=calls),
        },
        config=PipelineConfig(
            enabled_stages=["trino_test", "compare", "report"],
            stage_order=["trino_test", "compare", "report"],
            enable_api_validation=True,
            enable_trino_test=True,
            api_validation_blocking=False,
            trino_test_blocking=True,
        ),
    )

    result = runner.run_pipeline(
        "demo",
        {
            "query_name": "demo",
            "current_part": 0,
            "total_parts": 0,
            "status": "validating",
            "error_msg": None,
            "metadata_path": str(state_manager.metadata_path),
            "dspy_lm_initialized": False,
        },
        start_stage="trino_test",
    )

    assert result["status"] == "review"
    assert calls == ["trino_test", "report"]
    saved_state = state_manager.load_state()
    assert saved_state["pipeline"]["final_decision"] == "review"


def test_runtime_variable_resolver_prefers_explicit_set_values():
    header = HeaderParser.parse(
        """/* @header
params:
  - name: launch_id
  - name: actual_date
    default: $TODAY[-1]
*/"""
    )

    resolver = ParameterResolver.from_sql_parts(
        header,
        [
            "@set launch_id = 42\nSELECT :launch_id",
            "@set actual_date = '2026-04-01'\nSELECT CAST(:actual_date AS date)",
        ],
    )

    assert resolver.values["launch_id"] == "42"
    assert resolver.values["actual_date"] == "'2026-04-01'"


def test_compare_keys_metrics_uses_not_exists_for_nullable_keys():
    executed = []

    class FakeTester(TrinoRuntimeTester):
        def __init__(self):
            self.runtime_schema = settings.trino_schema or settings.trino_test_schema

        def _scalar(self, connection, sql):
            executed.append(sql)
            return 0

    tester = FakeTester()
    tester._compare_keys_metrics(
        connection=None,
        target=QualifiedTable("sandbox", "target_trino"),
        reference=QualifiedTable("dma", "target"),
        keys=["nullable_key"],
        common_columns={"nullable_key": "bigint", "metric": "bigint"},
        filter_sql="",
    )

    assert "NOT EXISTS" in executed[0]
    assert "NOT EXISTS" in executed[1]


def test_report_stage_writes_markdown_and_json(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.update_pipeline(final_decision="done_with_warnings")
    context = StageContext(
        query_name="demo",
        state_manager=state_manager,
        state={"query_name": "demo", "status": "initialized"},
        config=PipelineConfig.from_settings(),
    )

    result = ReportStage().run(context)

    assert result.status == "success"
    md_path = Path(result.artifacts["analysis_report_md"])
    json_path = Path(result.artifacts["analysis_report_json"])
    assert md_path.exists()
    assert json_path.exists()


def test_report_stage_collects_errors_and_ignores_review_notes_list(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.append_issue(
        "trino_test",
        "runtime table missing",
        details={"part": 2},
    )
    state_manager.set_review_notes(["Inspect trino_test_report.json"])
    context = StageContext(
        query_name="demo",
        state_manager=state_manager,
        state={"query_name": "demo", "status": "review"},
        config=PipelineConfig.from_settings(),
    )

    result = ReportStage().run(context)

    assert result.status == "success"
    json_path = Path(result.artifacts["analysis_report_json"])
    report = json_path.read_text(encoding="utf-8")
    assert "runtime table missing" in report
    assert "trino_test" in report


def test_report_stage_includes_stage_fix_log_with_diff_preview(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.update_part_metadata(
        8,
        {
            "pattern_context": {
                "history": [
                    {
                        "resolved": True,
                        "source_version": 0,
                        "result_version": 1,
                        "found_patterns": [
                            {"id": "uncasted_parameter", "description": "uncasted parameter in predicate"}
                        ],
                        "change_details": {
                            "changed_lines": 2,
                            "diff_preview": [
                                "--- pattern_guard_old",
                                "+++ pattern_guard_new",
                                "-WHERE dt = $date_from",
                                "+WHERE dt = CAST($date_from AS DATE)",
                            ],
                        },
                    }
                ]
            }
        },
    )
    state_manager.append_knowledge(
        "fix_attempt_journal",
        {
            "stage": "runtime_test",
            "status": "saved",
            "target_part": 9,
            "error_or_diff_context": "Column 'coi.buyer_id' cannot be resolved",
            "summary": "Replaced unresolved column reference",
            "change_type": "line_patch",
            "edit_summary": [{"op": "replace_line", "line": 12, "changed_lines": 1}],
            "diff_summary": {
                "preview": [
                    "--- part_9_old",
                    "+++ part_9_new",
                    "-coi.buyer_id",
                    "+coi.seller_id",
                ]
            },
            "used_evidence": ["inspect_source_columns(coi)"],
            "old_version": 1,
            "new_version": 2,
        },
    )
    context = StageContext(
        query_name="demo",
        state_manager=state_manager,
        state={"query_name": "demo", "status": "initialized"},
        config=PipelineConfig.from_settings(),
    )

    result = ReportStage().run(context)

    md_path = Path(result.artifacts["analysis_report_md"])
    json_path = Path(result.artifacts["analysis_report_json"])
    md_report = md_path.read_text(encoding="utf-8")
    json_report = json.loads(json_path.read_text(encoding="utf-8"))

    assert "## Stage Fix Log" in md_report
    assert "### STAGE pattern_guard" in md_report
    assert "### STAGE trino_test" in md_report
    assert "-coi.buyer_id" in md_report
    assert "+coi.seller_id" in md_report
    assert "CAST($date_from AS DATE)" in md_report
    assert json_report["stage_fix_log"]["trino_test"][0]["diff_preview"][2] == "-coi.buyer_id"
    assert json_report["stage_fix_log"]["pattern_guard"][0]["diff_preview"][3] == "+WHERE dt = CAST($date_from AS DATE)"


def test_report_stage_skips_pattern_guard_entries_without_real_changes(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)
    state_manager = StateManager("demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.update_part_metadata(
        8,
        {
            "pattern_context": {
                "history": [
                    {
                        "resolved": True,
                        "source_version": 0,
                        "result_version": 1,
                        "found_patterns": [
                            {"id": "uncasted_parameter", "description": "uncasted parameter in predicate"}
                        ],
                        "change_details": {
                            "changed_lines": 0,
                            "diff_preview": ["--- pattern_guard_old", "+++ pattern_guard_new"],
                        },
                    }
                ]
            }
        },
    )
    context = StageContext(
        query_name="demo",
        state_manager=state_manager,
        state={"query_name": "demo", "status": "initialized"},
        config=PipelineConfig.from_settings(),
    )

    result = ReportStage().run(context)

    md_path = Path(result.artifacts["analysis_report_md"])
    json_path = Path(result.artifacts["analysis_report_json"])
    md_report = md_path.read_text(encoding="utf-8")
    json_report = json.loads(json_path.read_text(encoding="utf-8"))

    assert "part_8" not in md_report
    assert "pattern_guard" not in json_report.get("stage_fix_log", {})


def test_translator_rejects_single_line_sql_longer_than_50_chars():
    translator = PartTranslator(compiled_module=object(), state_manager=None)
    sql = "SELECT user_id, created_at, updated_at, status FROM analytics_src.some_table"

    is_valid, error = translator._validate_output(sql)

    assert not is_valid
    assert "single line longer than 50 characters" in error


def test_translator_passes_validation_feedback_on_retry(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("dma_demo", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_total_parts(1)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.vertica_parts_path / "dma_demo_part_0.sql").write_text("SELECT 1", encoding="utf-8")

    class FakeModule:
        def __init__(self):
            self.contexts = []

        def __call__(self, vertica_sql, context_hint):
            self.contexts.append(context_hint)
            if len(self.contexts) == 1:
                return type("Result", (), {"trino_sql": "SELECT user_id, created_at, updated_at, status FROM analytics_src.some_table"})()
            return type("Result", (), {"trino_sql": "SELECT 1\nFROM analytics_src.some_table"})()

    module = FakeModule()
    translator = PartTranslator(compiled_module=module, state_manager=state_manager)
    success, error = translator.translate_part(0)

    assert success is True
    assert error is None
    assert len(module.contexts) == 2
    assert "=== RETRY FEEDBACK ===" in module.contexts[1]
    assert "single line longer than 50 characters" in module.contexts[1]
