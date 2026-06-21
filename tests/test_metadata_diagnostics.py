from core.state_manager import StateManager


def test_state_manager_valid_statuses_match_workflow_states():
    assert "review" in StateManager.VALID_STATUSES
    assert "failed" not in StateManager.VALID_STATUSES


def test_mark_final_status_review(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("demo_query", base_path=workflow_root / "in_progress")
    state_manager.initialize()

    state_manager.mark_final_status("review", "Moved to review")

    state = state_manager.load_state()
    assert state["status"] == "review"
    assert state["final_status"] == "review"
    assert state["error_msg"] == "Moved to review"
    assert state["completed_at"] is not None


def test_state_manager_persists_diagnostics(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("demo_query", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.append_issue(
        "formatter",
        "Formatting failed for demo_part.sql",
        severity="warning",
        details={"file": "demo_part.sql", "message": "sqlfluff parsing error"},
    )
    state_manager.set_review_notes(["Check diagnostics.formatter.errors", "Inspect formatter errors"])

    state = state_manager.load_state()
    diagnostics = state["diagnostics"]

    assert diagnostics["review_notes"] == ["Check diagnostics.formatter.errors", "Inspect formatter errors"]
    assert len(diagnostics["formatter"]["errors"]) == 1
    assert diagnostics["formatter"]["errors"][0]["stage"] == "formatter"
    assert diagnostics["formatter"]["errors"][0]["severity"] == "warning"


def test_state_manager_persists_stage_status_and_details(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("demo_query", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.set_stage_status(
        "api_validation",
        "warning",
        details={"remaining_errors": 2, "validation_report_url": "http://example.test/report"},
    )

    state = state_manager.load_state()
    diagnostics = state["diagnostics"]

    assert diagnostics["api_validation"]["status"] == "warning"
    assert diagnostics["api_validation"]["details"]["remaining_errors"] == 2
    assert diagnostics["api_validation"]["details"]["validation_report_url"] == "http://example.test/report"
    assert diagnostics["api_validation"]["updated_at"] is not None


def test_set_total_parts_does_not_change_workflow_status(tmp_path, monkeypatch):
    workflow_root = tmp_path / "workflow"
    monkeypatch.chdir(tmp_path)

    state_manager = StateManager("demo_query", base_path=workflow_root / "in_progress")
    state_manager.initialize()
    state_manager.update_state({"status": "splitting"})

    state_manager.set_total_parts(3)

    state = state_manager.load_state()
    assert state["total_parts"] == 3
    assert state["status"] == "splitting"
