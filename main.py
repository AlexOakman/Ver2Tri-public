#!/usr/bin/env python3
"""
Ver2Tri - Vertica to Trino SQL Migration Agent.

Runtime orchestration is intentionally PipelineRunner-only. The CLI owns
workflow setup/finalization; core stages own ETL behavior.
"""

from __future__ import annotations

import argparse
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Dict, Literal, Optional, TypedDict

from config import settings
from core.assembler import Assembler
from core.pipeline import PipelineRunner
from core.state_manager import StateManager


_dashboard_process = None
_dashboard_url = None
_pipeline_runner: Optional[PipelineRunner] = None


class MigrationState(TypedDict):
    """Runtime state for one SQL migration."""

    query_name: str
    current_part: int
    total_parts: int
    status: Literal[
        "initialized",
        "splitting",
        "translating",
        "validating",
        "assembling",
        "completed",
        "review",
    ]
    error_msg: Optional[str]
    metadata_path: Optional[str]
    dspy_lm_initialized: bool


def _find_free_dashboard_port() -> Optional[int]:
    for candidate in range(8501, 8510):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            if sock.connect_ex(("localhost", candidate)) != 0:
                return candidate
    return None


def _build_dashboard_command(dashboard_path: Path, port: int) -> list[str]:
    return [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(dashboard_path),
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--server.maxUploadSize",
        "10",
        "--browser.gatherUsageStats",
        "false",
        "--theme.base",
        "dark",
    ]


def _start_dashboard_process(cmd: list[str]) -> subprocess.Popen[bytes]:
    if sys.platform == "win32":
        return subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
    return subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )


def _open_dashboard_browser(url: str) -> None:
    time.sleep(3)
    webbrowser.open(url)


def launch_dashboard() -> None:
    """Starts Streamlit dashboard in a background process."""
    global _dashboard_process, _dashboard_url

    dashboard_path = Path(__file__).parent / "dashboard.py"
    if not dashboard_path.exists():
        print("[WARN] dashboard.py not found; dashboard will not be started")
        return

    port = _find_free_dashboard_port()
    if port is None:
        print("[WARN] All ports 8501-8509 are busy. Open manually: streamlit run dashboard.py")
        return

    try:
        _dashboard_process = _start_dashboard_process(_build_dashboard_command(dashboard_path, port))
        _dashboard_url = f"http://localhost:{port}"
        print(f"\n{'=' * 60}")
        print(f"Dashboard started: {_dashboard_url}")
        print("Auto-refresh every 2 seconds")
        print(f"{'=' * 60}\n")
        threading.Thread(target=_open_dashboard_browser, args=(_dashboard_url,), daemon=True).start()
    except Exception as exc:
        print(f"[WARN] Failed to start dashboard: {exc}")


def cleanup_dashboard() -> None:
    """Stops dashboard process started by this CLI."""
    global _dashboard_process
    if _dashboard_process:
        try:
            print("\nStopping dashboard...")
            _dashboard_process.terminate()
            _dashboard_process.wait(timeout=3)
        except Exception:
            _dashboard_process.kill()


def signal_handler(sig, frame) -> None:  # type: ignore[no-untyped-def]
    cleanup_dashboard()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def get_pipeline_runner() -> PipelineRunner:
    """Singleton PipelineRunner for CLI and dashboard retry calls."""
    global _pipeline_runner
    if _pipeline_runner is None:
        from core.pipeline_stages import build_pipeline_runner

        _pipeline_runner = build_pipeline_runner()
    return _pipeline_runner


def _build_migration_state(
    query_name: str,
    state_manager: StateManager,
    *,
    status: str,
    saved_state: Optional[Dict[str, object]] = None,
) -> MigrationState:
    saved_state = saved_state or state_manager.load_state() or {}
    return MigrationState(
        query_name=query_name,
        current_part=saved_state.get("current_part", 0),
        total_parts=saved_state.get("total_parts", 0),
        status=status,  # type: ignore[typeddict-item]
        error_msg=None,
        metadata_path=str(state_manager.metadata_path),
        dspy_lm_initialized=False,
    )


def _copy_source_to_work_dir(query_name: str, state_manager: StateManager) -> None:
    src_file = settings.in_queue_path / f"{query_name}.sql"
    dst_file = state_manager.work_dir / f"{query_name}.sql"
    if src_file.exists() and not dst_file.exists():
        shutil.copy2(src_file, dst_file)
        print("[Init] File copied to in_progress")


def initialize_workflow(query_name: str) -> MigrationState:
    """
    Prepare workflow files and return runtime state.

    This is intentionally outside PipelineRunner: initialization is filesystem
    setup, not an ETL stage.
    """
    print(f"\n{'=' * 60}")
    print(f"Start processing: {query_name}")
    print(f"{'=' * 60}")

    state_manager = StateManager(query_name)
    existing_state = state_manager.load_state()

    if existing_state and existing_state.get("status") == "completed":
        print("[Init] File was already completed. Skipping.")
        return _build_migration_state(query_name, state_manager, status="completed", saved_state=existing_state)

    if existing_state and existing_state.get("status") not in {"completed", "review"}:
        current_part = existing_state.get("current_part", 0)
        total_parts = existing_state.get("total_parts", 0)
        status = existing_state.get("status", "initialized")
        print(f"[Init] Resuming progress: part {current_part}/{total_parts}, status={status}")
        return _build_migration_state(query_name, state_manager, status=status, saved_state=existing_state)

    if not existing_state:
        state_manager.initialize()
        _copy_source_to_work_dir(query_name, state_manager)

    return _build_migration_state(query_name, state_manager, status="initialized")


def finalize_workflow(state: MigrationState) -> MigrationState:
    """Move workflow artifacts into done or review storage."""
    query_name = state["query_name"]
    state_manager = StateManager(query_name)
    saved_state = state_manager.load_state() or {}
    final_decision = (saved_state.get("pipeline") or {}).get("final_decision")
    status = "review" if final_decision == "review" or state["status"] == "review" else "completed"

    print("\n[Finalize] Moving workflow artifacts...")
    try:
        assembler = Assembler(state_manager)
        if status == "review":
            assembler.finalize_workflow(move_to="review")
            print(f"[Finalize] File moved to review: {query_name}")
            return {**state, "status": "review"}

        assembler.finalize_workflow(move_to="done")
        print(f"[Finalize] Completed successfully: {query_name}")
        return {**state, "status": "completed"}
    except Exception as exc:
        print(f"[Finalize] Error: {exc}")
        return {**state, "status": "review", "error_msg": f"Finalization error: {exc}"}


def process_single_file(query_name: str, runner: Optional[PipelineRunner] = None) -> Dict[str, object]:
    """Process one queued SQL file through PipelineRunner-only orchestration."""
    settings.ensure_dirs()
    initial_state = initialize_workflow(query_name)
    if initial_state["status"] in {"completed", "review"}:
        return initial_state

    pipeline_runner = runner or get_pipeline_runner()
    final_state = pipeline_runner.run_pipeline(query_name, initial_state, start_stage="split")
    return finalize_workflow(final_state)  # type: ignore[arg-type]


def _build_runtime_state(query_name: str, state_manager: StateManager) -> MigrationState:
    """Build runtime state from metadata.json for manual retry."""
    saved_state = state_manager.load_state()
    return _build_migration_state(
        query_name,
        state_manager,
        status=(saved_state or {}).get("status", "initialized"),
        saved_state=saved_state,
    )


def _has_all_translated_parts(state_manager: StateManager, total_parts: int) -> bool:
    """Return True when every part has at least one Trino SQL version."""
    if total_parts <= 0:
        return False
    return all(state_manager.get_latest_version_path(part_num) is not None for part_num in range(total_parts))


def determine_retry_start_stage(query_name: str, requested_stage: str = "auto") -> str:
    """Choose a safe retry entry point."""
    if requested_stage != "auto":
        return requested_stage

    state_manager = StateManager(query_name)
    state = state_manager.load_state() or {}
    total_parts = state.get("total_parts", 0)
    diagnostics = state.get("diagnostics") or {}
    reports = state.get("reports") or {}
    test_runtime = state.get("test_runtime") or {}

    if _stage_needs_retry(diagnostics, "compare") or reports.get("compare_report"):
        return "compare"
    if (
        _stage_needs_retry(diagnostics, "trino_test")
        or reports.get("trino_test_report")
        or test_runtime.get("last_failed_part") is not None
        or test_runtime.get("last_error_text")
    ):
        return "trino_test"
    if _stage_needs_retry(diagnostics, "api_validation"):
        return "api_validate"
    if _stage_needs_retry(diagnostics, "pattern_guard"):
        return "pattern_guard"

    if total_parts <= 0:
        return "split"
    if _has_all_translated_parts(state_manager, total_parts):
        return "pattern_guard"
    return "translate"


def _stage_needs_retry(diagnostics: dict, stage: str) -> bool:
    block = diagnostics.get(stage)
    if not isinstance(block, dict):
        return False
    return block.get("status") in {"failed", "warning"}


def retry_file(query_name: str, requested_stage: str = "auto") -> Dict[str, object]:
    """Retry a workflow from an explicit PipelineRunner stage."""
    settings.ensure_dirs()

    state_manager = StateManager(query_name)
    if not state_manager.work_dir.exists():
        raise FileNotFoundError(f"Work directory not found: {state_manager.work_dir}")

    start_stage = determine_retry_start_stage(query_name, requested_stage)
    state_manager.prepare_for_retry(start_stage)

    report_path = state_manager.work_dir / "reports" / "api_validation_report.json"
    if report_path.exists():
        report_path.unlink()

    state = _build_runtime_state(query_name, state_manager)
    if start_stage in {"split", "translate"}:
        state["current_part"] = 0

    final_state = get_pipeline_runner().run_pipeline(query_name, state, start_stage=start_stage)
    return finalize_workflow(final_state)  # type: ignore[arg-type]


def scan_and_process() -> None:
    """Scan in_queue and process every SQL file."""
    settings.ensure_dirs()

    if not settings.in_queue_path.exists():
        print(f"Directory not found: {settings.in_queue_path}")
        return

    sql_files = list(settings.in_queue_path.glob("*.sql"))
    if not sql_files:
        print(f"No files to process in {settings.in_queue_path}")
        return

    print("Ver2Tri Migration Agent")
    print(f"Files found: {len(sql_files)}")

    results = []
    runner = get_pipeline_runner()
    for index, sql_file in enumerate(sql_files, 1):
        print(f"\n\nFile {index}/{len(sql_files)}: {sql_file.name}")
        final_state = process_single_file(sql_file.stem, runner)
        results.append(
            {
                "query_name": sql_file.stem,
                "status": final_state["status"],
                "error": final_state.get("error_msg"),
            }
        )
        if index < len(sql_files):
            print(f"\n{'=' * 60}")
            print("Moving to next file...")

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    successful = sum(1 for result in results if result["status"] == "completed")
    review = sum(1 for result in results if result["status"] == "review")
    print(f"Completed: {successful}")
    print(f"Review:    {review}")

    for result in results:
        if result["status"] != "completed":
            print(f"\n   {result['query_name']}: {result['status']}")
            if result["error"]:
                print(f"      Error: {str(result['error'])[:100]}...")


def reset_in_progress_states() -> int:
    """
    Recreate metadata.json for all workflow/in_progress tasks without touching SQL artifacts.
    """
    settings.ensure_dirs()

    reset_count = 0
    for work_dir in sorted(settings.in_progress_path.iterdir()):
        if not work_dir.is_dir():
            continue

        has_parts = (work_dir / "vertica_parts").exists() or (work_dir / "trino_parts").exists()
        has_metadata = (work_dir / "metadata.json").exists()
        if not has_parts and not has_metadata:
            continue

        query_name = work_dir.name
        state_manager = StateManager(query_name)
        state_manager.initialize()
        total_parts = len(list((work_dir / "vertica_parts").glob(f"{query_name}_part_*.sql")))
        if total_parts == 0:
            trino_part_pattern = re.compile(
                rf"^{re.escape(query_name)}_part_(?P<part_num>\d+)_trino(?:_v(?P<version>\d+))?\.sql$"
            )
            total_parts = len(
                {
                    int(match.group("part_num"))
                    for path in (work_dir / "trino_parts").glob(f"{query_name}_part_*_trino*.sql")
                    for match in [trino_part_pattern.match(path.name)]
                    if match
                }
            )
        if total_parts > 0:
            state_manager.set_total_parts(total_parts)

        for path in (work_dir / "reports").glob("*.json"):
            if path.exists():
                path.unlink()
        journal_path = work_dir / "logs" / "fix_attempt_journal.json"
        if journal_path.exists():
            journal_path.unlink()

        reset_count += 1
        print(f"State reset: {query_name}")

    return reset_count


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ver2Tri - Vertica to Trino SQL Migration Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --no-dashboard
  python main.py --file query_001
  python main.py --reset query_001
  python main.py --reset-in-progress-states
        """,
    )
    parser.add_argument("--file", type=str, help="Process one file")
    parser.add_argument("--reset", type=str, help="Reset file state")
    parser.add_argument("--retry", type=str, help="Retry file from workflow/in_progress")
    parser.add_argument(
        "--retry-from",
        type=str,
        choices=[
            "auto",
            "split",
            "translate",
            "pattern_guard",
            "format",
            "assemble",
            "api_validate",
            "trino_test",
            "compare",
            "report",
        ],
        default="auto",
        help="Entry point for retry",
    )
    parser.add_argument(
        "--reset-in-progress-states",
        action="store_true",
        help="Recreate metadata.json in workflow/in_progress/* without touching SQL parts",
    )
    parser.add_argument("--list", action="store_true", help="Show queue")
    parser.add_argument("--no-dashboard", action="store_true", help="Do not start web dashboard")
    return parser


def _maybe_launch_dashboard(args: argparse.Namespace) -> None:
    if not args.no_dashboard:
        launch_dashboard()
        time.sleep(1.5)


def _handle_list() -> None:
    files = list(settings.in_queue_path.glob("*.sql"))
    print("Files in queue:")
    for file_path in files:
        print(f"   - {file_path.name}")


def _handle_reset(query_name: str) -> None:
    state_manager = StateManager(query_name)
    state_manager.initialize()
    print(f"State reset for: {query_name}")


def _handle_reset_in_progress_states() -> None:
    reset_count = reset_in_progress_states()
    print(f"States reset in in_progress: {reset_count}")


def _print_result_banner(label: str, result: Dict[str, object]) -> None:
    print(f"\n{'=' * 60}")
    print(f"{label}: {result['status']}")
    if result.get("error_msg"):
        print(f"Error: {result['error_msg']}")


def _handle_retry(args: argparse.Namespace) -> None:
    result = retry_file(args.retry, args.retry_from)
    _print_result_banner("Retry result", result)


def _handle_process_request(args: argparse.Namespace) -> None:
    if args.file:
        result = process_single_file(args.file)
        _print_result_banner("Result", result)
        return
    scan_and_process()


def _handle_cli_args(args: argparse.Namespace) -> bool:
    if args.list:
        _handle_list()
        return True

    if args.reset:
        _handle_reset(args.reset)
        return True

    if args.reset_in_progress_states:
        _handle_reset_in_progress_states()
        return True

    if args.retry:
        _handle_retry(args)
        return True

    _handle_process_request(args)
    return False


def _print_completion_message(no_dashboard: bool) -> None:
    print(f"\n{'=' * 60}")
    if no_dashboard:
        print("Done.")
    else:
        print("Done. Dashboard remains open for exports, checks and manual retry.")
    print(f"{'=' * 60}")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _maybe_launch_dashboard(args)
    exited_early = _handle_cli_args(args)
    if exited_early:
        return
    _print_completion_message(args.no_dashboard)


if __name__ == "__main__":
    main()
