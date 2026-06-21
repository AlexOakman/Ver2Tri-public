"""
Trino-only result comparison stage.

This stage intentionally runs after technical runtime testing.  It compares the
runtime-created Trino target with the original Trino replica and records a
machine-readable report for later debugger-agent improvements.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from config import settings
from core.state_manager import StateManager
from core.trino_tester import (
    HeaderMetadata,
    RuntimeVariableResolver,
    TrinoConnectionFactory,
    TrinoRuntimeTester,
)


class TrinoCompareAgent:
    """Runs compare in Trino and records a focused compare report."""

    def __init__(
        self,
        state_manager: StateManager,
        connection: Optional[Any] = None,
        connection_factory: Optional[TrinoConnectionFactory] = None,
    ):
        self.state_manager = state_manager
        self.query_name = state_manager.query_name
        self.connection = connection
        self.connection_factory = connection_factory or TrinoConnectionFactory()
        self.runtime_schema = settings.trino_schema or settings.trino_test_schema
        self._tester = TrinoRuntimeTester(
            state_manager,
            connection=connection,
            connection_factory=self.connection_factory,
        )
        self._tester.runtime_schema = self.runtime_schema

    def run(self) -> Tuple[bool, Dict[str, Any]]:
        report: Dict[str, Any] = {
            "success": False,
            "stage": "compare",
            "schema": self.runtime_schema,
            "started_at": datetime.utcnow().isoformat(),
            "compare": {},
            "reconciliation_analysis": {},
            "event_log": [],
            "compare_sessions": [],
        }
        self._update_compare_runtime(current_phase="bootstrap", last_event="compare_started")
        try:
            self._log_event(report, "compare_started")
            header = self._load_compare_header()
            resolver = self._build_resolver(header)
            compare = self._run_compare_query(header, resolver)
            compare["column_trace"] = self._trace_mismatch_columns(compare)
            reconciliation = self._build_reconciliation(compare)
            report["compare"] = compare
            report["reconciliation_analysis"] = reconciliation
            report["success"] = compare.get("success", False)
            report["finished_at"] = datetime.utcnow().isoformat()
            self._log_event(report, "compare_finished", success=report["success"], compare_summary=compare)
            self._update_compare_runtime(
                current_phase="finished",
                diff_summary=compare,
                root_cause_summary=reconciliation,
                last_event="compare_finished",
            )
            self._write_report(report)
            return report["success"], report
        except Exception as exc:
            report["error"] = str(exc)
            report["finished_at"] = datetime.utcnow().isoformat()
            self._log_event(report, "compare_failed", error=str(exc))
            self._update_compare_runtime(current_phase="failed", root_cause_summary=str(exc), last_event="compare_failed")
            self._write_report(report)
            return False, report

    def _load_compare_header(self) -> HeaderMetadata:
        return self._tester._load_header()

    def _build_resolver(self, header: HeaderMetadata) -> RuntimeVariableResolver:
        state = self.state_manager.load_state() or {}
        runtime = state.get("test_runtime") or {}
        part_sql_by_num = self._load_latest_parts()
        resolver = RuntimeVariableResolver.from_sql_parts(header, part_sql_by_num.values())
        declared = runtime.get("declared_variables")
        if isinstance(declared, dict):
            resolver.values.update(declared)
        return resolver

    def _load_latest_parts(self) -> Dict[int, str]:
        return self._tester._load_latest_parts()

    def _run_compare_query(self, header: HeaderMetadata, resolver: RuntimeVariableResolver) -> Dict[str, Any]:
        connection = self.connection or self.connection_factory.connect()
        return self._tester._compare(connection, header, resolver)

    def _build_reconciliation(self, compare: Dict[str, Any]) -> Dict[str, Any]:
        return self._tester._analyze_reconciliation(compare)

    def _write_report(self, report: Dict[str, Any]) -> None:
        path = self.state_manager.work_dir / "reports" / "compare_report.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    def _trace_mismatch_columns(self, compare: Dict[str, Any]) -> Dict[str, Any]:
        mismatches = compare.get("metric_mismatches")
        if not isinstance(mismatches, dict) or not mismatches:
            return {}
        state = self.state_manager.load_state() or {}
        memory = ((state.get("parts") or {}).get("intent_memory") or {})
        trace: Dict[str, Any] = {}
        for column in list(mismatches)[:20]:
            candidates = []
            for part_key, intent in memory.items():
                text = json.dumps(intent, ensure_ascii=False, default=str).lower()
                if column.lower() in text:
                    candidates.append(
                        {
                            "part": part_key,
                            "creates_tables": intent.get("creates_tables", []),
                            "reads_tables": intent.get("reads_tables", []),
                            "output_columns": intent.get("output_columns", []),
                        }
                    )
            trace[column] = candidates[:10]
        return trace

    def _log_event(self, report: Dict[str, Any], event: str, **payload: Any) -> None:
        item = {"event": event, "timestamp": datetime.utcnow().isoformat(), **payload}
        report.setdefault("event_log", []).append(item)
        self._update_compare_runtime(last_event=item)

    def _update_compare_runtime(self, **updates: Any) -> None:
        self.state_manager.update_section("compare_runtime", updates)
