"""
Concrete stage implementations for the hybrid modular pipeline.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import settings
from core.api_validation_fixer import APIValidationFixer
from core.assembler import Assembler
from core.compare_agent import TrinoCompareAgent
from core.llm_profiles import ensure_no_proxy_for_llm
from core.formatter import TrinoFormatter
from core.migration_knowledge import PartIntentMemory
from core.pattern_guard import PatternGuard
from core.pipeline import BaseStage, PipelineConfig, PipelineRunner, StageContext, StageResult, StageStatus
from core.splitter import SQLSplitter
from core.state_manager import StateManager
from core.translator import PartTranslator
from core.trino_tester import TrinoRuntimeTester


def _ensure_dspy_lm() -> None:
    ensure_no_proxy_for_llm(settings.llm_base_url)


def _update_part_intent_memory(state_manager: StateManager, part_num: int) -> None:
    try:
        PartIntentMemory(state_manager).update_part(part_num)
    except Exception:
        pass


def _translation_progress_result(
    total_parts: int,
    *,
    next_part: Optional[int] = None,
    skipped_part: Optional[int] = None,
) -> StageResult:
    details: Dict[str, Any] = {"translated_parts": total_parts} if next_part is None else {"current_part": next_part}
    if skipped_part is not None:
        details["skipped"] = skipped_part
    updates = {
        "current_part": total_parts if next_part is None else next_part,
        "status": "translating",
    }
    return StageResult(status=StageStatus.SUCCESS, blocking=True, details=details, updates=updates)


def _get_next_part_with_interpolate_deps(
    state_manager: StateManager,
    current_part: int,
    visited: Optional[set[int]] = None,
    depth: int = 0,
) -> Optional[int]:
    if visited is None:
        visited = set()
    if depth > 100 or current_part in visited:
        return None
    visited.add(current_part)

    state = state_manager.load_state() or {}
    total_parts = state.get("total_parts", 0)
    parts_map = state.get("parts_map", {})
    pending_parts: List[tuple[int, Dict[str, Any]]] = []
    for index in range(total_parts):
        info = parts_map.get(f"part_{index}", {})
        if not info.get("translated"):
            pending_parts.append((index, info))

    if not pending_parts:
        return None

    for part_num, part_info in pending_parts:
        sources = part_info.get("interpolate_sources", [])
        all_sources_ready = True
        for src in sources:
            src_part = src["source_part"]
            src_info = parts_map.get(f"part_{src_part}", {})
            if not src_info.get("translated"):
                all_sources_ready = False
                if src_part in [item[0] for item in pending_parts]:
                    result = _get_next_part_with_interpolate_deps(state_manager, src_part, visited.copy(), depth + 1)
                    if result is not None:
                        return result
        if all_sources_ready:
            return part_num
    return None


class SplitStage:
    stage_id = "split"
    enabled_by_default = True
    blocking = True
    can_retry_from_here = True
    produced_artifacts: List[str] = []
    consumed_context_keys: List[str] = []

    def run(self, context: StageContext) -> StageResult:
        state_manager = context.state_manager
        state_manager.set_current_operation("🔍 Анализ структуры SQL", {"phase": "splitting"})
        splitter = SQLSplitter(state_manager)
        details: Dict[str, Any] = {}
        try:
            if splitter.should_skip():
                existing_state = state_manager.load_state() or {}
                total_parts = existing_state.get("total_parts", 0)
                parts_map = existing_state.get("parts_map", {})
            else:
                sql_file = state_manager.work_dir / f"{context.query_name}.sql"
                sql_content = sql_file.read_text(encoding="utf-8")
                parts = splitter.split(sql_content)
                splitter.save_parts(parts)
                total_parts = len(parts)
                parts_map = (state_manager.load_state() or {}).get("parts_map", {})

            dependencies = {}
            for part_key, info in parts_map.items():
                dependencies[part_key] = {
                    "interpolate_dependency": info.get("interpolate_sources", []),
                    "table_dependency": info.get("dependencies", {}).get("external_deps", []),
                    "execution_order_hint": info.get("dependencies", {}).get("creates", []),
                }
            state_manager.set_total_parts(total_parts)
            state_manager.update_section("parts", {"dependencies": dependencies})
            details = {"total_parts": total_parts, "dependencies_source": "metadata.json:parts.dependencies"}
            return StageResult(
                status=StageStatus.SUCCESS,
                blocking=True,
                details=details,
                artifacts={},
                updates={"total_parts": total_parts, "status": "splitting"},
            )
        except Exception as exc:
            return StageResult(
                status=StageStatus.FAILED,
                blocking=True,
                issues=[{"message": f"Split error: {exc}", "details": details}],
                details={"error": str(exc)},
            )


class TranslateStage:
    stage_id = "translate"
    enabled_by_default = True
    blocking = True
    can_retry_from_here = True
    produced_artifacts = []
    consumed_context_keys = ["parts.dependencies"]

    def _advance_to_next_part(self, state_manager: StateManager, current_part: int, total_parts: int) -> StageResult:
        next_part = _get_next_part_with_interpolate_deps(state_manager, current_part)
        if next_part is None:
            return _translation_progress_result(total_parts)
        return _translation_progress_result(total_parts, next_part=next_part, skipped_part=current_part)

    def run(self, context: StageContext) -> StageResult:
        state = context.state
        state_manager = context.state_manager
        state_manager.clear_stage_issues("translate")
        current_part = state.get("current_part", 0)
        total_parts = state.get("total_parts", 0)
        state_manager.set_current_operation(
            f"🌐 Перевод Part {current_part}/{total_parts}",
            {"part": current_part, "total": total_parts, "phase": "translating"},
        )

        if total_parts > 0 and current_part >= total_parts:
            result = _translation_progress_result(total_parts)
            result.details["skipped"] = "current_part_at_or_after_total_parts"
            return result

        translator = PartTranslator(state_manager=state_manager)

        part_status = state_manager.get_part_status(current_part)
        if part_status.get("translated"):
            return self._advance_to_next_part(state_manager, current_part, total_parts)

        success, error = translator.translate_part(current_part)
        if not success:
            return StageResult(
                status=StageStatus.FAILED,
                blocking=True,
                issues=[
                    {
                        "message": f"Part {current_part} translation failed",
                        "details": {"part_num": current_part, "error": error},
                    }
                ],
                details={"failed_part": current_part, "error": error},
                review_notes=[
                    "Проверь diagnostics.translate.errors",
                    f"Открой trino_parts для part_{current_part} и сравни с vertica_parts",
                ],
            )

        self._write_translation_context(state_manager, current_part)
        _update_part_intent_memory(state_manager, current_part)
        next_part = _get_next_part_with_interpolate_deps(state_manager, current_part)
        if next_part is None:
            return _translation_progress_result(total_parts)
        return _translation_progress_result(total_parts, next_part=next_part)

    def _write_translation_context(self, state_manager: StateManager, part_num: int) -> None:
        part_meta = state_manager.get_part_metadata(part_num)
        path = state_manager.work_dir / "logs" / f"translation_context_part_{part_num}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(part_meta, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


class PatternGuardStage:
    stage_id = "pattern_guard"
    enabled_by_default = True
    blocking = True
    can_retry_from_here = True
    produced_artifacts = []
    consumed_context_keys = ["parts_map"]

    def _record_pattern_guard_history(
        self,
        state_manager: StateManager,
        part_num: int,
        found_patterns: List[Dict[str, Any]],
        success: bool,
    ) -> None:
        state_manager.append_knowledge(
            "pattern_guard_history",
            {
                "part_num": part_num,
                "patterns": found_patterns,
                "resolved": success,
                "timestamp": datetime.utcnow().isoformat(),
            },
            key=f"part_{part_num}",
        )

    @staticmethod
    def _failed_pattern_part(part_num: int, found_patterns: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "part_num": part_num,
            "patterns": [
                {"id": item.get("id"), "description": item.get("description")}
                for item in found_patterns
            ],
        }

    def run(self, context: StageContext) -> StageResult:
        state_manager = context.state_manager
        total_parts = context.state.get("total_parts", 0)
        state_manager.clear_stage_issues("pattern_guard")
        guard = PatternGuard(state_manager=state_manager)
        failed_parts: List[Dict[str, Any]] = []

        try:
            for part_num in range(total_parts):
                state_manager.set_current_operation(
                    f"🛡️ Проверка паттернов Part {part_num}/{total_parts}",
                    {"part": part_num, "total": total_parts, "phase": "validating"},
                )
                part_status = state_manager.get_part_status(part_num)
                if part_status.get("validated"):
                    continue

                is_clean, found_patterns = guard.check_patterns(part_num)
                if is_clean:
                    state_manager.set_part_status(
                        part_num,
                        "validated",
                        {"fix_version": state_manager.get_latest_version_number(part_num)},
                    )
                else:
                    success, _ = guard.fix_patterns(part_num, found_patterns)
                    self._record_pattern_guard_history(state_manager, part_num, found_patterns, success)
                    if not success:
                        failed_parts.append(self._failed_pattern_part(part_num, found_patterns))
                _update_part_intent_memory(state_manager, part_num)

            if failed_parts:
                return StageResult(
                    status=StageStatus.FAILED,
                    blocking=True,
                    issues=[
                        {
                            "message": f"Part {item['part_num']} failed pattern validation",
                            "details": item,
                        }
                        for item in failed_parts
                    ],
                    details={"failed_parts": [item["part_num"] for item in failed_parts]},
                    review_notes=[
                        "Проверь parts со статусом pattern_error в metadata.parts_map",
                        "Посмотри diagnostics для списка найденных запрещенных паттернов",
                    ],
                )
            return StageResult(status=StageStatus.SUCCESS, blocking=True, details={"validated_parts": total_parts})
        except Exception as exc:
            return StageResult(
                status=StageStatus.FAILED,
                blocking=True,
                issues=[{"message": f"PatternGuard error: {exc}", "details": {"error": str(exc)}}],
                details={"error": str(exc)},
            )


class FormatStage:
    stage_id = "format"
    enabled_by_default = True
    blocking = False
    can_retry_from_here = True
    produced_artifacts = []
    consumed_context_keys = ["parts_map"]

    def run(self, context: StageContext) -> StageResult:
        formatter = TrinoFormatter()
        state_manager = context.state_manager
        total_parts = context.state.get("total_parts", 0)
        try:
            formatted_count, error_count = formatter.format_parts(
                parts_dir=state_manager.trino_parts_path,
                query_name=context.query_name,
                total_parts=total_parts,
            )
            issues = [
                {
                    "message": f"Formatting failed for {Path(issue['file']).name}",
                    "severity": "warning",
                    "details": issue,
                }
                for issue in formatter.last_errors
            ]
            status = StageStatus.WARNING if error_count > 0 else StageStatus.SUCCESS
            details = {"formatted_count": formatted_count, "error_count": error_count}
            return StageResult(status=status, blocking=False, issues=issues, details=details)
        except Exception as exc:
            return StageResult(
                status=StageStatus.WARNING,
                blocking=False,
                issues=[{"message": f"Formatting stage failed: {exc}", "severity": "warning", "details": {"error": str(exc)}}],
                details={"error": str(exc)},
            )


class AssembleStage:
    stage_id = "assemble"
    enabled_by_default = True
    blocking = True
    can_retry_from_here = True
    produced_artifacts = ["final_sql"]
    consumed_context_keys = ["parts_map"]

    def run(self, context: StageContext) -> StageResult:
        assembler = Assembler(context.state_manager)
        try:
            final_path = assembler.assemble_final()
            return StageResult(
                status=StageStatus.SUCCESS,
                blocking=True,
                details={
                    "final_sql": str(final_path),
                },
                artifacts={"final_sql": str(final_path)},
            )
        except Exception as exc:
            return StageResult(
                status=StageStatus.FAILED,
                blocking=True,
                issues=[{"message": f"Assembly error: {exc}", "details": {"error": str(exc)}}],
                details={"error": str(exc)},
            )


class APIValidateStage:
    stage_id = "api_validate"
    enabled_by_default = True
    blocking = False
    can_retry_from_here = True
    produced_artifacts = ["api_validation_report"]
    consumed_context_keys = ["knowledge.pattern_guard_history"]

    def run(self, context: StageContext) -> StageResult:
        fixer = APIValidationFixer(context.state_manager)
        context.state_manager.set_current_operation("🔌 API валидация SQL", {"phase": "api_validating"})
        try:
            success, report = fixer.validate_and_fix(max_iterations=settings.pattern_fix_max_iter)
            report_path = context.state_manager.work_dir / "reports" / "api_validation_report.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
            context.state_manager.register_report("api_validation_report", str(report_path))
            context.state_manager.append_knowledge(
                "api_validation_history",
                {
                    "success": success,
                    "remaining_errors": report.get("remaining_errors", []),
                    "timestamp": datetime.utcnow().isoformat(),
                },
                key="global",
            )
            if success:
                return StageResult(
                    status=StageStatus.SUCCESS,
                    blocking=False,
                    artifacts={"api_validation_report": str(report_path)},
                    details={"validation_report_url": report.get("validation_report_url")},
                )

            remaining = report.get("remaining_errors", [])
            issues = [
                {
                    "message": error.get("title") or error.get("name") or "API validation error",
                    "severity": "warning",
                    "details": error,
                }
                for error in remaining
            ]
            return StageResult(
                status=StageStatus.WARNING,
                blocking=False,
                issues=issues,
                artifacts={"api_validation_report": str(report_path)},
                details={
                    "remaining_errors": len(remaining),
                    "validation_report_url": report.get("validation_report_url"),
                },
            )
        except Exception as exc:
            return StageResult(
                status=StageStatus.WARNING,
                blocking=False,
                issues=[{"message": f"API validation error: {exc}", "severity": "warning", "details": {"error": str(exc)}}],
                details={"error": str(exc)},
            )


class TrinoTestStage:
    stage_id = "trino_test"
    enabled_by_default = True
    blocking = True
    can_retry_from_here = True
    produced_artifacts = ["trino_test_report"]
    consumed_context_keys = ["knowledge.pattern_guard_history", "knowledge.api_validation_history"]

    def run(self, context: StageContext) -> StageResult:
        tester = TrinoRuntimeTester(context.state_manager)
        context.state_manager.set_current_operation("🧪 Trino runtime test", {"phase": "trino_testing"})
        success, report = tester.run()
        report_path = context.state_manager.work_dir / "reports" / "trino_test_report.json"
        context.state_manager.register_report("trino_test_report", str(report_path))
        context.state_manager.update_section(
            "test_runtime",
            {
                "executed_parts": report.get("part_execution", []),
                "created_runtime_tables": report.get("runtime_tables", []),
                "declared_variables": report.get("variables", {}),
                "last_failed_part": report.get("failed_part"),
                "last_error_text": report.get("error"),
            },
        )
        context.state_manager.append_knowledge(
            "runtime_test_history",
            {
                "success": success,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )
        if success:
            details = {
                "report_path": str(report_path),
                "schema": report.get("schema"),
            }
            if report.get("warnings"):
                return StageResult(status=StageStatus.WARNING, blocking=True, details=details, artifacts={"trino_test_report": str(report_path)})
            return StageResult(status=StageStatus.SUCCESS, blocking=True, details=details, artifacts={"trino_test_report": str(report_path)})
        error_message = report.get("error") or "Trino runtime test or comparison failed"
        return StageResult(
            status=StageStatus.FAILED,
            blocking=context.config.trino_test_blocking,
            issues=[{"message": error_message, "details": report}],
            artifacts={"trino_test_report": str(report_path)},
            details={
                "report_path": str(report_path),
                "schema": report.get("schema"),
            },
            review_notes=[
                "Открой trino_test_report.json и проверь failed part",
                "Проверь TRINO_SCHEMA и доступы к Trino в .env",
                "Если ошибка в SQL, повтори запуск с --retry-from trino_test после правки/latest fix",
            ],
        )


class CompareStage:
    stage_id = "compare"
    enabled_by_default = True
    blocking = True
    can_retry_from_here = True
    produced_artifacts = ["compare_report"]
    consumed_context_keys = ["test_runtime", "knowledge.fix_attempt_journal", "parts.intent_memory"]

    def run(self, context: StageContext) -> StageResult:
        agent = TrinoCompareAgent(context.state_manager)
        context.state_manager.set_current_operation("⚖️ Trino compare", {"phase": "compare"})
        success, report = agent.run()
        report_path = context.state_manager.work_dir / "reports" / "compare_report.json"
        context.state_manager.register_report("compare_report", str(report_path))
        context.state_manager.update_section(
            "test_runtime",
            {
                "reconciliation_summary": report.get("reconciliation_analysis", {}),
            },
        )
        context.state_manager.append_knowledge(
            "runtime_test_history",
            {
                "stage": "compare",
                "success": success,
                "compare": report.get("compare", {}),
                "reconciliation_analysis": report.get("reconciliation_analysis", {}),
                "timestamp": datetime.utcnow().isoformat(),
            },
        )
        details = {
            "report_path": str(report_path),
            "schema": report.get("schema"),
            "compare": report.get("compare", {}),
            "reconciliation_analysis": report.get("reconciliation_analysis", {}),
        }
        if success:
            return StageResult(status=StageStatus.SUCCESS, blocking=True, details=details, artifacts={"compare_report": str(report_path)})
        return StageResult(
            status=StageStatus.FAILED,
            blocking=context.config.compare_blocking,
            issues=[{"message": report.get("error") or "Trino compare failed", "details": report}],
            artifacts={"compare_report": str(report_path)},
            details=details,
            review_notes=[
                "Открой compare_report.json и проверь diff_summary/root_cause_summary",
                "Если технический runtime test прошел, но compare не сошелся, исправления лучше начинать с trace нужной колонки к part",
            ],
        )


class ReportStage:
    stage_id = "report"
    enabled_by_default = True
    blocking = False
    can_retry_from_here = True
    produced_artifacts = ["analysis_report_md", "analysis_report_json"]
    consumed_context_keys = ["diagnostics", "reports", "knowledge", "test_runtime", "compare_runtime"]

    @staticmethod
    def _trim_text(value: Any, limit: int = 500) -> str:
        text = str(value or "").strip()
        return text[:limit]

    @classmethod
    def _extract_stage_fix_log(cls, state: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        parts_map = state.get("parts_map") or {}
        knowledge = state.get("knowledge") or {}
        stage_fix_log = {
            "pattern_guard": cls._pattern_guard_fix_log(parts_map),
            "api_validation": cls._api_validation_fix_log(knowledge),
            "trino_test": cls._trino_test_fix_log(knowledge),
        }
        return {stage: items for stage, items in stage_fix_log.items() if items}

    @classmethod
    def _pattern_guard_fix_log(cls, parts_map: Dict[str, Any]) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for part_key, part_info in sorted(parts_map.items()):
            if not isinstance(part_info, dict):
                continue
            part_num = part_key.replace("part_", "")
            history = ((part_info.get("pattern_context") or {}).get("history") or [])
            for entry in history:
                if not isinstance(entry, dict) or not entry.get("resolved"):
                    continue
                found_patterns = entry.get("found_patterns") or []
                change_details = entry.get("change_details") or {}
                changed_lines = int(change_details.get("changed_lines", 0) or 0)
                diff_preview = change_details.get("diff_preview", []) or []
                if changed_lines <= 0 and not any(
                    isinstance(line, str) and line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
                    for line in diff_preview
                ):
                    continue
                entries.append(
                    {
                        "part": int(part_num) if str(part_num).isdigit() else part_num,
                        "problem": "; ".join(
                            pattern.get("description", pattern.get("id", "forbidden pattern"))
                            for pattern in found_patterns
                            if isinstance(pattern, dict)
                        ) or "Forbidden Vertica pattern detected",
                        "solution_summary": "Removed forbidden pattern(s) with a minimal SQL rewrite",
                        "change_type": "pattern_guard_fix",
                        "edit_summary": [{"op": "sql_diff", "changed_lines": changed_lines}],
                        "diff_preview": diff_preview,
                        "used_evidence": [pattern.get("id") for pattern in found_patterns if isinstance(pattern, dict) and pattern.get("id")],
                        "source_version": entry.get("source_version"),
                        "result_version": entry.get("result_version"),
                        "status": "fixed",
                    }
                )
        return entries

    @classmethod
    def _api_validation_fix_log(cls, knowledge: Dict[str, Any]) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        history = knowledge.get("api_validation_history") or {}
        for part_key, payload in sorted(history.items()):
            if not isinstance(payload, dict):
                continue
            for fix in payload.get("fixes_applied", []) or []:
                if not isinstance(fix, dict):
                    continue
                part_num = fix.get("part")
                solution_summary = []
                if fix.get("type"):
                    solution_summary.append(str(fix["type"]))
                if fix.get("error"):
                    solution_summary.append(f"error={fix['error']}")
                if fix.get("count") is not None:
                    solution_summary.append(f"count={fix['count']}")
                entries.append(
                    {
                        "part": part_num if part_num is not None else part_key,
                        "problem": payload.get("remaining_errors") or "API validation fix applied",
                        "solution_summary": ", ".join(solution_summary) or "API validation auto-fix applied",
                        "change_type": fix.get("type", "api_validation_fix"),
                        "edit_summary": [],
                        "diff_preview": [],
                        "used_evidence": [],
                        "status": "fixed",
                    }
                )
        return entries

    @classmethod
    def _trino_test_fix_log(cls, knowledge: Dict[str, Any]) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for fix in knowledge.get("fix_attempt_journal", []) or []:
            if not isinstance(fix, dict) or fix.get("stage") != "runtime_test" or fix.get("status") != "saved":
                continue
            entries.append(
                {
                    "part": fix.get("target_part"),
                    "problem": cls._trim_text(fix.get("error_or_diff_context")),
                    "solution_summary": fix.get("summary") or fix.get("planner_summary") or "Runtime fix saved",
                    "change_type": fix.get("change_type", "runtime_test_fix"),
                    "edit_summary": fix.get("edit_summary", []),
                    "diff_preview": ((fix.get("diff_summary") or {}).get("preview") or []),
                    "used_evidence": fix.get("used_evidence", []),
                    "source_version": fix.get("old_version"),
                    "result_version": fix.get("new_version"),
                    "status": fix.get("status"),
                }
            )
        return entries

    @classmethod
    def _append_stage_fix_log_markdown(cls, md_lines: List[str], stage_fix_log: Dict[str, List[Dict[str, Any]]]) -> None:
        md_lines.extend(["", "## Stage Fix Log"])
        if not stage_fix_log:
            md_lines.append("- No recorded automatic fixes")
            return

        for stage_name, entries in stage_fix_log.items():
            md_lines.extend(["", f"### STAGE {stage_name}"])
            for entry in entries:
                md_lines.append(f"- part_{entry.get('part')}")
                md_lines.append(f"  Problem: {cls._trim_text(entry.get('problem'), 800) or 'n/a'}")
                md_lines.append(f"  Solution: {cls._trim_text(entry.get('solution_summary'), 800) or 'n/a'}")
                if entry.get("change_type"):
                    md_lines.append(f"  Change type: `{entry['change_type']}`")
                if entry.get("edit_summary"):
                    md_lines.append(f"  Edit summary: `{json.dumps(entry['edit_summary'], ensure_ascii=False, default=str)}`")
                diff_preview = entry.get("diff_preview") or []
                if diff_preview:
                    md_lines.append("  Diff preview:")
                    for line in diff_preview[:20]:
                        md_lines.append(f"    {line}")
                if entry.get("used_evidence"):
                    md_lines.append(f"  Evidence: `{json.dumps(entry['used_evidence'], ensure_ascii=False, default=str)}`")

    def run(self, context: StageContext) -> StageResult:
        state = context.state_manager.load_state() or {}
        diagnostics = state.get("diagnostics", {})
        pipeline = state.get("pipeline", {})
        reports = state.get("reports", {})
        knowledge = state.get("knowledge", {})
        runtime = state.get("test_runtime", {})
        compare_runtime = state.get("compare_runtime", {})
        final_decision = pipeline.get("final_decision") or ("review" if context.state.get("status") == "review" else "done")
        stage_fix_log = self._extract_stage_fix_log(state)

        stage_statuses = {
            stage: (diagnostics.get(stage) or {}).get("status", "pending")
            for stage in ("split", "translate", "pattern_guard", "format", "assemble", "api_validation", "trino_test", "compare", "report")
        }
        unresolved_issues = []
        for stage_name, block in diagnostics.items():
            if not isinstance(block, dict):
                continue
            stage_issues = block.get("errors") or block.get("issues") or []
            for issue in stage_issues:
                if not isinstance(issue, dict):
                    continue
                unresolved_issues.append({"stage": stage_name, **issue})
        report_json = {
            "query_name": context.query_name,
            "generated_at": datetime.utcnow().isoformat(),
            "final_status": final_decision,
            "stage_statuses": stage_statuses,
            "counters": {
                "diagnostic_stages": len(diagnostics),
                "unresolved_issues": len(unresolved_issues),
                "fix_attempts": len((knowledge or {}).get("fix_attempt_journal", []) or []),
                "runtime_executed_parts": len((runtime or {}).get("executed_parts", []) or []),
            },
            "unresolved_issues": unresolved_issues[:50],
            "artifact_paths": {key: value for key, value in reports.items() if value},
            "stage_fix_log": stage_fix_log,
            "root_cause_summary": {
                "runtime_last_error": runtime.get("last_error_text"),
                "compare_root_cause": compare_runtime.get("root_cause_summary"),
                "compare_diff_summary": compare_runtime.get("diff_summary"),
            },
        }
        md_lines = [
            f"# Analysis Report: {context.query_name}",
            "",
            f"- Final status: `{final_decision}`",
            f"- Generated at: `{report_json['generated_at']}`",
            "",
            "## Stage Statuses",
        ]
        for stage in ("split", "translate", "pattern_guard", "format", "assemble", "api_validation", "trino_test", "compare", "report"):
            md_lines.append(f"- `{stage}`: `{stage_statuses.get(stage, 'pending')}`")
        md_lines.extend(["", "## Manual Review Notes"])
        notes = diagnostics.get("review_notes") or []
        if notes:
            for note in notes:
                md_lines.append(f"- {note}")
        else:
            md_lines.append("- None")
        md_lines.extend(["", "## Artifacts"])
        for key, value in reports.items():
            if value:
                md_lines.append(f"- `{key}`: `{value}`")
        md_lines.extend(["", "## Runtime Summary"])
        compare_summary = runtime.get("reconciliation_summary") or {}
        if compare_summary:
            md_lines.append(f"- Reconciliation: `{json.dumps(compare_summary, ensure_ascii=False, default=str)}`")
        else:
            md_lines.append("- Reconciliation: not available")
        md_lines.extend(["", "## Compare Summary"])
        diff_summary = compare_runtime.get("diff_summary") or {}
        if diff_summary:
            md_lines.append(f"- Compare: `{json.dumps(diff_summary, ensure_ascii=False, default=str)[:2000]}`")
        else:
            md_lines.append("- Compare: not available")
        self._append_stage_fix_log_markdown(md_lines, stage_fix_log)

        reports_dir = context.state_manager.work_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        md_path = reports_dir / "analysis_report.md"
        json_path = reports_dir / "analysis_report.json"
        md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
        json_path.write_text(json.dumps(report_json, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        return StageResult(
            status=StageStatus.SUCCESS,
            blocking=False,
            artifacts={
                "analysis_report_md": str(md_path),
                "analysis_report_json": str(json_path),
            },
            details={"final_status": final_decision},
        )


def build_stage_registry() -> Dict[str, BaseStage]:
    return {
        "split": SplitStage(),
        "translate": TranslateStage(),
        "pattern_guard": PatternGuardStage(),
        "format": FormatStage(),
        "assemble": AssembleStage(),
        "api_validate": APIValidateStage(),
        "trino_test": TrinoTestStage(),
        "compare": CompareStage(),
        "report": ReportStage(),
    }


def build_pipeline_runner(config: Optional[PipelineConfig] = None) -> PipelineRunner:
    return PipelineRunner(build_stage_registry(), config=config)
