"""
Modular pipeline primitives for the PipelineRunner-only orchestration.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Protocol

from config import settings
from core.state_manager import StateManager


class StageStatus(str, Enum):
    SUCCESS = "success"
    WARNING = "warning"
    FAILED = "failed"
    SKIPPED = "skipped"
    RUNNING = "running"


class PipelineDecision(str, Enum):
    DONE = "done"
    DONE_WITH_WARNINGS = "done_with_warnings"
    REVIEW = "review"


@dataclass
class PipelineConfig:
    enabled_stages: List[str]
    stage_order: List[str]
    enable_api_validation: bool
    enable_trino_test: bool
    api_validation_blocking: bool = False
    trino_test_blocking: bool = True
    enable_compare: bool = True
    compare_blocking: bool = True

    @classmethod
    def from_settings(cls) -> "PipelineConfig":
        stage_order = [stage for stage in settings.enabled_stages if stage != "finalize"]
        return cls(
            enabled_stages=stage_order,
            stage_order=stage_order,
            enable_api_validation=settings.enable_api_validation,
            enable_trino_test=settings.enable_trino_test,
            api_validation_blocking=settings.api_validation_blocking,
            trino_test_blocking=settings.trino_test_blocking,
            enable_compare=settings.enable_compare,
            compare_blocking=settings.compare_blocking,
        )

    def is_enabled(self, stage_id: str) -> bool:
        return stage_id in self.enabled_stages


@dataclass
class StageContext:
    query_name: str
    state_manager: StateManager
    state: Dict[str, Any]
    config: PipelineConfig
    artifacts: Dict[str, Any] = field(default_factory=dict)
    knowledge: Dict[str, Any] = field(default_factory=dict)
    runtime: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StageResult:
    status: StageStatus
    blocking: bool
    issues: List[Dict[str, Any]] = field(default_factory=list)
    artifacts: Dict[str, Any] = field(default_factory=dict)
    updates: Dict[str, Any] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)
    decision: Optional[PipelineDecision] = None
    review_notes: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if isinstance(self.status, str):
            self.status = StageStatus(self.status)
        if isinstance(self.decision, str):
            self.decision = PipelineDecision(self.decision)


class BaseStage(Protocol):
    stage_id: str
    enabled_by_default: bool
    blocking: bool
    produced_artifacts: List[str]
    consumed_context_keys: List[str]
    can_retry_from_here: bool

    def run(self, context: StageContext) -> StageResult:
        ...


class PipelineRunner:
    """
    Executes reusable stage modules and synchronizes metadata/status with StateManager.
    """

    def __init__(self, stage_registry: Dict[str, BaseStage], config: Optional[PipelineConfig] = None):
        self.stage_registry = stage_registry
        self.config = config or PipelineConfig.from_settings()

    def get_stage(self, stage_id: str) -> BaseStage:
        if stage_id not in self.stage_registry:
            raise KeyError(f"Unknown stage: {stage_id}")
        return self.stage_registry[stage_id]

    def build_context(self, query_name: str, state: Dict[str, Any]) -> StageContext:
        state_manager = StateManager(query_name)
        saved_state = state_manager.load_state() or {}
        return StageContext(
            query_name=query_name,
            state_manager=state_manager,
            state=state,
            config=self.config,
            artifacts=saved_state.get("reports") or {},
            knowledge=saved_state.get("knowledge") or {},
            runtime=saved_state.get("test_runtime") or {},
        )

    def run_stage(self, stage_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
        stage = self.get_stage(stage_id)
        context = self.build_context(state["query_name"], state)
        context.state_manager.update_pipeline(
            enabled_stages=self.config.enabled_stages,
            stage_order=self.config.stage_order,
            current_stage=stage_id,
        )

        if not self.config.is_enabled(stage_id):
            result = StageResult(
                status=StageStatus.SKIPPED,
                blocking=False,
                details={"reason": "disabled_by_config"},
                decision=PipelineDecision.DONE_WITH_WARNINGS if stage_id == "api_validate" else None,
            )
        else:
            result = stage.run(context)

        return self._apply_stage_result(stage_id, context, result)

    def _apply_stage_result(self, stage_id: str, context: StageContext, result: StageResult) -> Dict[str, Any]:
        state_manager = context.state_manager
        if result.artifacts:
            state_manager.update_section("reports", result.artifacts)
        if result.review_notes:
            state_manager.set_review_notes(result.review_notes)

        for issue in result.issues:
            state_manager.append_issue(
                stage_id,
                issue.get("message", "stage issue"),
                severity=issue.get("severity", "error"),
                details=issue.get("details"),
            )

        status_value = result.status.value
        final_decision = self._resolve_final_decision(result)

        pipeline_updates: Dict[str, Any] = {
            "current_stage": stage_id,
            "last_stage_result": {
                "status": status_value,
                "blocking": result.blocking,
                "details": result.details,
            },
        }
        if final_decision is not None:
            pipeline_updates["final_decision"] = final_decision.value
        state_manager.update_pipeline(**pipeline_updates)
        state_manager.set_stage_status(stage_id, status_value, details=result.details)
        if result.updates:
            state_manager.update_state(result.updates)

        next_state = dict(context.state)
        next_state.update(result.updates)

        if result.status == StageStatus.FAILED and result.blocking:
            next_state["status"] = "review"
            if result.issues:
                next_state["error_msg"] = result.issues[-1].get("message")
        elif result.status in {StageStatus.WARNING, StageStatus.SUCCESS, StageStatus.SKIPPED}:
            if next_state.get("status") == "review" and not result.blocking:
                pass

        return next_state

    @staticmethod
    def _resolve_final_decision(result: StageResult) -> Optional[PipelineDecision]:
        if result.decision is not None:
            return result.decision
        if result.status == StageStatus.FAILED and result.blocking:
            return PipelineDecision.REVIEW
        if result.status in {StageStatus.WARNING, StageStatus.SKIPPED}:
            return PipelineDecision.DONE_WITH_WARNINGS
        return None

    def run_until(self, start_stage: str, state: Dict[str, Any], stop_before: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        stop_set = set(stop_before or [])
        started = False
        current_state = dict(state)
        for stage_id in self.config.stage_order:
            if not started:
                started = stage_id == start_stage
                if not started:
                    continue
            if stage_id in stop_set:
                break
            current_state = self.run_stage(stage_id, current_state)
            if current_state.get("status") == "review":
                break
        return current_state

    def run_pipeline(self, query_name: str, initial_state: Dict[str, Any], start_stage: str = "split") -> Dict[str, Any]:
        """
        Execute the configured stage pipeline from a concrete entry point.

        The runner is the single source of orchestration truth: stage order comes
        from PipelineConfig, translate loops until all parts are processed, and
        blocking failures stop the flow through the structured StageResult
        contract.
        """
        if start_stage not in self.config.stage_order:
            raise ValueError(f"Unsupported pipeline start stage: {start_stage}")

        current_state = dict(initial_state)
        current_state["query_name"] = query_name
        started = False

        for stage_id in self.config.stage_order:
            if not started:
                started = stage_id == start_stage
                if not started:
                    continue

            if stage_id == "translate":
                current_state = self._run_translate_loop(current_state)
            else:
                current_state = self._run_stage_with_log(stage_id, current_state)

            if current_state.get("status") == "review":
                if self._should_run_report_after_review(stage_id):
                    current_state = self._run_stage_with_log("report", current_state)
                break

        return current_state

    def _run_translate_loop(self, state: Dict[str, Any]) -> Dict[str, Any]:
        current_state = dict(state)
        while True:
            current_state = self._run_stage_with_log("translate", current_state)
            if current_state.get("status") == "review":
                return current_state
            if current_state.get("current_part", 0) >= current_state.get("total_parts", 0):
                return current_state

    def _run_stage_with_log(self, stage_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
        stage_number = self.config.stage_order.index(stage_id) + 1
        total_stages = len(self.config.stage_order)
        print(f"\n[Stage {stage_number}/{total_stages}] {stage_id}...")
        return self.run_stage(stage_id, state)

    def _should_run_report_after_review(self, failed_stage_id: str) -> bool:
        if "report" not in self.config.stage_order or failed_stage_id == "report":
            return False
        report_index = self.config.stage_order.index("report")
        failed_index = self.config.stage_order.index(failed_stage_id)
        runtime_stage_ids = {"trino_test", "compare"}
        return failed_stage_id in runtime_stage_ids and failed_index < report_index
