"""
StateManager - Атомарное управление состоянием миграции.
Обеспечивает идемпотентность и восстановление после сбоев через metadata.json.
"""

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from config import settings


class StateManager:
    """
    Менеджер состояния для одного SQL-файла миграции.
    Все операции атомарны (write to temp -> fsync -> rename).
    """
    
    # Схема обязательных полей metadata.json
    REQUIRED_FIELDS = ["status", "current_part", "total_parts", "parts_map", "created_at"]
    
    # Допустимые статусы файла
    VALID_STATUSES = [
        "initialized", "splitting", "translating", "validating",
        "assembling", "completed", "review"
    ]
    
    # Допустимые статусы части
    VALID_PART_STATUSES = [
        "pending", "split", "translated", "pattern_fixed", 
        "validated", "pattern_error", "completed"
    ]
    
    # Маппинг статусов части в булевы флаги
    STATUS_FLAGS_MAP = {
        "translated": {"translated": True},
        "pattern_fixed": {"translated": True, "pattern_fixed": True},
        "validated": {"translated": True, "validated": True},
        "pattern_error": {"translated": True, "pattern_error": True},
        "completed": {"translated": True, "validated": True, "completed": True},
    }
    STATUS_BOOLEAN_FIELDS = ["translated", "pattern_fixed", "validated", "pattern_error", "completed"]
    DIAGNOSTIC_STAGES = [
        "split",
        "translate",
        "pattern_guard",
        "format",
        "formatter",
        "assemble",
        "api_validation",
        "trino_test",
        "compare",
        "report",
    ]
    DIAGNOSTIC_STAGE_ALIASES = {
        "formatter": "format",
        "api_validate": "api_validation",
    }
    RETRY_STAGE_TO_DIAGNOSTICS = {
        "split": ["split", "translate", "pattern_guard", "format", "assemble", "api_validation", "trino_test", "compare", "report"],
        "translate": ["translate", "pattern_guard", "format", "assemble", "api_validation", "trino_test", "compare", "report"],
        "pattern_guard": ["pattern_guard", "format", "assemble", "api_validation", "trino_test", "compare", "report"],
        "format": ["format", "assemble", "api_validation", "trino_test", "compare", "report"],
        "assemble": ["assemble", "api_validation", "trino_test", "compare", "report"],
        "api_validate": ["api_validation", "trino_test", "compare", "report"],
        "trino_test": ["trino_test", "compare", "report"],
        "compare": ["compare", "report"],
        "report": ["report"],
    }
    
    def __init__(self, query_name: str, base_path: Optional[Path] = None):
        """
        Инициализация путей к metadata.json и директориям частей.
        
        Args:
            query_name: Имя файла без расширения (например, "query_001")
            base_path: Базовый путь для workflow (по умолчанию из config)
        """
        self.query_name = query_name
        self.base_path = base_path or settings.in_progress_path
        self.work_dir = self.base_path / query_name
        self.metadata_path = self.work_dir / "metadata.json"
        
        # Пути к поддиректориям
        self.vertica_parts_path = self.work_dir / "vertica_parts"
        self.trino_parts_path = self.work_dir / "trino_parts"
        self.validations_path = self.work_dir / "validations"
        self.logs_path = self.work_dir / "logs"
        
    def _ensure_work_dir(self) -> None:
        """Создает рабочую директорию и все поддиректории."""
        dirs = [
            self.work_dir,
            self.vertica_parts_path,
            self.trino_parts_path,
            self.validations_path,
            self.logs_path
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)
    
    def _atomic_write(self, path: Path, data: dict) -> None:
        """
        Атомарная запись JSON файла (write to temp -> fsync -> rename).
        
        Args:
            path: Путь к файлу
            data: Данные для записи
        """
        # Создаем временный файл в той же директории
        fd, temp_path = tempfile.mkstemp(
            suffix=".tmp", 
            dir=path.parent,
            prefix=".metadata_"
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            
            # Атомарное переименование
            os.replace(temp_path, path)
        except Exception:
            # Очистка временного файла при ошибке
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
    
    def initialize(self) -> dict:
        """
        Создает начальную структуру metadata.json со статусом "initialized".
        
        Returns:
            dict: Начальное состояние
        """
        self._ensure_work_dir()
        
        initial_state = {
            "query_name": self.query_name,
            "status": "initialized",
            "current_part": 0,
            "total_parts": 0,
            "parts_map": {},
            "pipeline": self._default_pipeline_state(),
            "parts": self._default_parts_state(),
            "knowledge": self._default_knowledge_state(),
            "test_runtime": self._default_test_runtime_state(),
            "compare_runtime": self._default_compare_runtime_state(),
            "reports": self._default_reports_state(),
            "diagnostics": self._default_diagnostics(),
            "created_at": datetime.utcnow().isoformat(),
            "last_modified": datetime.utcnow().isoformat(),
            "error_msg": None,
            "final_status": None
        }
        
        self._atomic_write(self.metadata_path, initial_state)
        return initial_state

    def _default_diagnostics(self) -> dict:
        """Возвращает стандартную структуру diagnostics."""
        diagnostics = {
            stage: {
                "status": "pending",
                "errors": [],
                "updated_at": None,
                "details": {},
            }
            for stage in self.DIAGNOSTIC_STAGES
        }
        diagnostics["formatter"] = diagnostics["format"]
        diagnostics["review_notes"] = []
        return diagnostics

    def _default_pipeline_state(self) -> dict:
        return {
            "pipeline_version": getattr(settings, "pipeline_version", "2"),
            "enabled_stages": getattr(settings, "enabled_stages", []),
            "current_stage": None,
            "final_decision": None,
            "stage_order": getattr(settings, "enabled_stages", []),
            "last_stage_result": None,
        }

    def _default_parts_state(self) -> dict:
        return {
            "dependencies": {},
            "latest_versions": {},
            "write_targets": {},
            "created_tables": {},
            "intent_memory": {},
        }

    def _default_knowledge_state(self) -> dict:
        return {
            "pattern_guard_history": {},
            "api_validation_history": {},
            "runtime_test_history": [],
            "llm_fix_history": [],
            "migration_knowledge_history": [],
            "fix_attempt_journal": [],
        }

    def _default_test_runtime_state(self) -> dict:
        return {
            "test_schema": getattr(settings, "trino_test_schema", "sandbox"),
            "declared_variables": {},
            "executed_parts": [],
            "created_runtime_tables": [],
            "current_part": None,
            "current_fix_attempt": None,
            "current_fix_target_part": None,
            "current_repair_session_id": None,
            "current_repair_phase": None,
            "current_repair_target_part": None,
            "current_repair_plan": None,
            "last_failed_part": None,
            "last_error_text": None,
            "root_failed_part": None,
            "rebuild_start_part": None,
            "reconciliation_summary": {},
            "last_event": None,
            "last_repair_summary": None,
        }

    def _default_compare_runtime_state(self) -> dict:
        return {
            "current_phase": None,
            "current_compare_session_id": None,
            "current_repair_target_part": None,
            "diff_summary": {},
            "root_cause_summary": None,
            "last_event": None,
        }

    def _default_reports_state(self) -> dict:
        return {
            "api_validation_report": None,
            "trino_test_report": None,
            "compare_report": None,
            "fix_attempt_journal": None,
            "analysis_report_md": None,
            "analysis_report_json": None,
        }

    def _sync_diagnostics_aliases(self, diagnostics: dict) -> dict:
        diagnostics["formatter"] = diagnostics["format"]
        return diagnostics

    def _empty_diagnostics_entry(self) -> dict:
        return {
            "status": "pending",
            "errors": [],
            "updated_at": None,
            "details": {},
        }

    def _normalize_state_sections(self, state: dict) -> dict:
        state["diagnostics"] = self._normalize_diagnostics(state.get("diagnostics"))
        state["pipeline"] = self._normalize_pipeline(state.get("pipeline"))
        state["parts"] = self._normalize_parts_section(state.get("parts"))
        state["knowledge"] = self._normalize_knowledge(state.get("knowledge"))
        state["test_runtime"] = self._normalize_test_runtime(state.get("test_runtime"))
        state["compare_runtime"] = self._normalize_compare_runtime(state.get("compare_runtime"))
        state["reports"] = self._normalize_reports(state.get("reports"))
        return state

    def _ensure_state(self) -> dict:
        state = self.load_state()
        if state is None:
            state = self.initialize()
        return self._normalize_state_sections(state)

    def _write_state(self, state: dict) -> dict:
        state["last_modified"] = datetime.utcnow().isoformat()
        self._atomic_write(self.metadata_path, state)
        return state

    def _part_status_defaults(self) -> dict:
        return {
            "translated": False,
            "validated": False,
            "fix_version": 0,
            "error": None,
            "status": "pending",
        }

    def _get_diagnostics_state(self) -> tuple[dict, dict]:
        state = self._ensure_state()
        diagnostics = self._normalize_diagnostics(state.get("diagnostics"))
        return state, diagnostics

    def _normalize_diagnostics(self, diagnostics: Optional[dict]) -> dict:
        """Нормализует diagnostics к актуальной схеме."""
        normalized = self._default_diagnostics()
        if not isinstance(diagnostics, dict):
            return normalized

        review_notes = diagnostics.get("review_notes")
        if isinstance(review_notes, list):
            normalized["review_notes"] = review_notes

        legacy_issues = diagnostics.get("issues")
        if isinstance(legacy_issues, list):
            for issue in legacy_issues:
                if not isinstance(issue, dict):
                    continue
                stage = self._normalize_stage_name(issue.get("stage"))
                if stage in self.DIAGNOSTIC_STAGES:
                    normalized[stage]["errors"].append(issue)
                    normalized[stage]["updated_at"] = issue.get("created_at")

        for stage in self.DIAGNOSTIC_STAGES:
            source_stage = self._normalize_stage_name(stage)
            stage_block = diagnostics.get(source_stage) or diagnostics.get(stage)
            if not isinstance(stage_block, dict):
                continue
            if stage_block.get("status"):
                normalized[stage]["status"] = stage_block["status"]
            errors = stage_block.get("errors")
            if isinstance(errors, list):
                normalized[stage]["errors"] = errors
            if stage_block.get("updated_at"):
                normalized[stage]["updated_at"] = stage_block["updated_at"]
            details = stage_block.get("details")
            if isinstance(details, dict):
                normalized[stage]["details"] = details

        return self._sync_diagnostics_aliases(normalized)

    def _normalize_stage_name(self, stage: Optional[str]) -> str:
        if not stage:
            return ""
        return self.DIAGNOSTIC_STAGE_ALIASES.get(stage, stage)

    def _normalize_pipeline(self, pipeline: Optional[dict]) -> dict:
        normalized = self._default_pipeline_state()
        if isinstance(pipeline, dict):
            normalized.update({key: value for key, value in pipeline.items() if value is not None})
        return normalized

    def _normalize_parts_section(self, parts: Optional[dict]) -> dict:
        normalized = self._default_parts_state()
        if isinstance(parts, dict):
            for key in normalized:
                value = parts.get(key)
                if isinstance(value, dict):
                    normalized[key] = value
        return normalized

    def _normalize_knowledge(self, knowledge: Optional[dict]) -> dict:
        normalized = self._default_knowledge_state()
        if isinstance(knowledge, dict):
            for key, value in knowledge.items():
                if key in {"pattern_guard_history", "api_validation_history"} and isinstance(value, dict):
                    normalized[key] = value
                elif key in {"runtime_test_history", "llm_fix_history", "migration_knowledge_history", "fix_attempt_journal"} and isinstance(value, list):
                    normalized[key] = value
        return normalized

    def _normalize_test_runtime(self, runtime: Optional[dict]) -> dict:
        normalized = self._default_test_runtime_state()
        if isinstance(runtime, dict):
            normalized.update(runtime)
        return normalized

    def _normalize_compare_runtime(self, runtime: Optional[dict]) -> dict:
        normalized = self._default_compare_runtime_state()
        if isinstance(runtime, dict):
            normalized.update(runtime)
        return normalized

    def _normalize_reports(self, reports: Optional[dict]) -> dict:
        normalized = self._default_reports_state()
        if isinstance(reports, dict):
            normalized.update(reports)
        return normalized
    
    def load_state(self) -> Optional[dict]:
        """
        Читает metadata.json, если существует.
        
        Returns:
            dict: Состояние или None если файл не существует
        """
        if not self.metadata_path.exists():
            return None
        
        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            # Валидация схемы
            for field in self.REQUIRED_FIELDS:
                if field not in state:
                    raise ValueError(f"Missing required field: {field}")

            return self._normalize_state_sections(state)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"[StateManager] Error loading state for {self.query_name}: {e}")
            return None
    
    def update_state(self, updates: dict, sync: bool = True) -> dict:
        """
        Атомарное обновление состояния (мердж с текущим).
        
        Args:
            updates: Dict с обновлениями
            sync: Если True - форсированный сброс на диск
            
        Returns:
            dict: Обновленное состояние
        """
        current_state = self._ensure_state()
        current_state.update(updates)
        
        if sync:
            self._write_state(current_state)
        
        return current_state
    
    def get_part_status(self, part_num: int) -> dict:
        """
        Возвращает статус конкретной части.
        
        Args:
            part_num: Номер части (0-based)
            
        Returns:
            dict: {"translated": bool, "validated": bool, "fix_version": int, "error": str}
        """
        state = self.load_state()
        if state is None:
            return self._part_status_defaults()
        
        parts_map = state.get("parts_map", {})
        part_key = f"part_{part_num}"
        
        if part_key not in parts_map:
            return self._part_status_defaults()
        
        part_info = parts_map[part_key]
        return {
            "translated": part_info.get("translated", False),
            "validated": part_info.get("validated", False),
            "fix_version": part_info.get("fix_version", 0),
            "error": part_info.get("error", None),
            "status": part_info.get("status", "pending"),
            "completed": part_info.get("completed", False),
            "pattern_error": part_info.get("pattern_error", False),
        }
    
    def set_part_status(
        self, 
        part_num: int, 
        status: str, 
        metadata: Optional[dict] = None
    ) -> dict:
        """
        Устанавливает статус части.
        
        Args:
            part_num: Номер части
            status: Один из VALID_PART_STATUSES
            metadata: Дополнительные метаданные (опционально)
            
        Returns:
            dict: Обновленное состояние части
        """
        if status not in self.VALID_PART_STATUSES:
            raise ValueError(f"Invalid part status: {status}. Must be one of {self.VALID_PART_STATUSES}")
        
        state = self._ensure_state()
        parts_map = state.get("parts_map", {})
        part_key = f"part_{part_num}"
        
        part_info = parts_map.get(part_key, {})
        part_info["status"] = status
        part_info["last_modified"] = datetime.utcnow().isoformat()

        # Сбрасываем вычисляемые булевы флаги, чтобы не оставлять stale-состояние
        for field in self.STATUS_BOOLEAN_FIELDS:
            part_info[field] = False

        # Автоматически устанавливаем булевы флаги на основе статуса
        if status in self.STATUS_FLAGS_MAP:
            part_info.update(self.STATUS_FLAGS_MAP[status])
        
        # Добавляем дополнительные метаданные
        if metadata:
            part_info.update(metadata)
        
        parts_map[part_key] = part_info
        state["parts_map"] = parts_map
        self._write_state(state)
        return part_info

    def mark_final_status(self, status: str, error_msg: Optional[str] = None) -> dict:
        """
        Устанавливает финальный статус всего файла.
        
        Args:
            status: "completed" или "review"
            error_msg: Описание причины (если review)
            
        Returns:
            dict: Обновленное состояние
        """
        if status not in ["completed", "review"]:
            raise ValueError(f"Invalid final status: {status}")
        
        updates = {
            "status": status,
            "final_status": status,
            "completed_at": datetime.utcnow().isoformat()
        }
        
        if error_msg:
            updates["error_msg"] = error_msg
        
        return self.update_state(updates)

    def clear_stage_issues(self, stage: str) -> dict:
        """Удаляет ранее сохраненные issues для конкретного этапа."""
        _, diagnostics = self._get_diagnostics_state()
        stage = self._normalize_stage_name(stage)
        if stage not in self.DIAGNOSTIC_STAGES:
            return self.update_state({"diagnostics": diagnostics})
        diagnostics[stage]["errors"] = []
        diagnostics[stage]["updated_at"] = datetime.utcnow().isoformat()
        return self.update_state({"diagnostics": self._sync_diagnostics_aliases(diagnostics)})

    def set_stage_status(
        self,
        stage: str,
        status: str,
        *,
        details: Optional[dict] = None,
    ) -> dict:
        """Сохраняет явный статус этапа в diagnostics."""
        _, diagnostics = self._get_diagnostics_state()
        stage = self._normalize_stage_name(stage)
        if stage not in self.DIAGNOSTIC_STAGES:
            raise ValueError(f"Unsupported diagnostics stage: {stage}")

        diagnostics[stage]["status"] = status
        diagnostics[stage]["updated_at"] = datetime.utcnow().isoformat()
        if details is not None:
            diagnostics[stage]["details"] = details
        return self.update_state({"diagnostics": self._sync_diagnostics_aliases(diagnostics)})

    def append_issue(
        self,
        stage: str,
        message: str,
        *,
        severity: str = "error",
        details: Optional[dict] = None,
    ) -> dict:
        """Добавляет структурированную запись о проблеме в metadata."""
        _, diagnostics = self._get_diagnostics_state()
        original_stage = stage
        stage = self._normalize_stage_name(stage)
        if stage not in self.DIAGNOSTIC_STAGES:
            raise ValueError(f"Unsupported diagnostics stage: {stage}")

        issue = {
            "stage": original_stage,
            "severity": severity,
            "message": message,
            "details": details or {},
            "created_at": datetime.utcnow().isoformat(),
        }
        diagnostics[stage]["errors"].append(issue)
        diagnostics[stage]["updated_at"] = issue["created_at"]
        return self.update_state({"diagnostics": self._sync_diagnostics_aliases(diagnostics)})

    def set_review_notes(self, notes: List[str]) -> dict:
        """Сохраняет краткий список того, что нужно проверить вручную."""
        _, diagnostics = self._get_diagnostics_state()
        diagnostics["review_notes"] = notes
        return self.update_state({"diagnostics": diagnostics})

    def update_pipeline(self, **updates: Any) -> dict:
        """Обновляет секцию pipeline в metadata."""
        state = self._ensure_state()
        pipeline = self._normalize_pipeline(state.get("pipeline"))
        pipeline.update(updates)
        return self.update_state({"pipeline": pipeline})

    def update_section(self, section: str, updates: Dict[str, Any]) -> dict:
        """Обновляет произвольную dict-секцию metadata верхнего уровня."""
        state = self._ensure_state()
        current = state.get(section)
        if not isinstance(current, dict):
            current = {}
        current.update(updates)
        return self.update_state({section: current})

    def append_knowledge(self, bucket: str, item: Any, *, key: Optional[str] = None) -> dict:
        """Добавляет запись в секцию knowledge."""
        state = self._ensure_state()
        knowledge = self._normalize_knowledge(state.get("knowledge"))
        if bucket in {"pattern_guard_history", "api_validation_history"}:
            if key is None:
                raise ValueError(f"key is required for knowledge bucket {bucket}")
            history = knowledge[bucket].get(key, [])
            if not isinstance(history, list):
                history = []
            history.append(item)
            knowledge[bucket][key] = history
        elif bucket in {"runtime_test_history", "llm_fix_history", "migration_knowledge_history", "fix_attempt_journal"}:
            history = knowledge.get(bucket, [])
            if not isinstance(history, list):
                history = []
            history.append(item)
            knowledge[bucket] = history
        else:
            raise ValueError(f"Unsupported knowledge bucket: {bucket}")

        return self.update_state({"knowledge": knowledge})

    def register_report(self, report_key: str, report_path: Optional[str]) -> dict:
        """Регистрирует путь к generated report artifact."""
        state = self._ensure_state()
        reports = self._normalize_reports(state.get("reports"))
        reports[report_key] = report_path
        return self.update_state({"reports": reports})

    def get_part_metadata(self, part_num: int) -> Dict[str, Any]:
        """Возвращает raw metadata части из parts_map."""
        state = self.load_state()
        if state is None:
            return {}
        return state.get("parts_map", {}).get(f"part_{part_num}", {})

    def update_part_metadata(self, part_num: int, metadata: dict) -> dict:
        """Обновляет metadata конкретной части без смены её статуса."""
        state = self._ensure_state()
        parts_map = state.get("parts_map", {})
        part_key = f"part_{part_num}"
        part_info = parts_map.get(part_key, {})
        part_info.update(metadata)
        part_info["last_modified"] = datetime.utcnow().isoformat()
        parts_map[part_key] = part_info

        state["parts_map"] = parts_map
        self._write_state(state)
        return part_info

    def prepare_for_retry(self, start_stage: str) -> dict:
        """
        Подготавливает metadata.json к повторному запуску с выбранного этапа.
        Сбрасывает диагностические блоки для этапа и всех последующих.
        """
        _, diagnostics = self._get_diagnostics_state()
        for stage in self.RETRY_STAGE_TO_DIAGNOSTICS.get(start_stage, []):
            normalized_stage = self._normalize_stage_name(stage)
            diagnostics[normalized_stage] = self._empty_diagnostics_entry()

        updates = {
            "status": "initialized",
            "final_status": None,
            "completed_at": None,
            "error_msg": None,
            "diagnostics": diagnostics,
            "current_operation": f"🔁 Retry scheduled from {start_stage}",
            "operation_details": {"retry_from": start_stage},
        }

        if start_stage == "translate":
            updates["current_part"] = 0
        elif start_stage == "split":
            updates["current_part"] = 0

        diagnostics["review_notes"] = []
        updates["diagnostics"] = self._sync_diagnostics_aliases(diagnostics)
        return self.update_state(updates)
    
    def set_total_parts(self, total: int) -> dict:
        """
        Устанавливает общее количество частей после разбиения.
        Не меняет workflow-статус: переходами управляет оркестратор.
        
        Args:
            total: Количество частей
            
        Returns:
            dict: Обновленное состояние
        """
        return self.update_state({"total_parts": total})
    
    def is_all_parts_completed(self) -> bool:
        """
        Проверяет, все ли части имеют статус "completed".
        
        Returns:
            bool: True если все части завершены
        """
        return self.get_next_pending_part() is None
    
    def get_translation_versions(self, part_num: int) -> List[str]:
        """
        Возвращает список версий перевода для части.
        
        Args:
            part_num: Номер части
            
        Returns:
            List[str]: Список путей к файлам версий
        """
        return [str(path) for path in self.get_translation_version_paths(part_num)]

    def get_translation_version_paths(self, part_num: int) -> List[Path]:
        """
        Возвращает пути ко всем найденным версиям перевода части, отсортированные по версии.
        """
        pattern = re.compile(
            rf"^{re.escape(self.query_name)}_part_{part_num}_trino(?:_v(?P<version>\d+))?\.sql$"
        )
        versioned_paths = []

        for path in self.trino_parts_path.glob(f"{self.query_name}_part_{part_num}_trino*.sql"):
            match = pattern.match(path.name)
            if not match:
                continue

            version = int(match.group("version") or 0)
            versioned_paths.append((version, path))

        versioned_paths.sort(key=lambda item: item[0])
        return [path for _, path in versioned_paths]

    def get_latest_version_number(self, part_num: int) -> int:
        """Возвращает номер последней найденной версии части."""
        pattern = re.compile(
            rf"^{re.escape(self.query_name)}_part_{part_num}_trino(?:_v(?P<version>\d+))?\.sql$"
        )
        latest_version = 0

        for path in self.trino_parts_path.glob(f"{self.query_name}_part_{part_num}_trino*.sql"):
            match = pattern.match(path.name)
            if not match:
                continue
            latest_version = max(latest_version, int(match.group("version") or 0))

        return latest_version

    def get_latest_version_path(self, part_num: int) -> Optional[Path]:
        """
        Находит последнюю доступную версию файла части.
        
        Args:
            part_num: Номер части
            
        Returns:
            Path: Путь к последней версии или None
        """
        versions = self.get_translation_version_paths(part_num)
        if not versions:
            return None
        return versions[-1]

    def set_current_operation(self, operation: str, details: Optional[dict] = None) -> dict:
        """
        Устанавливает текущую выполняемую операцию для отображения в дашборде.

        Args:
            operation: Описание операции (например, "Translating part 3/5")
            details: Дополнительные детали (part_num, iteration и т.д.)
        """
        updates = {
            "current_operation": operation,
            "operation_updated_at": datetime.utcnow().isoformat()
        }
        if details:
            updates["operation_details"] = details
        return self.update_state(updates)
