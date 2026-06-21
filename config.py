import os
from pathlib import Path
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    """
    Централизованная конфигурация окружения.
    """
    
    # LLM Configuration
    llm_base_url: str = "http://localhost:8000/v1"
    llm_model: str = "qwen3.5-122b"
    llm_repair_model: Optional[str] = None
    llm_api_key: str = "dummy"
    llm_timeout: int = 1800
    llm_max_tokens: int = 50000
    llm_max_retries: int = 3
    llm_profile_translate: str = "fast_no_think"
    llm_profile_pattern_guard: str = "fast_no_think"
    llm_profile_api_validate: str = "deep_think"
    llm_profile_trino_test: str = "deep_think"
    llm_profile_compare: str = "deep_think"
    llm_profile_repair: str = "repair_think"
    llm_profile_fast_no_think_model: Optional[str] = None
    llm_profile_deep_think_model: Optional[str] = None
    llm_profile_repair_think_model: Optional[str] = None
    llm_profile_fast_no_think_reasoning_effort: str = ""
    llm_profile_deep_think_reasoning_effort: str = ""
    llm_profile_repair_think_reasoning_effort: str = ""
    llm_profile_fast_no_think_extra_body_json: str = ""
    llm_profile_deep_think_extra_body_json: str = ""
    llm_profile_repair_think_extra_body_json: str = ""

    # SQL Formatter Configuration (sqlfluff)
    enable_sql_formatter: bool = True
    sql_formatter_dialect: str = "trino"  # "trino" или "presto"
    sql_formatter_rules: Optional[str] = None  # "L001,L002" или None для всех правил
    sql_formatter_exclude_rules: str = "L034"  # Исключить правила (L034 = forbid SELECT *)
    
    # DSPy MIPROv2 Configuration
    mipro_max_iterations: int = 40
    mipro_num_candidates: int = 13

    # API Validator Configuration
    enable_api_validation: bool = False  # Disabled by default for public/local runs.
    api_validator_url: str = "http://localhost:8080"
    api_validator_max_retries: int = 10
    api_validator_timeout: int = 60
    api_validator_poll_interval: int = 5  # секунды между polling'ом
    api_validation_blocking: bool = False

    # Trino Runtime Test Configuration
    trino_host: str = "localhost"
    trino_port: int = 443
    trino_user: Optional[str] = None
    trino_password: Optional[str] = None
    trino_catalog: str = "dwh"
    trino_schema: Optional[str] = None
    trino_ssl: bool = True
    enable_trino_test: bool = False
    trino_test_blocking: bool = True
    enable_compare: bool = False
    compare_blocking: bool = True
    trino_test_max_fix_iterations: int = 4
    trino_test_sample_limit: int = 1000
    trino_compare_full_limit: int = 100_000_000
    trino_test_schema: str = "sandbox"

    # Modular pipeline
    pipeline_version: str = "2"
    pipeline_stage_order: str = (
        "split,translate,pattern_guard,format,assemble,api_validate,trino_test,compare,report,finalize"
    )
    
    # Workflow Limits
    translation_retry_limit: int = 2
    pattern_fix_max_iter: int = 3
    
    # Paths
    workflow_base_path: Path = Path("./workflow")
    checkpoint_path: Path = Path("./checkpoint/compiled_module.json")
    private_golden_dataset_path: Optional[Path] = None
    
    # Logging
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    @property
    def in_queue_path(self) -> Path:
        return self.workflow_base_path / "in_queue"

    @property
    def in_progress_path(self) -> Path:
        return self.workflow_base_path / "in_progress"

    @property
    def done_path(self) -> Path:
        return self.workflow_base_path / "done"

    @property
    def review_path(self) -> Path:
        return self.workflow_base_path / "review"

    @property
    def golden_dataset_path(self) -> Path:
        if self.private_golden_dataset_path:
            return self.private_golden_dataset_path
        return Path("./golden_dataset")
    
    @property
    def header_rules_first_part(self) -> Path:
        """Правила для первой части (header/DDL)."""
        return Path("./golden_dataset") / "header_rule_first_part.MD"

    @property
    def header_rules_last_part(self) -> Path:
        """Правила для последней части (INSERT)."""
        return Path("./golden_dataset") / "header_rule_last_part.MD"

    def ensure_dirs(self):
        """Создает необходимые директории, если они не существуют."""
        dirs = [
            self.in_queue_path,
            self.in_progress_path,
            self.done_path / "vertica",
            self.done_path / "trino",
            self.review_path / "vertica",
            self.review_path / "trino",
            self.checkpoint_path.parent,
            self.golden_dataset_path
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

    @property
    def enabled_stages(self) -> list[str]:
        """Возвращает упорядоченный список активных stage ids."""
        stages = [item.strip() for item in self.pipeline_stage_order.split(",") if item.strip()]
        enabled: list[str] = []
        for stage in stages:
            if stage == "api_validate" and not self.enable_api_validation:
                continue
            if stage == "trino_test" and not self.enable_trino_test:
                continue
            if stage == "compare" and (not self.enable_compare or not self.enable_trino_test):
                continue
            enabled.append(stage)
        return enabled


@lru_cache()
def get_settings() -> Settings:
    """
    Singleton-фабрика для получения настроек.
    """
    return Settings()


# ВАЖНО: Эта переменная должна быть на уровне модуля, чтобы импорт работал
settings = get_settings()
