from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from config import settings
from core.api_validation_fixer import APIValidationFixer, FixStrategy
from core.llm_profiles import (
    get_openai_request_kwargs,
    resolve_profile,
    resolve_stage_profile,
)
from core.pattern_guard import PatternGuard, RawPatternRepairClient
from core.state_manager import StateManager
from core.translator import PartTranslator
from core.trino_tester import RawRepairLLMClient


def _state_manager(tmp_path, query_name: str = "demo") -> StateManager:
    state_manager = StateManager(query_name, base_path=tmp_path / "workflow" / "in_progress")
    state_manager.initialize()
    return state_manager


def test_resolve_profile_defaults_to_llm_model(monkeypatch):
    monkeypatch.setattr(settings, "llm_model", "openai/main-model")
    monkeypatch.setattr(settings, "llm_profile_fast_no_think_model", None)

    profile = resolve_profile("fast_no_think")

    assert profile.model == "openai/main-model"


def test_resolve_profile_repair_prefers_legacy_repair_model(monkeypatch):
    monkeypatch.setattr(settings, "llm_model", "openai/main-model")
    monkeypatch.setattr(settings, "llm_repair_model", "repair-model")
    monkeypatch.setattr(settings, "llm_profile_repair_think_model", None)

    profile = resolve_profile("repair_think")

    assert profile.model == "repair-model"


def test_resolve_stage_profile_uses_expected_defaults():
    assert resolve_stage_profile("translate") == "fast_no_think"
    assert resolve_stage_profile("pattern_guard") == "fast_no_think"
    assert resolve_stage_profile("api_validate") == "deep_think"
    assert resolve_stage_profile("trino_test") == "deep_think"
    assert resolve_stage_profile("compare") == "deep_think"
    assert resolve_stage_profile("repair") == "repair_think"


def test_get_openai_request_kwargs_parses_reasoning_and_extra_body(monkeypatch):
    monkeypatch.setattr(settings, "llm_profile_repair_think_reasoning_effort", "high")
    monkeypatch.setattr(settings, "llm_profile_repair_think_extra_body_json", '{"think": true}')

    kwargs = get_openai_request_kwargs("repair_think")

    assert kwargs["reasoning_effort"] == "high"
    assert kwargs["extra_body"] == {"think": True}


def test_invalid_extra_body_json_raises_clear_error(monkeypatch):
    monkeypatch.setattr(settings, "llm_profile_fast_no_think_extra_body_json", "{bad json")

    with pytest.raises(ValueError, match="Invalid EXTRA_BODY_JSON"):
        resolve_profile("fast_no_think")


def test_raw_repair_client_complete_uses_repair_profile_kwargs(monkeypatch):
    monkeypatch.setattr(settings, "llm_profile_repair_think_model", "openai/repair-model")
    monkeypatch.setattr(settings, "llm_profile_repair_think_reasoning_effort", "medium")
    monkeypatch.setattr(settings, "llm_profile_repair_think_extra_body_json", '{"think": true}')

    captured = {}

    class FakeCompletions:
        def create(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="patched sql"))]
            )

    client = RawRepairLLMClient.__new__(RawRepairLLMClient)
    client.client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))

    result = client.complete("fix it")

    assert result == "patched sql"
    assert captured["model"] == "repair-model"
    assert captured["reasoning_effort"] == "medium"
    assert captured["extra_body"] == {"think": True}


def test_raw_pattern_repair_client_complete_uses_repair_profile_kwargs(monkeypatch):
    monkeypatch.setattr(settings, "llm_profile_repair_think_model", "openai/repair-model")
    monkeypatch.setattr(settings, "llm_profile_repair_think_reasoning_effort", "medium")
    monkeypatch.setattr(settings, "llm_profile_repair_think_extra_body_json", '{"think": true}')

    captured = {}

    class FakeCompletions:
        def create(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="patched sql"))]
            )

    client = RawPatternRepairClient.__new__(RawPatternRepairClient)
    client.client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))

    result = client.complete("fix pattern")

    assert result == "patched sql"
    assert captured["model"] == "repair-model"
    assert captured["reasoning_effort"] == "medium"
    assert captured["extra_body"] == {"think": True}


def test_translator_uses_fast_no_think_profile(tmp_path, monkeypatch):
    state_manager = _state_manager(tmp_path, "translator_demo")
    state_manager.set_total_parts(1)
    state_manager.vertica_parts_path.mkdir(parents=True, exist_ok=True)
    (state_manager.vertica_parts_path / "translator_demo_part_0.sql").write_text("SELECT 1", encoding="utf-8")

    captured = {}

    class FakeModule:
        def __call__(self, vertica_sql, context_hint):
            return SimpleNamespace(trino_sql="SELECT 1\nFROM analytics_src.some_table")

    monkeypatch.setattr("core.translator.ensure_dspy_defaults", lambda: None)
    monkeypatch.setattr(
        "core.translator.get_dspy_lm",
        lambda profile_name, **kwargs: captured.setdefault("profile_name", profile_name) or object(),
    )
    translator = PartTranslator(compiled_module=FakeModule(), state_manager=state_manager)
    success, error = translator.translate_part(0)

    assert success is True
    assert error is None
    assert captured["profile_name"] == "fast_no_think"


def test_api_validation_fixer_uses_deep_think_profile(tmp_path, monkeypatch):
    state_manager = _state_manager(tmp_path, "api_demo")
    captured = {}

    class FakeModule:
        def __call__(self, vertica_sql, context_hint):
            return SimpleNamespace(trino_sql="SELECT fixed\n")

    monkeypatch.setattr("core.api_validation_fixer.ensure_dspy_defaults", lambda: None)
    monkeypatch.setattr(
        "core.api_validation_fixer.get_dspy_lm",
        lambda profile_name, **kwargs: captured.setdefault("profile_name", profile_name) or object(),
    )
    fixer = APIValidationFixer.__new__(APIValidationFixer)
    fixer.state_manager = state_manager
    fixer.query_name = state_manager.query_name
    fixer.compiled_module = FakeModule()
    monkeypatch.setattr(fixer, "_load_part_content", lambda part_num, is_vertica: "SELECT source")
    monkeypatch.setattr(fixer, "_get_error_details", lambda validator, error_type, target_part: {"title": "err"})
    monkeypatch.setattr(fixer, "_build_fix_context", lambda **kwargs: "ctx")
    saved = {}
    monkeypatch.setattr(fixer, "_save_fixed_version", lambda part_num, fixed_sql, error_type: saved.update({"sql": fixed_sql}))

    fixer._fix_part_with_llm(
        FixStrategy("header_param_fix", 0, [], False, "desc"),
        validator=object(),
    )

    assert saved["sql"] == "SELECT fixed\n"
    assert captured["profile_name"] == "deep_think"
