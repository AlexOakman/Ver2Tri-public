import ipaddress
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Literal, Tuple
from urllib.parse import urlparse

import dspy

from config import settings


LLMProfileName = Literal["fast_no_think", "deep_think", "repair_think"]
LLMStageName = Literal["translate", "pattern_guard", "api_validate", "trino_test", "compare", "repair"]
_VALID_PROFILE_NAMES = {"fast_no_think", "deep_think", "repair_think"}
_DSPY_LM_CACHE: Dict[Tuple[str, str, str, str, int, float, bool], dspy.LM] = {}


@dataclass(frozen=True)
class LLMProfileConfig:
    profile_name: LLMProfileName
    model: str
    reasoning_effort: str
    extra_body: Dict[str, Any]
    base_url: str
    api_key: str
    timeout: int


def resolve_stage_profile(stage_name: LLMStageName) -> LLMProfileName:
    profile_name = getattr(settings, f"llm_profile_{stage_name}")
    return _normalize_profile_name(profile_name)


def resolve_profile(profile_name: str) -> LLMProfileConfig:
    normalized = _normalize_profile_name(profile_name)
    suffix = normalized
    model = _resolve_profile_model(normalized, suffix)
    reasoning_effort = getattr(settings, f"llm_profile_{suffix}_reasoning_effort").strip()
    extra_body_raw = getattr(settings, f"llm_profile_{suffix}_extra_body_json").strip()
    extra_body = _parse_extra_body_json(normalized, extra_body_raw)
    return LLMProfileConfig(
        profile_name=normalized,
        model=model,
        reasoning_effort=reasoning_effort,
        extra_body=extra_body,
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key,
        timeout=settings.llm_timeout,
    )


def get_dspy_lm(
    profile_name: str,
    *,
    max_tokens: int,
    temperature: float,
    cache: bool,
    ensure_no_proxy: bool,
) -> dspy.LM:
    profile = resolve_profile(profile_name)
    if ensure_no_proxy:
        ensure_no_proxy_for_llm(profile.base_url)
    cache_key = (
        profile.profile_name,
        profile.model,
        profile.base_url,
        profile.api_key,
        max_tokens,
        temperature,
        cache,
    )
    lm = _DSPY_LM_CACHE.get(cache_key)
    if lm is None:
        lm = dspy.LM(
            model=profile.model,
            api_base=profile.base_url,
            api_key=profile.api_key,
            max_tokens=max_tokens,
            temperature=temperature,
            cache=cache,
        )
        _DSPY_LM_CACHE[cache_key] = lm
    return lm


def get_openai_request_kwargs(profile_name: str) -> Dict[str, Any]:
    profile = resolve_profile(profile_name)
    kwargs: Dict[str, Any] = {}
    if profile.reasoning_effort:
        kwargs["reasoning_effort"] = profile.reasoning_effort
    if profile.extra_body:
        kwargs["extra_body"] = profile.extra_body
    return kwargs


def ensure_dspy_defaults() -> None:
    dspy.configure(cache=False, adapter=dspy.ChatAdapter())


def ensure_no_proxy_for_llm(base_url: str) -> None:
    host = urlparse(base_url).hostname
    if not host:
        return

    should_bypass = host in {"localhost", "127.0.0.1", "::1"}
    if not should_bypass:
        try:
            ip = ipaddress.ip_address(host)
            should_bypass = ip.is_private or ip.is_loopback
        except ValueError:
            should_bypass = False

    if not should_bypass:
        return

    current = os.environ.get("NO_PROXY", "")
    entries = [entry.strip() for entry in current.split(",") if entry.strip()]
    if host not in entries:
        entries.append(host)
        updated = ",".join(entries)
        os.environ["NO_PROXY"] = updated
        os.environ["no_proxy"] = updated


def strip_openai_provider_prefix(model: str) -> str:
    if model.startswith("openai/"):
        return model.split("/", 1)[1]
    return model


def _normalize_profile_name(profile_name: str) -> LLMProfileName:
    if profile_name not in _VALID_PROFILE_NAMES:
        raise ValueError(
            f"Unsupported LLM profile '{profile_name}'. Expected one of: {', '.join(sorted(_VALID_PROFILE_NAMES))}"
        )
    return profile_name  # type: ignore[return-value]


def _resolve_profile_model(profile_name: LLMProfileName, suffix: str) -> str:
    profile_model = getattr(settings, f"llm_profile_{suffix}_model")
    if profile_model:
        return profile_model
    if profile_name == "repair_think" and settings.llm_repair_model:
        return settings.llm_repair_model
    return settings.llm_model


def _parse_extra_body_json(profile_name: str, raw_value: str) -> Dict[str, Any]:
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid EXTRA_BODY_JSON for LLM profile '{profile_name}': {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"EXTRA_BODY_JSON for LLM profile '{profile_name}' must decode to an object")
    return parsed
