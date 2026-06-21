# dashboard.py
import streamlit as st
import json
import subprocess
import sys
from pathlib import Path
from config import settings
import time
from typing import Dict, List, Optional
import re
from datetime import datetime, timezone

DIAGNOSTIC_STAGES = ["translate", "pattern_guard", "formatter", "api_validation", "trino_test", "compare", "report"]
STALE_TRANSLATING_MINUTES = 30
RETRY_SENTINEL_RE = re.compile(r"Source file for part \d+ not found", re.IGNORECASE)

# Автообновление
try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=2000, key="auto_refresh")
except ImportError:
    pass

st.set_page_config(page_title="Ver2Tri Monitor", layout="wide", initial_sidebar_state="collapsed")

st.title("🚀 Ver2Tri Migration Monitor")
st.caption(f"📡 Последнее обновление: {time.strftime('%H:%M:%S')}")


def read_json_file(file_path: Path) -> Optional[dict]:
    """Безопасное чтение JSON файла."""
    if not file_path.exists():
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def extract_validation_url(api_report: dict) -> Optional[str]:
    """
    Извлекает ссылку на отчет из reports/api_validation_report.json.
    Сначала ищет поле 'url' или 'validation_report_url', если не найдено - из текста ошибки.
    """
    # Прямой поиск URL в ключах отчета
    direct_keys = ['url', 'validation_report_url', 'report_url']
    for key in direct_keys:
        if api_report.get(key):
            return str(api_report[key])
    
    # Поиск URL в тексте ошибки как запасной вариант
    error_msg = api_report.get("error", "")
    if error_msg:
        url_pattern = r'https?://[^\\s<>"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, error_msg)
        if urls:
            return urls[0]
    
    return None


def get_validation_report_url(
    api_report: Optional[dict],
    diagnostics: Optional[dict] = None,
) -> Optional[str]:
    """Возвращает ссылку на validation report из api_report или metadata diagnostics."""
    if isinstance(api_report, dict):
        direct_url = extract_validation_url(api_report)
        if direct_url:
            return direct_url

    if isinstance(diagnostics, dict):
        api_validation = diagnostics.get("api_validation")
        if isinstance(api_validation, dict):
            details = api_validation.get("details")
            if isinstance(details, dict):
                return extract_validation_url(details)

    return None


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """Парсит ISO datetime из metadata.json."""
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def format_duration(started_at: Optional[str], finished_at: Optional[str] = None) -> str:
    """Форматирует общее время миграции в компактный вид."""
    start_dt = parse_iso_datetime(started_at)
    if not start_dt:
        return "—"

    end_dt = parse_iso_datetime(finished_at) or datetime.now(timezone.utc)
    total_seconds = max(0, int((end_dt - start_dt).total_seconds()))

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}ч {minutes}м"
    if minutes > 0:
        return f"{minutes}м {seconds}с"
    return f"{seconds}с"


def normalize_diagnostics(diagnostics: Optional[dict]) -> dict:
    normalized = {
        stage: {"status": "pending", "errors": [], "updated_at": None, "details": {}}
        for stage in DIAGNOSTIC_STAGES
    }
    normalized["review_notes"] = []

    if not isinstance(diagnostics, dict):
        return normalized

    if isinstance(diagnostics.get("review_notes"), list):
        normalized["review_notes"] = diagnostics["review_notes"]

    for stage in DIAGNOSTIC_STAGES:
        stage_block = diagnostics.get(stage)
        if isinstance(stage_block, dict):
            if stage_block.get("status"):
                normalized[stage]["status"] = stage_block["status"]
            if isinstance(stage_block.get("errors"), list):
                normalized[stage]["errors"] = stage_block["errors"]
            if stage_block.get("updated_at"):
                normalized[stage]["updated_at"] = stage_block["updated_at"]
            if isinstance(stage_block.get("details"), dict):
                normalized[stage]["details"] = stage_block["details"]

    legacy_issues = diagnostics.get("issues")
    if isinstance(legacy_issues, list):
        for issue in legacy_issues:
            if not isinstance(issue, dict):
                continue
            stage = issue.get("stage")
            if stage in DIAGNOSTIC_STAGES:
                normalized[stage]["errors"].append(issue)

    return normalized


def diagnostics_summary(diagnostics: dict) -> str:
    parts = []
    for stage in DIAGNOSTIC_STAGES:
        count = len(diagnostics.get(stage, {}).get("errors", []))
        status = diagnostics.get(stage, {}).get("status")
        if count or status not in (None, "pending"):
            suffix = f"{stage}: {status or 'unknown'}"
            if count:
                suffix += f" ({count})"
            parts.append(suffix)
    return " | ".join(parts) if parts else "Ошибок не зафиксировано"


def stage_errors_text(diagnostics: dict, stage: str) -> str:
    """Собирает ошибки конкретного этапа в одну строку для экспорта."""
    errors = diagnostics.get(stage, {}).get("errors", [])
    messages = []
    for issue in errors:
        if not isinstance(issue, dict):
            continue
        message = issue.get("message", "Без сообщения")
        details = issue.get("details")
        if details:
            message = f"{message} | {json.dumps(details, ensure_ascii=False, default=str)}"
        messages.append(message)
    return "\n".join(messages)


def is_stage_retryable(diagnostics: dict, stage: str) -> bool:
    """Проверяет, требует ли этап retry по diagnostics."""
    return (diagnostics.get(stage) or {}).get("status") in {"failed", "warning"}


def stale_translating_info(status: str, last_modified: Optional[str]) -> dict:
    """Возвращает сведения о зависшем translating workflow по last_modified."""
    updated_at = parse_iso_datetime(last_modified)
    if status != "translating" or not updated_at:
        return {"is_stale": False, "minutes": None, "last_modified": last_modified}

    age_seconds = max(0, int((datetime.now(timezone.utc) - updated_at).total_seconds()))
    age_minutes = age_seconds // 60
    return {
        "is_stale": age_minutes >= STALE_TRANSLATING_MINUTES,
        "minutes": age_minutes,
        "last_modified": last_modified,
    }


def stringify_error_details(details: object) -> str:
    if not details:
        return ""
    if isinstance(details, str):
        return details
    return json.dumps(details, ensure_ascii=False, default=str)


def make_error_summary(
    stage: str,
    message: object,
    *,
    source: str,
    details: object = None,
    updated_at: Optional[str] = None,
) -> Optional[dict]:
    if message is None:
        return None
    message_text = str(message).strip()
    if not message_text:
        return None
    return {
        "stage": stage,
        "source": source,
        "message": message_text,
        "details": stringify_error_details(details),
        "updated_at": updated_at,
    }


def is_retry_sentinel_error(error_item: Optional[dict]) -> bool:
    if not error_item:
        return False
    message = error_item.get("message", "")
    details = error_item.get("details", "")
    return bool(RETRY_SENTINEL_RE.search(f"{message} {details}"))


def collect_diagnostic_errors(diagnostics: dict) -> List[dict]:
    errors = []
    for stage in DIAGNOSTIC_STAGES:
        stage_block = diagnostics.get(stage) or {}
        updated_at = stage_block.get("updated_at")
        details = stage_block.get("details")
        if is_stage_retryable(diagnostics, stage) and details:
            message = "Stage failed"
            if isinstance(details, dict):
                message = details.get("error") or details.get("message") or details.get("warning") or message
            item = make_error_summary(
                stage,
                message,
                source="diagnostics",
                details=details,
                updated_at=updated_at,
            )
            if item:
                errors.append(item)
        for issue in stage_block.get("errors", []):
            if not isinstance(issue, dict):
                continue
            item = make_error_summary(
                stage,
                issue.get("message", "Без сообщения"),
                source="diagnostics",
                details=issue.get("details"),
                updated_at=issue.get("created_at") or issue.get("updated_at") or updated_at,
            )
            if item:
                errors.append(item)
    return errors


def latest_error(errors: List[dict]) -> Optional[dict]:
    if not errors:
        return None

    def sort_key(item: dict) -> tuple[int, int]:
        parsed = parse_iso_datetime(item.get("updated_at"))
        timestamp = int(parsed.timestamp()) if parsed else 0
        try:
            stage_order = DIAGNOSTIC_STAGES.index(item.get("stage"))
        except ValueError:
            stage_order = -1
        return timestamp, stage_order

    return sorted(errors, key=sort_key, reverse=True)[0]


def extract_report_error(stage: str, report: Optional[dict], source: str) -> Optional[dict]:
    if not isinstance(report, dict):
        return None
    return make_error_summary(
        stage,
        report.get("error") or report.get("message"),
        source=source,
        details=report.get("details") or report.get("repair_plan"),
        updated_at=report.get("updated_at") or report.get("completed_at") or report.get("created_at"),
    )


def extract_api_report_error(api_report: Optional[dict]) -> Optional[dict]:
    if not isinstance(api_report, dict):
        return None
    remaining_errors = api_report.get("remaining_errors")
    if remaining_errors:
        return make_error_summary(
            "api_validation",
            f"API validation errors: {len(remaining_errors)}",
            source="api_validation_report",
            details=remaining_errors,
            updated_at=api_report.get("updated_at") or api_report.get("completed_at"),
        )
    return extract_report_error("api_validation", api_report, "api_validation_report")


def extract_runtime_error(runtime_state: dict) -> Optional[dict]:
    if not isinstance(runtime_state, dict):
        return None
    return make_error_summary(
        "trino_test",
        runtime_state.get("last_error_text") or runtime_state.get("error_text"),
        source="test_runtime",
        details={
            key: runtime_state.get(key)
            for key in ("part_num", "fix_attempt", "replay_target_part", "phase")
            if runtime_state.get(key) is not None
        },
        updated_at=runtime_state.get("updated_at"),
    )


def extract_pipeline_retry_error(metadata: dict, diagnostics: dict) -> Optional[dict]:
    pipeline = metadata.get("pipeline") if isinstance(metadata.get("pipeline"), dict) else {}
    last_stage_result = (
        pipeline.get("last_stage_result")
        if isinstance(pipeline.get("last_stage_result"), dict)
        else {}
    )
    if last_stage_result.get("status") == "failed":
        details = last_stage_result.get("details") or {}
        return make_error_summary(
            last_stage_result.get("stage") or pipeline.get("current_stage") or "pipeline",
            details.get("error") or last_stage_result.get("error") or last_stage_result.get("message"),
            source="last_stage_result",
            details=details,
            updated_at=last_stage_result.get("finished_at") or last_stage_result.get("started_at"),
        )
    return latest_error(collect_diagnostic_errors(diagnostics))


def extract_error_pair(
    metadata: dict,
    diagnostics: dict,
    api_report: Optional[dict],
    trino_test_report: Optional[dict],
    compare_report: Optional[dict],
    runtime_state: dict,
) -> tuple[Optional[dict], Optional[dict]]:
    """Разделяет настоящую причину остановки и ошибку последнего retry."""
    retry_error = extract_pipeline_retry_error(metadata, diagnostics)
    diagnostic_errors = collect_diagnostic_errors(diagnostics)
    candidates = [
        extract_runtime_error(runtime_state),
        extract_report_error("trino_test", trino_test_report, "trino_test_report"),
        extract_report_error("compare", compare_report, "compare_report"),
        extract_api_report_error(api_report),
        *diagnostic_errors,
    ]
    real_errors = [
        item for item in candidates
        if item and not is_retry_sentinel_error(item)
    ]
    return latest_error(real_errors), retry_error


def path_to_uri(path: Optional[Path]) -> str:
    """Преобразует локальный путь в file:// URI для Excel."""
    if path is None:
        return ""
    try:
        return path.resolve().as_uri()
    except ValueError:
        return str(path)


def has_all_translated_parts(work_dir: Path, query_name: str, total_parts: int) -> bool:
    """Проверяет, что у файла есть Trino-части для повторного старта с pattern_guard."""
    if total_parts <= 0:
        return False

    trino_parts_dir = work_dir / "trino_parts"
    version_pattern = re.compile(
        rf"^{re.escape(query_name)}_part_(?P<part_num>\d+)_trino(?:_v(?P<version>\d+))?\.sql$"
    )

    found_parts = set()
    for file_path in trino_parts_dir.glob(f"{query_name}_part_*_trino*.sql"):
        match = version_pattern.match(file_path.name)
        if match:
            found_parts.add(int(match.group("part_num")))

    return len(found_parts) >= total_parts


def compute_retry_stage(work_dir: Path, query_name: str, total_parts: int) -> str:
    """Определяет, с какого этапа retry пойдёт для файла."""
    metadata = read_json_file(work_dir / "metadata.json") or {}
    diagnostics = normalize_diagnostics(metadata.get("diagnostics"))
    reports = metadata.get("reports") if isinstance(metadata.get("reports"), dict) else {}
    runtime_state = metadata.get("test_runtime") if isinstance(metadata.get("test_runtime"), dict) else {}

    if is_stage_retryable(diagnostics, "compare") or reports.get("compare_report"):
        return "compare"
    if (
        is_stage_retryable(diagnostics, "trino_test")
        or reports.get("trino_test_report")
        or runtime_state.get("last_failed_part") is not None
        or runtime_state.get("last_error_text")
        or runtime_state.get("error_text")
    ):
        return "trino_test"
    if is_stage_retryable(diagnostics, "api_validation"):
        return "api_validate"
    if is_stage_retryable(diagnostics, "pattern_guard"):
        return "pattern_guard"
    if has_all_translated_parts(work_dir, query_name, total_parts):
        return "pattern_guard"
    return "translate"


def open_in_file_manager(target_path: Path) -> tuple[bool, str]:
    """Открывает локальную папку в системном файловом менеджере."""
    if not target_path.exists():
        return False, f"Path not found: {target_path}"

    try:
        if sys.platform == "darwin":
            subprocess.Popen(["open", str(target_path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif sys.platform == "win32":
            subprocess.Popen(["explorer", str(target_path)])
        else:
            subprocess.Popen(["xdg-open", str(target_path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True, f"Opened: {target_path}"
    except Exception as exc:
        return False, str(exc)


def launch_retry_process(query_name: str, retry_stage: str) -> tuple[bool, str]:
    """Запускает повторную обработку файла отдельным процессом main.py."""
    project_root = Path(__file__).resolve().parent
    main_path = project_root / "main.py"
    cmd = [
        sys.executable,
        str(main_path),
        "--retry",
        query_name,
        "--retry-from",
        retry_stage,
        "--no-dashboard",
    ]

    try:
        popen_kwargs = {
            "cwd": str(project_root),
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        if sys.platform == "win32":
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_CONSOLE
        else:
            popen_kwargs["start_new_session"] = True

        subprocess.Popen(cmd, **popen_kwargs)
        return True, f"Retry started for {query_name} from {retry_stage}"
    except Exception as exc:
        return False, str(exc)


def build_excel_xml(rows: List[dict]) -> bytes:
    def escape(value: object) -> str:
        text = "" if value is None else str(value)
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    headers = [
        "name", "bucket", "status", "final_status", "elapsed_time", "operation",
        "last_modified", "stale_translating", "stale_minutes",
        "last_real_error_stage", "last_real_error_message",
        "last_retry_error_stage", "last_retry_error_message",
        "retry_stage", "work_dir", "work_dir_uri", "source_sql_path", "source_sql_uri",
        "final_trino_path", "final_trino_uri", "validation_report_url",
        "translate_errors", "translate_error_messages",
        "pattern_guard_errors", "pattern_guard_error_messages",
        "formatter_errors", "formatter_error_messages",
        "api_validation_errors", "api_validation_error_messages", "review_notes",
    ]
    rows_xml = [
        '<Row>' + ''.join(
            f'<Cell><Data ss:Type="String">{escape(header)}</Data></Cell>'
            for header in headers
        ) + '</Row>'
    ]
    for row in rows:
        rows_xml.append(
            '<Row>' + ''.join(
                f'<Cell><Data ss:Type="String">{escape(row.get(header, ""))}</Data></Cell>'
                for header in headers
            ) + '</Row>'
        )

    xml = (
        '<?xml version="1.0"?>\n'
        '<?mso-application progid="Excel.Sheet"?>\n'
        '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" '
        'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">\n'
        '<Worksheet ss:Name="Ver2Tri"><Table>\n'
        + "\n".join(rows_xml) +
        '\n</Table></Worksheet>\n</Workbook>'
    )
    return xml.encode("utf-8")


def render_diagnostics(file_info: dict) -> None:
    diagnostics = file_info.get("diagnostics", {})
    for stage in DIAGNOSTIC_STAGES:
        errors = diagnostics.get(stage, {}).get("errors", [])
        status = diagnostics.get(stage, {}).get("status")
        details = diagnostics.get(stage, {}).get("details")
        if errors or status not in (None, "pending"):
            st.markdown(f"**{stage}**")
            if status not in (None, "pending"):
                st.caption(f"status: {status}")
            if details:
                st.caption(json.dumps(details, ensure_ascii=False, indent=2, default=str))
            for issue in errors:
                st.write(f"- {issue.get('message', 'Без сообщения')}")
                if issue.get("details"):
                    st.caption(json.dumps(issue["details"], ensure_ascii=False, indent=2, default=str))
    review_notes = diagnostics.get("review_notes", [])
    if review_notes:
        st.markdown("**review_notes**")
        for note in review_notes:
            st.write(f"- {note}")


def render_error_card(title: str, error_item: Optional[dict]) -> None:
    st.markdown(f"**{title}**")
    if not error_item:
        st.caption("Не зафиксирована")
        return
    st.write(f"`{error_item.get('stage', '—')}` · {error_item.get('source', '—')}")
    st.caption(error_item.get("message", "Без сообщения"))
    if error_item.get("details"):
        st.code(error_item["details"], language="text")


def render_recovery_summary(file_info: dict) -> None:
    stale = file_info.get("stale_translating") or {}
    if stale.get("is_stale"):
        st.warning(
            "Stale translating: "
            f"last_modified={stale.get('last_modified') or '—'}, "
            f"{stale.get('minutes')} мин. без обновлений"
        )

    cols = st.columns(2)
    with cols[0]:
        render_error_card("Последняя настоящая ошибка", file_info.get("last_real_error"))
    with cols[1]:
        render_error_card("Ошибка последнего retry", file_info.get("last_retry_error"))


def _empty_workflow_stats() -> Dict[str, object]:
    return {
        "total": 0,
        "done": 0,
        "review": 0,
        "remaining": 0,
        "active_files": [],
        "completed_files": [],
        "review_files": [],
    }


def _resolve_final_trino_path(work_dir: Path, query_name: str, final_status: Optional[str]) -> Path:
    assembled_sql_path = work_dir / "final" / f"{query_name}_final.sql"
    review_trino_path = settings.review_path / "trino" / f"{query_name}_trino.sql"
    done_trino_path = settings.done_path / "trino" / f"{query_name}_trino.sql"

    if final_status == "review" and review_trino_path.exists():
        return review_trino_path
    if final_status == "completed" and done_trino_path.exists():
        return done_trino_path
    return assembled_sql_path


def _resolve_registered_report_path(work_dir: Path, reports: Optional[dict], report_key: str) -> Optional[Path]:
    if not isinstance(reports, dict):
        return None
    raw_path = reports.get(report_key)
    if not raw_path:
        return None
    path = Path(raw_path)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    if not path.exists():
        fallback = work_dir / "reports" / path.name
        if fallback.exists():
            return fallback
    return path if path.exists() else None


def _load_workflow_reports(work_dir: Path, reports: Optional[dict]) -> tuple[Optional[dict], Optional[dict], Optional[dict]]:
    api_path = _resolve_registered_report_path(work_dir, reports, "api_validation_report")
    trino_path = _resolve_registered_report_path(work_dir, reports, "trino_test_report")
    compare_path = _resolve_registered_report_path(work_dir, reports, "compare_report")
    return (
        read_json_file(api_path) if api_path else None,
        read_json_file(trino_path) if trino_path else None,
        read_json_file(compare_path) if compare_path else None,
    )


def _count_distinct_parts(parts_dir: Path, query_name: str, kind: str) -> int:
    if not parts_dir.exists():
        return 0
    if kind == "trino":
        pattern = re.compile(
            rf"^{re.escape(query_name)}_part_(?P<part_num>\d+)_trino(?:_v(?P<version>\d+))?\.sql$"
        )
        glob_pattern = f"{query_name}_part_*_trino*.sql"
    else:
        pattern = re.compile(
            rf"^{re.escape(query_name)}_part_(?P<part_num>\d+)\.sql$"
        )
        glob_pattern = f"{query_name}_part_*.sql"
    part_nums = set()
    for path in parts_dir.glob(glob_pattern):
        match = pattern.match(path.name)
        if match:
            part_nums.add(int(match.group("part_num")))
    return len(part_nums)


def _effective_total_parts(work_dir: Path, query_name: str, metadata_total_parts: int) -> int:
    if isinstance(metadata_total_parts, int) and metadata_total_parts > 0:
        return metadata_total_parts
    vertica_parts = _count_distinct_parts(work_dir / "vertica_parts", query_name, "sql")
    trino_parts = _count_distinct_parts(work_dir / "trino_parts", query_name, "trino")
    return max(vertica_parts, trino_parts)


def _stage_progress_percent(
    *,
    status: str,
    final_status: Optional[str],
    total_parts: int,
    current_part: int,
    diagnostics: dict,
    reports: Optional[dict],
    runtime_state: dict,
    compare_runtime: dict,
    work_dir: Path,
    query_name: str,
) -> float:
    if final_status in {"completed", "review"}:
        return 100.0

    translated_parts = _count_distinct_parts(work_dir / "trino_parts", query_name, "trino")
    translated_ratio = min(1.0, translated_parts / total_parts) if total_parts > 0 else 0.0
    current_ratio = min(1.0, current_part / total_parts) if total_parts > 0 else 0.0
    ratio = max(translated_ratio, current_ratio)

    if (diagnostics.get("report") or {}).get("status") == "success":
        return 99.0
    if (diagnostics.get("compare") or {}).get("status") in {"success", "warning"} or reports and reports.get("compare_report"):
        return 97.0
    if (diagnostics.get("trino_test") or {}).get("status") in {"success", "warning"} or runtime_state.get("executed_parts"):
        return 92.0
    if (diagnostics.get("api_validation") or {}).get("status") in {"success", "warning"} or reports and reports.get("api_validation_report"):
        return 86.0
    if (diagnostics.get("assemble") or {}).get("status") == "success" or (work_dir / "final" / f"{query_name}_final.sql").exists():
        return 80.0
    if (diagnostics.get("formatter") or {}).get("status") in {"success", "warning"}:
        return 72.0
    if (diagnostics.get("pattern_guard") or {}).get("status") in {"success", "warning"}:
        return 60.0 + (10.0 * ratio)
    if status == "translating" or translated_parts > 0:
        return 10.0 + (50.0 * ratio)
    if status == "splitting":
        return 5.0
    if compare_runtime.get("current_phase"):
        return 97.0
    return 0.0


def _progress_label(file_info: dict) -> str:
    total_parts = file_info.get("total_parts", 0) or 0
    current_part = file_info.get("current_part", 0) or 0
    progress = float(file_info.get("progress", 0) or 0)
    status = file_info.get("status", "unknown")
    operation = str(file_info.get("operation") or "").strip()

    if total_parts > 0 and current_part > 0:
        clamped_part = min(current_part, total_parts)
        return f"Part {clamped_part}/{total_parts}"

    if total_parts > 0 and status == "translating":
        approx_part = max(1, min(total_parts, round((progress / 100.0) * total_parts)))
        return f"Part ~{approx_part}/{total_parts}"

    if progress > 0:
        stage_label = operation if operation and operation != "—" else status
        return f"{stage_label} ({progress:.0f}%)"

    if total_parts > 0:
        return f"Part 0/{total_parts}"

    return "Инициализация..."


def _derive_workflow_bucket(status: str, final_status: Optional[str]) -> str:
    if final_status == "completed":
        return "done"
    if final_status in ("review", "failed") or status in {"review", "failed"}:
        return "review"
    return "active"


def _build_file_info(work_dir: Path, metadata: dict) -> dict:
    query_name = work_dir.name
    status = metadata.get("status", "unknown")
    final_status = metadata.get("final_status")
    total_parts = _effective_total_parts(work_dir, query_name, metadata.get("total_parts", 0))
    created_at = metadata.get("created_at")
    completed_at = metadata.get("completed_at")
    last_modified = metadata.get("last_modified")
    diagnostics = normalize_diagnostics(metadata.get("diagnostics"))
    reports = metadata.get("reports") if isinstance(metadata.get("reports"), dict) else {}
    runtime_state = metadata.get("test_runtime", {}) if isinstance(metadata.get("test_runtime"), dict) else {}
    compare_runtime = metadata.get("compare_runtime", {}) if isinstance(metadata.get("compare_runtime"), dict) else {}
    retry_stage = compute_retry_stage(work_dir, query_name, total_parts)
    source_sql_path = work_dir / f"{query_name}.sql"
    final_trino_path = _resolve_final_trino_path(work_dir, query_name, final_status)
    api_report, trino_test_report, compare_report = _load_workflow_reports(work_dir, reports)
    validation_report_url = get_validation_report_url(api_report, diagnostics)
    stale_info = stale_translating_info(status, last_modified)
    last_real_error, last_retry_error = extract_error_pair(
        metadata,
        diagnostics,
        api_report,
        trino_test_report,
        compare_report,
        runtime_state,
    )

    return {
        "name": query_name,
        "status": status,
        "final_status": final_status,
        "total_parts": total_parts,
        "current_part": metadata.get("current_part", 0),
        "progress": _stage_progress_percent(
            status=status,
            final_status=final_status,
            total_parts=total_parts,
            current_part=metadata.get("current_part", 0),
            diagnostics=diagnostics,
            reports=reports,
            runtime_state=runtime_state,
            compare_runtime=compare_runtime,
            work_dir=work_dir,
            query_name=query_name,
        ),
        "operation": metadata.get("current_operation", "—"),
        "operation_details": metadata.get("operation_details", {}),
        "created_at": created_at,
        "completed_at": completed_at,
        "last_modified": last_modified,
        "stale_translating": stale_info,
        "last_real_error": last_real_error,
        "last_retry_error": last_retry_error,
        "has_api_errors": bool((api_report or {}).get("remaining_errors")) if api_report else False,
        "validation_report_url": validation_report_url,
        "elapsed_time": format_duration(created_at, completed_at),
        "diagnostics": diagnostics,
        "runtime_state": runtime_state,
        "compare_runtime": compare_runtime,
        "trino_test_report": trino_test_report,
        "compare_report": compare_report,
        "diagnostics_summary": diagnostics_summary(diagnostics),
        "retry_stage": retry_stage,
        "work_dir": str(work_dir.resolve()),
        "work_dir_path": work_dir,
        "work_dir_uri": path_to_uri(work_dir),
        "source_sql_path": str(source_sql_path.resolve()),
        "source_sql_uri": path_to_uri(source_sql_path),
        "final_trino_path": str(final_trino_path.resolve()) if final_trino_path.exists() else "",
        "final_trino_uri": path_to_uri(final_trino_path) if final_trino_path.exists() else "",
    }


def _append_file_info(stats: Dict[str, object], file_info: dict) -> None:
    stats["total"] += 1
    bucket = _derive_workflow_bucket(file_info["status"], file_info["final_status"])
    if bucket == "done":
        stats["done"] += 1
        stats["completed_files"].append(file_info)
    elif bucket == "review":
        stats["review"] += 1
        stats["review_files"].append(file_info)
    else:
        stats["remaining"] += 1
        stats["active_files"].append(file_info)


def get_workflow_stats() -> Dict:
    """
    Сканирует все папки в in_progress/, читает metadata.json и reports/api_validation_report.json
    из каждой папки для определения точного статуса.
    """
    stats = _empty_workflow_stats()

    if not settings.in_progress_path.exists():
        return stats

    for work_dir in settings.in_progress_path.iterdir():
        if not work_dir.is_dir():
            continue
        metadata = read_json_file(work_dir / "metadata.json")
        if metadata is None:
            continue
        _append_file_info(stats, _build_file_info(work_dir, metadata))

    return stats


def get_in_queue_files() -> List[Path]:
    """Получает список всех SQL файлов в папке in_queue."""
    if not settings.in_queue_path.exists():
        return []
    
    # Получаем только .sql файлы, исключая скрытые файлы
    files = [
        f for f in settings.in_queue_path.iterdir() 
        if f.is_file() and f.suffix == '.sql' and not f.name.startswith('.')
    ]
    return sorted(files)


# Получаем данные
stats = get_workflow_stats()
in_queue_files = get_in_queue_files()
all_files = stats["active_files"] + stats["completed_files"] + stats["review_files"]
stale_translating_count = sum(
    1 for item in all_files
    if (item.get("stale_translating") or {}).get("is_stale")
)

# Используем количество файлов в in_queue как общее количество
total_base = len(in_queue_files)

# Расчет процентов
def calc_pct(value, total):
    if total == 0:
        return 0
    return (value / total) * 100


# KPI Карточки - в ряд по 4 метрики
st.subheader("📊 Общая статистика")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("📁 Всего файлов", f"{total_base}")

with col2:
    pct_done = calc_pct(stats['done'], total_base)
    st.metric(
        "✅ Done", 
        f"{stats['done']} / {total_base}",
        delta=f"{pct_done:.1f}%"
    )

# Расчет оставшихся файлов: Общее - Done - Ревью
remaining_calculated = total_base - stats['done'] - stats['review']
with col3:
    pct_remaining = calc_pct(remaining_calculated, total_base)
    st.metric(
        "⏳ Осталось", 
        f"{remaining_calculated} / {total_base}",
        delta=f"{pct_remaining:.1f}%",
        delta_color="off"
    )

with col4:
    pct_review = calc_pct(stats['review'], total_base)
    st.metric(
        "👀 На ревью", 
        f"{stats['review']} / {total_base}",
        delta=f"{pct_review:.1f}%",
        delta_color="inverse" if stats['review'] > 0 else "off"
    )

# Общий прогресс-бар (прогресс завершенных)
st.divider()
if total_base > 0:
    completed_pct = stats['done'] / total_base
    st.progress(
        completed_pct, 
        text=f"Общий прогресс: {stats['done']}/{total_base} завершено ({completed_pct*100:.0f}%)"
    )
else:
    st.progress(0, text="Нет активных задач")

if all_files:
    export_rows = []
    for bucket, files in (
        ("active", stats["active_files"]),
        ("done", stats["completed_files"]),
        ("review", stats["review_files"]),
    ):
        for file_info in files:
            stale = file_info.get("stale_translating") or {}
            real_error = file_info.get("last_real_error") or {}
            retry_error = file_info.get("last_retry_error") or {}
            export_rows.append({
                "name": file_info["name"],
                "bucket": bucket,
                "status": file_info["status"],
                "final_status": file_info["final_status"],
                "elapsed_time": file_info["elapsed_time"],
                "operation": file_info["operation"],
                "last_modified": file_info.get("last_modified") or "",
                "stale_translating": stale.get("is_stale", False),
                "stale_minutes": stale.get("minutes") or "",
                "last_real_error_stage": real_error.get("stage", ""),
                "last_real_error_message": real_error.get("message", ""),
                "last_retry_error_stage": retry_error.get("stage", ""),
                "last_retry_error_message": retry_error.get("message", ""),
                "retry_stage": file_info["retry_stage"],
                "work_dir": file_info["work_dir"],
                "work_dir_uri": file_info["work_dir_uri"],
                "source_sql_path": file_info["source_sql_path"],
                "source_sql_uri": file_info["source_sql_uri"],
                "final_trino_path": file_info["final_trino_path"],
                "final_trino_uri": file_info["final_trino_uri"],
                "validation_report_url": file_info.get("validation_report_url") or "",
                "translate_errors": len(file_info["diagnostics"]["translate"]["errors"]),
                "translate_error_messages": stage_errors_text(file_info["diagnostics"], "translate"),
                "pattern_guard_errors": len(file_info["diagnostics"]["pattern_guard"]["errors"]),
                "pattern_guard_error_messages": stage_errors_text(file_info["diagnostics"], "pattern_guard"),
                "formatter_errors": len(file_info["diagnostics"]["formatter"]["errors"]),
                "formatter_error_messages": stage_errors_text(file_info["diagnostics"], "formatter"),
                "api_validation_errors": len(file_info["diagnostics"]["api_validation"]["errors"]),
                "api_validation_error_messages": stage_errors_text(file_info["diagnostics"], "api_validation"),
                "review_notes": " | ".join(file_info["diagnostics"].get("review_notes", [])),
            })

    st.download_button(
        "📥 Экспорт в Excel",
        data=build_excel_xml(export_rows),
        file_name=f"ver2tri_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls",
        mime="application/vnd.ms-excel",
    )

# Детализация
st.caption(f"🔍 Детали: Активно: {len(stats['active_files'])} | "
           f"Завершено: {len(stats['completed_files'])} | "
           f"На ревью: {len(stats['review_files'])} | "
           f"Stale translating: {stale_translating_count}")

st.divider()

# Таблица активных задач
if stats['active_files']:
    st.subheader(f"⚙️ В работе ({len(stats['active_files'])})")
    
    for mig in stats['active_files']:
        cols = st.columns([3, 2, 4, 1])
        
        status_icons = {
            "initialized": "🔵", 
            "splitting": "🟡", 
            "translating": "🟠",
            "validating": "🟣", 
            "assembling": "🔵", 
            "api_validating": "🔌",
            "unknown": "⚪"
        }
        icon = status_icons.get(mig['status'], "⚪")
        
        with cols[0]:
            st.markdown(f"""
            **{mig['name']}**<br>
            {icon} `{mig['status']}`<br>
            <small style="color: #888;">⏱ {mig['elapsed_time']}</small><br>
            <small style="color: #888;">{mig['operation']}</small>
            """, unsafe_allow_html=True)
        
        with cols[2]:
            if mig['total_parts'] > 0:
                progress = min(mig['progress'] / 100, 1.0)  # ограничиваем 1.0
                st.progress(
                    progress,
                    text=_progress_label(mig)
                )
            else:
                st.progress(0, text="Инициализация...")

        if (
            (mig.get("stale_translating") or {}).get("is_stale")
            or mig.get("last_real_error")
            or mig.get("last_retry_error")
        ):
            render_recovery_summary(mig)

        runtime_state = mig.get("runtime_state") or {}
        if runtime_state:
            runtime_cols = st.columns([2, 2, 2, 6])
            with runtime_cols[0]:
                st.caption(f"Runtime status: {runtime_state.get('status', '—')}")
            with runtime_cols[1]:
                phase = runtime_state.get("phase") or "—"
                st.caption(f"Phase: {phase}")
            with runtime_cols[2]:
                part_value = runtime_state.get("part_num")
                if part_value is not None:
                    st.caption(f"Part: {part_value}")
                else:
                    st.caption("Part: —")
            with runtime_cols[3]:
                message = runtime_state.get("message") or runtime_state.get("last_error_text") or runtime_state.get("error_text")
                if message:
                    st.caption(message)

            extra_lines = []
            if runtime_state.get("fix_attempt") is not None:
                extra_lines.append(f"fix attempt: {runtime_state['fix_attempt']}")
            if runtime_state.get("replay_target_part") is not None:
                extra_lines.append(f"replay target part: {runtime_state['replay_target_part']}")
            if runtime_state.get("error_text"):
                extra_lines.append(f"last error: {runtime_state['error_text']}")
            if runtime_state.get("diagnostic_query"):
                extra_lines.append("diagnostic query executed")
            if extra_lines:
                st.code("\n".join(extra_lines), language="text")

        trino_report = mig.get("trino_test_report") or {}
        if trino_report and (
            trino_report.get("runtime_fix_attempts")
            or trino_report.get("diagnostic_queries")
            or trino_report.get("introspection")
            or trino_report.get("error")
        ):
            with st.expander("Trino runtime details", expanded=False):
                if trino_report.get("error"):
                    st.error(trino_report["error"])
                if trino_report.get("runtime_fix_attempts"):
                    st.markdown("**Runtime fix attempts**")
                    for item in trino_report["runtime_fix_attempts"][-5:]:
                        st.write(
                            f"- part {item.get('part')} | attempt {item.get('attempt')} | {item.get('error', 'no error text')}"
                        )
                if trino_report.get("diagnostic_queries"):
                    st.markdown("**Diagnostic queries**")
                    for item in trino_report["diagnostic_queries"][-3:]:
                        st.write(f"- part {item.get('part')} | attempt {item.get('attempt')}")
                        st.caption(item.get("query", ""))
                if trino_report.get("introspection"):
                    st.markdown("**Introspection**")
                    for item in trino_report["introspection"][-3:]:
                        st.write(f"- part {item.get('part')} | requested: {', '.join(item.get('requested_items', []))}")

        compare_runtime = mig.get("compare_runtime") or {}
        compare_report = mig.get("compare_report") or {}
        if compare_runtime or compare_report:
            with st.expander("Trino compare details", expanded=False):
                if compare_runtime.get("current_phase"):
                    st.caption(f"Compare phase: {compare_runtime.get('current_phase')}")
                if compare_runtime.get("root_cause_summary"):
                    st.write(compare_runtime.get("root_cause_summary"))
                compare_block = compare_report.get("compare") if isinstance(compare_report, dict) else None
                if compare_block:
                    st.json(compare_block)
        
        st.divider()

# Завершенные (с ссылками на валидацию)
if stats['completed_files']:
    with st.expander(f"✅ Завершено ({len(stats['completed_files'])})"):
        for f in stats['completed_files']:
            cols = st.columns([3, 2, 2, 1, 1])
            
            with cols[0]:
                st.write(f"• {f['name']}")
                st.caption(f"⏱ {f['elapsed_time']}")
            
            with cols[1]:
                if f.get('validation_report_url'):
                    st.markdown(
                        f"🔗 [Отчет о валидации]({f['validation_report_url']})",
                        help="Нажмите для просмотра отчета"
                    )
                else:
                    st.caption("Нет отчета")

            with cols[2]:
                st.caption(f["diagnostics_summary"])

            with cols[3]:
                if st.button("📂 Папка", key=f"open-folder-done-{f['name']}"):
                    ok, message = open_in_file_manager(f["work_dir_path"])
                    if ok:
                        st.success(message)
                    else:
                        st.error(message)

            with cols[4]:
                if st.button("🔁 Retry", key=f"retry-done-{f['name']}"):
                    ok, message = launch_retry_process(f["name"], f["retry_stage"])
                    if ok:
                        st.success(message)
                    else:
                        st.error(message)

            with st.expander(f"Ошибки и заметки: {f['name']}"):
                st.caption(f"Следующий retry: {f['retry_stage']}")
                st.caption(f"Рабочая папка: {f['work_dir']}")
                render_recovery_summary(f)
                render_diagnostics(f)
            
            st.divider()

# На ревью (с подробностями об ошибках и ссылками)
if stats['review_files']:
    st.subheader(f"⚠️ Review-группы ({len(stats['review_files'])})")
    for f in sorted(stats['review_files'], key=lambda item: item["name"]):
        error_detail = " | API validation errors" if f.get('has_api_errors') else ""
        expander_title = f"{f['name']} | retry: {f['retry_stage']}{error_detail}"
        with st.expander(expander_title):
            header_cols = st.columns([3, 2])
            with header_cols[0]:
                st.markdown(f"**{f['name']}**")
            with header_cols[1]:
                if f.get('validation_report_url'):
                    st.markdown(f"🔗 [Отчет валидации]({f['validation_report_url']})")
                else:
                    st.caption("Нет отчета валидации")

            meta_cols = st.columns([2, 2, 2, 2])
            with meta_cols[0]:
                st.caption("Статус")
                st.write(f"`{f['status']}` / `{f['final_status'] or '—'}`")
            with meta_cols[1]:
                st.caption("Время")
                st.write(f["elapsed_time"])
            with meta_cols[2]:
                st.caption("Операция")
                st.write(f["operation"])
            with meta_cols[3]:
                st.caption("Следующий retry")
                st.write(f"`{f['retry_stage']}`")

            action_cols = st.columns([1, 1, 2])
            with action_cols[0]:
                if st.button("📂 Открыть папку", key=f"open-folder-{f['name']}"):
                    ok, message = open_in_file_manager(f["work_dir_path"])
                    if ok:
                        st.success(message)
                    else:
                        st.error(message)
            with action_cols[1]:
                if st.button("🔁 Retry", key=f"retry-{f['name']}"):
                    ok, message = launch_retry_process(f["name"], f["retry_stage"])
                    if ok:
                        st.success(message)
                    else:
                        st.error(message)
            with action_cols[2]:
                st.caption(f"Следующий retry: {f['retry_stage']}")

            st.caption(f"Рабочая папка: {f['work_dir']}")
            st.caption(f"Исходный SQL: {f['source_sql_path']}")
            if f.get("final_trino_path"):
                st.caption(f"Текущий финальный Trino: {f['final_trino_path']}")

            render_recovery_summary(f)
            st.markdown("**Ошибки по этапам**")
            render_diagnostics(f)
            st.divider()

# Список всех файлов с API report
st.divider()
st.subheader("📄 Все файлы")

if all_files:
    for file_info in sorted(all_files, key=lambda item: item["name"]):
        cols = st.columns([3, 2, 3])

        with cols[0]:
            st.write(f"• {file_info['name']}")

        with cols[1]:
            if file_info.get("validation_report_url"):
                st.markdown(f"🔗 [API report]({file_info['validation_report_url']})")
            else:
                st.caption("Нет API report")

        with cols[2]:
            st.caption(file_info["diagnostics_summary"])

        st.divider()
else:
    st.info("Нет файлов с metadata.json")

# Футер
st.divider()
if st.button("🔄 Обновить"):
    st.rerun()
