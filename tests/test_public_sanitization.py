from __future__ import annotations

import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
TEXT_SUFFIXES = {
    ".py",
    ".md",
    ".MD",
    ".json",
    ".toml",
    ".example",
    ".txt",
    ".yml",
    ".yaml",
}
BLOCKED_SUBSTRINGS = [
    "/Users/",
    "/home/",
    "k.avito",
    "stash",
    "mattermost",
    "confluence",
    "bitrix",
    "salesforce",
    "insert_existing_partitions_behavior",
]
BLOCKED_JUNK = {
    ".DS_Store",
}


def _tracked_files() -> list[Path]:
    output = subprocess.check_output(["git", "ls-files"], cwd=REPO_ROOT, text=True)
    return [REPO_ROOT / line for line in output.splitlines() if line.strip()]


def test_tracked_files_do_not_include_known_junk():
    tracked_names = {path.name for path in _tracked_files()}
    assert BLOCKED_JUNK.isdisjoint(tracked_names)


def test_repository_text_files_do_not_contain_blocked_public_markers():
    offenders: list[str] = []

    for path in _tracked_files():
        if path.suffix not in TEXT_SUFFIXES and path.name not in {".env.example", ".gitignore"}:
            continue
        text = path.read_text(encoding="utf-8")
        lowered = text.lower()
        for marker in BLOCKED_SUBSTRINGS:
            if marker.lower() in lowered:
                offenders.append(f"{path.relative_to(REPO_ROOT)} -> {marker}")
        if "_u_" in text:
            offenders.append(f"{path.relative_to(REPO_ROOT)} -> _u_ private schema marker")

    assert not offenders, "Blocked public markers found:\n" + "\n".join(sorted(offenders))
