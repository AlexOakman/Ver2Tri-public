import json
from pathlib import Path

from core.migration_knowledge import MigrationKnowledgeRegistry


class _DummyStateManager:
    def load_state(self):
        return {}


def test_repo_golden_dataset_contains_runtime_fix_examples():
    dataset_path = Path("golden_dataset/examples.json")
    items = json.loads(dataset_path.read_text(encoding="utf-8"))
    ids = {item["id"] for item in items}

    assert {
        "gd_066",
        "gd_067",
        "gd_068",
        "gd_069",
        "gd_070",
        "gd_071",
        "gd_072",
        "gd_073",
        "gd_074",
        "gd_075",
        "gd_076",
        "gd_077",
        "gd_078",
        "gd_079",
        "gd_080",
    } <= ids

    assert {
        "gd_112",
        "gd_113",
        "gd_114",
        "gd_115",
        "gd_116",
        "gd_117",
        "gd_118",
        "gd_119",
        "gd_120",
        "gd_121",
        "gd_122",
        "gd_123",
    } <= ids


def test_golden_blocks_match_missing_column_name_tokens():
    registry = MigrationKnowledgeRegistry(_DummyStateManager(), dataset_dir=Path("golden_dataset"))

    blocks = registry._golden_blocks({"missing", "column", "name", "ctas", "alias"})

    assert blocks
    assert any(("gd_066" in block["text"]) or ("gd_073" in block["text"]) for block in blocks)


def test_golden_blocks_match_same_select_alias_reuse_tokens():
    registry = MigrationKnowledgeRegistry(_DummyStateManager(), dataset_dir=Path("golden_dataset"))

    blocks = registry._golden_blocks({"same", "select", "alias", "reuse", "derived", "column"})

    assert blocks
    assert any(("gd_071" in block["text"]) or ("gd_072" in block["text"]) for block in blocks)


def test_golden_blocks_match_unpartitioned_refresh_tokens():
    registry = MigrationKnowledgeRegistry(_DummyStateManager(), dataset_dir=Path("golden_dataset"))

    blocks = registry._golden_blocks({"unpartitioned", "overwrite", "recreate", "truncate", "ctas"})

    assert blocks
    assert any(
        ("gd_078" in block["text"])
        or ("gd_119" in block["text"])
        or ("gd_121" in block["text"])
        for block in blocks
    )


def test_golden_blocks_match_version_id_ordinal_tokens():
    registry = MigrationKnowledgeRegistry(_DummyStateManager(), dataset_dir=Path("golden_dataset"))

    blocks = registry._golden_blocks(
        {"launch_id", "version_id", "group", "by", "3", "order", "by", "4", "renumber"}
    )

    assert blocks
    assert any(
        ("gd_084" in block["text"])
        or ("gd_088" in block["text"])
        or ("gd_112" in block["text"])
        or ("gd_113" in block["text"])
        or ("gd_115" in block["text"])
        or ("gd_117" in block["text"])
        for block in blocks
    )


def test_golden_blocks_match_incremental_reload_tokens():
    registry = MigrationKnowledgeRegistry(_DummyStateManager(), dataset_dir=Path("golden_dataset"))

    blocks = registry._golden_blocks(
        {"delete", "truncate", "overwrite", "ctas", "partition", "reload"}
    )

    assert blocks
    assert any(
        ("gd_118" in block["text"])
        or ("gd_119" in block["text"])
        or ("gd_120" in block["text"])
        or ("gd_121" in block["text"])
        or ("gd_122" in block["text"])
        or ("gd_123" in block["text"])
        for block in blocks
    )
