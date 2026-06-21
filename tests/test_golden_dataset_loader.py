import json

from dspy_modules.compiler import GoldenDatasetLoader


def test_golden_dataset_loader_preserves_context_hint_and_part_type(tmp_path):
    dataset = tmp_path / "examples.json"
    dataset.write_text(
        json.dumps(
            [
                {
                    "vertica": "select 1",
                    "trino": "SELECT 1",
                    "context_hint": "keep header rules",
                    "metadata": {"category": "header"},
                }
            ]
        ),
        encoding="utf-8",
    )

    examples = GoldenDatasetLoader(dataset).load()

    assert examples[0].context_hint == "keep header rules"
    assert examples[0].part_type == "header"

