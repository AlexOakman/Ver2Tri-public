import inspect
import json
import logging
from pathlib import Path

import dspy

from config import settings
from dspy_modules.compiler import DSPyCompiler
from dspy_modules.signature import REPAIR_MODE_GUIDANCE, VerticaToTrino, VerticaToTrinoProgram


class _FakeCompiledModule:
    def save(self, path: str, save_program: bool = False):
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if save_program:
            target.mkdir(parents=True, exist_ok=True)
            (target / "program.txt").write_text("saved", encoding="utf-8")
            return
        target.write_text("saved", encoding="utf-8")

    def __call__(self, vertica_sql: str, context_hint: str = "", part_type: str = ""):
        return type("Pred", (), {"trino_sql": "SELECT 1"})()


class _FakeOptimizer:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def compile(self, student, **kwargs):
        assert isinstance(student, VerticaToTrinoProgram)
        print("[FAKE] optimizer compile stdout")
        logging.getLogger("dspy.teleprompt.mipro_optimizer_v2").info("fake proposer log line")
        return _FakeCompiledModule()


class _FakeMetric:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def flush_validation_summary(self):
        trace_path = self.kwargs.get("validation_trace_path")
        summary_path = self.kwargs.get("validation_summary_path")
        if trace_path:
            Path(trace_path).write_text("", encoding="utf-8")
        if summary_path:
            Path(summary_path).write_text(
                json.dumps({"validation_examples": 0, "examples": []}),
                encoding="utf-8",
            )


def test_translate_signature_keeps_repair_guidance_out_of_training_prompt():
    signature_doc = inspect.getdoc(VerticaToTrino) or ""

    assert "Repair Mode" not in signature_doc
    assert "current_trino" not in signature_doc
    assert "allowed_change_scope" not in signature_doc
    assert "Repair Mode" in REPAIR_MODE_GUIDANCE


def test_compile_module_writes_compile_log(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "checkpoint_path", tmp_path / "checkpoint" / "compiled_module.json")
    compiler = DSPyCompiler()

    examples = [
        dspy.Example(
            vertica_sql=f"select {idx}",
            trino_sql=f"SELECT {idx}",
            context_hint="",
            part_type="query",
        ).with_inputs("vertica_sql", "context_hint", "part_type")
        for idx in range(5)
    ]

    compiler.dataset_loader.load = lambda: examples
    compiler.dataset_loader.get_hash = lambda: "deadbeef"
    monkeypatch.setattr(compiler, "_configure_lm", lambda: object())
    monkeypatch.setattr("dspy_modules.compiler.SQLTranslationMetric", _FakeMetric)
    monkeypatch.setattr("dspy_modules.compiler.MIPROv2", _FakeOptimizer)

    compiler.compile_module(
        num_trials=1,
        num_candidates=1,
        max_bootstrapped_demos=0,
        max_labeled_demos=1,
        minibatch_size=1,
        force=True,
    )

    log_files = sorted((tmp_path / "checkpoint" / "logs").glob("compile_*.log"))
    trace_files = sorted((tmp_path / "checkpoint" / "logs").glob("*_validation_metrics.jsonl"))
    summary_files = sorted((tmp_path / "checkpoint" / "logs").glob("*_validation_summary.json"))
    assert log_files
    assert trace_files
    assert summary_files

    log_text = log_files[-1].read_text(encoding="utf-8")
    metadata = json.loads((tmp_path / "checkpoint" / "compiled_module.json").read_text(encoding="utf-8"))

    assert "[DIAGNOSTICS] Compile context:" in log_text
    assert "student_type: VerticaToTrinoProgram" in log_text
    assert "[FAKE] optimizer compile stdout" in log_text
    assert "fake proposer log line" in log_text
    assert metadata["compile_log_path"] == str(log_files[-1])
    assert metadata["validation_trace_path"] == str(trace_files[-1])
    assert metadata["validation_summary_path"] == str(summary_files[-1])


def test_metric_writes_validation_summary(tmp_path):
    from dspy_modules.compiler import SQLTranslationMetric

    metric = SQLTranslationMetric(
        judge_lm=None,
        validation_trace_path=tmp_path / "validation_metrics.jsonl",
        validation_summary_path=tmp_path / "validation_summary.json",
    )
    metric._llm_judge_score = lambda example, pred: 0.0

    example = dspy.Example(
        example_id="gd_test",
        vertica_sql="SELECT 1",
        trino_sql="SELECT DATE '2024-01-01'",
        context_hint="",
        part_type="functions_date",
        dataset_split="val",
        dataset_index=80,
    ).with_inputs("vertica_sql", "context_hint", "part_type")
    pred = dspy.Prediction(trino_sql="SELECT '2024-01-01'")

    score = metric(example, pred, trace=None)
    metric.flush_validation_summary()

    assert score > 0.0
    trace_lines = (tmp_path / "validation_metrics.jsonl").read_text(encoding="utf-8").splitlines()
    summary = json.loads((tmp_path / "validation_summary.json").read_text(encoding="utf-8"))

    assert len(trace_lines) == 1
    event = json.loads(trace_lines[0])
    assert event["example_id"] == "gd_test"
    assert event["zero_score"] is False
    assert event["validity_failed"] is True
    assert "unwrapped_date_literal" in event["pattern_ids"]
    assert summary["validation_examples"] == 1
    assert summary["total_zero_scores"] == 0
    assert summary["examples"][0]["example_id"] == "gd_test"


def test_metric_does_not_flag_casted_parameter_or_casted_date_literal(tmp_path):
    from dspy_modules.compiler import SQLTranslationMetric

    metric = SQLTranslationMetric(
        judge_lm=None,
        validation_trace_path=tmp_path / "validation_metrics.jsonl",
        validation_summary_path=tmp_path / "validation_summary.json",
    )

    sql = (
        "SELECT CAST(:actual_date AS DATE) AS actual_date, "
        "CAST(:launch_id AS BIGINT) AS launch_id, "
        "CAST('2099-01-01' AS DATE) AS upper_bound"
    )

    is_clean, patterns = metric._check_vertica_patterns(sql)

    assert is_clean is True
    assert "uncasted_parameter" not in "".join(patterns)
    assert "unwrapped_date_literal" not in "".join(patterns)
