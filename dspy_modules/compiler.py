"""
Компилятор DSPy модулей с использованием MIPROv2.
Однопроходная компиляция с Graceful Shutdown для DSPy 2.6.27+.
# Полный прогон (24 часа)
python -m dspy_modules.compiler -n 45 -c 14 --bootstrapped-demos 8 --labeled-demos 7 --minibatch-size 8 --force
python -m dspy_modules.compiler -n 1 -c 1
"""

import json
import hashlib
import io
import logging
import os
import signal
import sys
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any, List, Optional
from datetime import datetime
import re 

import dspy
from dspy.teleprompt import MIPROv2

from config import settings
from core.llm_profiles import ensure_no_proxy_for_llm
from dspy_modules.signature import SQLJudge, VerticaToTrinoProgram


class GoldenDatasetLoader:
    """Загрузчик golden dataset с валидацией формата."""
    
    def __init__(self, dataset_path: Optional[Path] = None):
        self.dataset_path = dataset_path or settings.golden_dataset_path / "examples.json"
    
    def load(self) -> List[dspy.Example]:
        """Загружает и валидирует датасет."""
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Golden dataset not found: {self.dataset_path}")
        
        with open(self.dataset_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        examples = []
        for item in data:
            if not all(k in item for k in ["vertica", "trino"]):
                print(f"[WARNING] Skipping item {item.get('id', 'unknown')}: missing vertica/trino")
                continue
            
            ex = dspy.Example(
                example_id=item.get("id", "unknown"),
                vertica_sql=item["vertica"],
                trino_sql=item["trino"],
                context_hint=item.get("context_hint", ""),
                part_type=(item.get("metadata") or {}).get("category", item.get("part_type", ""))
            ).with_inputs("vertica_sql", "context_hint", "part_type")
            
            examples.append(ex)
        
        print(f"[INFO] Loaded {len(examples)} valid examples from golden dataset")
        return examples
    
    def get_hash(self) -> str:
        """Возвращает хеш датасета для версионирования."""
        if not self.dataset_path.exists():
            return ""
        with open(self.dataset_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()[:8]


class SQLTranslationMetric:
    """Комбинированная метрика: 20% validity + 20% exact + 60% LLM judge.
    
    Паттерны Vertica загружаются из golden_dataset/forbidden_patterns.json.
    Поддерживаются substring и regex типы паттернов.
    """
    
    def __init__(
        self, 
        judge_lm: Optional[dspy.LM] = None,
        patterns_path: Optional[Path] = None,
        validation_trace_path: Optional[Path] = None,
        validation_summary_path: Optional[Path] = None,
    ):
        self.judge_lm = judge_lm
        self.patterns_path = patterns_path or (
            settings.golden_dataset_path / "forbidden_patterns.json"
        )
        self.validation_trace_path = validation_trace_path
        self.validation_summary_path = validation_summary_path
        self._compiled_patterns: list[tuple[str, re.Pattern, str, str]] = []
        self._validation_stats: dict[str, dict[str, Any]] = {}
        self._validation_pattern_counts: Counter[str] = Counter()
        self._load_and_compile_patterns()

    def _load_and_compile_patterns(self):
        """Загружает и компилирует паттерны из JSON."""
        if not self.patterns_path.exists():
            raise FileNotFoundError(
                f"Forbidden patterns file not found: {self.patterns_path}"
            )
        
        with open(self.patterns_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for item in data.get("patterns", []):
            pattern_id = item.get("id", "unknown")
            pattern_str = item.get("pattern", "")
            pattern_type = item.get("type", "substring")
            description = item.get("description", "")
            severity = item.get("severity", "error")
            
            if not pattern_str:
                continue
            
            try:
                if pattern_type == "regex":
                    # Компилируем regex с флагом IGNORECASE для SQL
                    compiled = re.compile(pattern_str, re.IGNORECASE)
                else:
                    # Для substring используем re.escape и ищем как подстроку
                    compiled = re.compile(re.escape(pattern_str), re.IGNORECASE)
                
                self._compiled_patterns.append(
                    (pattern_id, compiled, description, severity)
                )
            except re.error as e:
                print(f"[WARNING] Failed to compile pattern '{pattern_id}': {e}")
                continue
        
        print(f"[INFO] Loaded {len(self._compiled_patterns)} forbidden patterns")        
    
    def _check_sql_validity(self, sql: str) -> tuple[float, list]:
        """Базовая проверка валидности SQL (0.0 или 1.0) + список ошибок."""
        errors = []
        
        if not sql or not isinstance(sql, str):
            return 0.0, ["Empty or non-string output"]
        
        sql_clean = sql.strip().upper()
        if not sql_clean:
            return 0.0, ["Empty SQL"]
        
        # Проверка наличия DML
        has_dml = any(kw in sql_clean for kw in ["SELECT", "INSERT", "CREATE", "WITH", "UPDATE", "DELETE"])
        if not has_dml:
            errors.append("No DML keywords found")
        
        # Проверка баланса скобок
        balanced_parens = sql.count("(") == sql.count(")")
        if not balanced_parens:
            errors.append("Unbalanced parentheses")
        
        validity = 1.0 if (has_dml and balanced_parens) else 0.0
        return validity, errors
    
    def _check_vertica_patterns(self, sql: str) -> tuple[bool, list]:
        """
        Проверяет наличие Vertica-специфичных паттернов с использованием
        скомпилированных regex.
        
        Returns:
            (is_clean, list_of_found_patterns)
        """
        if not sql:
            return True, []
        
        found = []
        
        for pattern_id, compiled, description, severity in self._compiled_patterns:
            if compiled.search(sql): 
                found.append(f"{pattern_id}: {description}")
        
        return len(found) == 0, found
    
    def _exact_match_score(self, pred: str, gold: str) -> float:
        """
        Jaccard similarity между множествами токенов SQL.
        Устойчив к перестановке колонок, лишним пробелам и регистру.
        """
        def tokenize(sql: str) -> set:
            """Токенизация SQL на ключевые слова, идентификаторы и операторы."""
            if not sql:
                return set()
            
            # Нормализация
            sql = sql.lower().strip()
            
            # Отделяем операторы и пунктуацию пробелами для корректного сплита
            operators = ['=', '<>', '!=', '<=', '>=', '<', '>', '+', '-', '*', '/', ',', ';', '(', ')', '.']
            for op in operators:
                sql = sql.replace(op, f' {op} ')
            
            # Разбиваем на токены и фильтруем пустые
            tokens = [t for t in sql.split() if t]
            return set(tokens)
        
        pred_tokens = tokenize(pred)
        gold_tokens = tokenize(gold)
        
        if not gold_tokens:
            return 1.0 if not pred_tokens else 0.0
        
        # Jaccard similarity: |A ∩ B| / |A ∪ B|
        intersection = len(pred_tokens & gold_tokens)
        union = len(pred_tokens | gold_tokens)
        
        if union == 0:
            return 0.0
            
        similarity = intersection / union
        
        # если полное совпадение множеств, но разный порядок - округляем до 1.0
        if similarity > 0.95:
            similarity = 1.0
            
        return similarity
    
    def _llm_judge_score(self, example: dspy.Example, pred: dspy.Prediction) -> float:
        """LLM-as-Judge оценка."""
        try:
            judge = dspy.Predict(SQLJudge)
            with dspy.context(lm=self.judge_lm or dspy.settings.lm, adapter=dspy.ChatAdapter()):
                result = judge(
                    vertica_sql=example.vertica_sql,
                    trino_sql=pred.trino_sql,
                    reference_trino=example.trino_sql
                )

                try:
                    logic_issues_list = json.loads(result.logic_issues) if result.logic_issues else []
                except json.JSONDecodeError:
                    logic_issues_list = [result.logic_issues] if result.logic_issues else []
                
                return float(result.score)
        except Exception as e:
            print(f"[WARNING] LLM Judge failed: {e}")
            return 0.0

    def _record_validation_event(
        self,
        example: dspy.Example,
        pred_sql: str,
        validity: float,
        exact: float,
        llm_score: float,
        total: float,
        base_errors: list[str],
        vertica_patterns: list[str],
    ):
        """Пишет raw-события и агрегирует summary для validation examples."""
        if getattr(example, "dataset_split", "") != "val":
            return

        example_id = getattr(example, "example_id", "unknown")
        pattern_ids = []
        for pattern in vertica_patterns:
            pattern_id, _, *_ = pattern.partition(":")
            pattern_ids.append(pattern_id.strip())

        event = {
            "example_id": example_id,
            "dataset_split": "val",
            "dataset_index": getattr(example, "dataset_index", None),
            "part_type": getattr(example, "part_type", ""),
            "score": round(total, 6),
            "validity": round(validity, 6),
            "exact": round(exact, 6),
            "llm_score": round(llm_score, 6),
            "failed": total < 1.0,
            "zero_score": total == 0.0,
            "validity_failed": validity == 0.0,
            "pattern_ids": pattern_ids,
            "patterns": vertica_patterns,
            "base_errors": base_errors,
            "generated_sql_preview": pred_sql[:400],
        }

        if self.validation_trace_path:
            with self.validation_trace_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, ensure_ascii=False) + "\n")

        stats = self._validation_stats.setdefault(
            example_id,
            {
                "example_id": example_id,
                "dataset_index": getattr(example, "dataset_index", None),
                "part_type": getattr(example, "part_type", ""),
                "evaluations": 0,
                "failures": 0,
                "zero_scores": 0,
                "validity_failures": 0,
                "score_sum": 0.0,
                "best_score": 0.0,
                "worst_score": 1.0,
                "pattern_counts": Counter(),
                "base_errors": Counter(),
            },
        )
        stats["evaluations"] += 1
        stats["score_sum"] += total
        stats["best_score"] = max(stats["best_score"], total)
        stats["worst_score"] = min(stats["worst_score"], total)
        if event["failed"]:
            stats["failures"] += 1
        if event["zero_score"]:
            stats["zero_scores"] += 1
        if event["validity_failed"]:
            stats["validity_failures"] += 1
        stats["pattern_counts"].update(pattern_ids)
        stats["base_errors"].update(base_errors)
        self._validation_pattern_counts.update(pattern_ids)

    def flush_validation_summary(self):
        """Сохраняет summary по validation examples."""
        if not self.validation_summary_path:
            return

        examples = []
        for stats in sorted(
            self._validation_stats.values(),
            key=lambda item: (
                -item["failures"],
                -item["zero_scores"],
                item["dataset_index"] if item["dataset_index"] is not None else 10**9,
                item["example_id"],
            ),
        ):
            evaluations = max(1, stats["evaluations"])
            examples.append(
                {
                    "example_id": stats["example_id"],
                    "dataset_index": stats["dataset_index"],
                    "part_type": stats["part_type"],
                    "evaluations": stats["evaluations"],
                    "failures": stats["failures"],
                    "zero_scores": stats["zero_scores"],
                    "validity_failures": stats["validity_failures"],
                    "avg_score": round(stats["score_sum"] / evaluations, 6),
                    "best_score": round(stats["best_score"], 6),
                    "worst_score": round(stats["worst_score"], 6),
                    "pattern_counts": dict(stats["pattern_counts"].most_common()),
                    "base_errors": dict(stats["base_errors"].most_common()),
                }
            )

        payload = {
            "validation_examples": len(self._validation_stats),
            "total_evaluations": sum(item["evaluations"] for item in self._validation_stats.values()),
            "total_failures": sum(item["failures"] for item in self._validation_stats.values()),
            "total_zero_scores": sum(item["zero_scores"] for item in self._validation_stats.values()),
            "pattern_counts": dict(self._validation_pattern_counts.most_common()),
            "examples": examples,
        }

        with self.validation_summary_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
    
    def __call__(self, example: dspy.Example, pred: dspy.Prediction, trace=None) -> float:
        """
        Вычисляет комбинированную метрику.
        
        Returns:
            float: Score от 0.0 до 1.0
        """
        # 1. Базовая SQL Validity
        base_validity, base_errors = self._check_sql_validity(pred.trino_sql)
        
        # 2. Проверка на Vertica-паттерны (критично!)
        is_clean, vertica_patterns = self._check_vertica_patterns(pred.trino_sql)
        
        # Финальная validity
        if not is_clean:
            validity = 0.0
            if trace is not None:
                print(f"[METRIC] VERTICA PATTERNS DETECTED: {'; '.join(vertica_patterns)}")
        else:
            validity = base_validity
        
        if trace is not None and base_errors and validity == 0.0:
            print(f"[METRIC] Base errors: {'; '.join(base_errors)}")
        
        # 3. Exact Match. При validity=0 не обнуляем сигнал полностью,
        # а используем мягкий штраф, чтобы bootstrap различал "почти хорошо"
        # и "совсем плохо".
        exact_raw = self._exact_match_score(pred.trino_sql, example.trino_sql)
        
        # 4. LLM Judge. Для SQL с forbidden patterns оставляем половинный вес,
        # но для явно битого SQL без базовой валидности не тратим лишний вызов judge.
        if base_validity > 0:
            llm_raw = self._llm_judge_score(example, pred)
        else:
            llm_raw = 0.0
        
        if validity == 0.0:
            exact = exact_raw * 0.5
            llm_score = llm_raw * 0.5
        else:
            exact = exact_raw
            llm_score = llm_raw
        
        total = (0.2 * validity) + (0.2 * exact) + (0.6 * llm_score)
        self._record_validation_event(
            example=example,
            pred_sql=pred.trino_sql,
            validity=validity,
            exact=exact,
            llm_score=llm_score,
            total=total,
            base_errors=base_errors,
            vertica_patterns=vertica_patterns,
        )
        
        if trace is not None:
            if validity == 0.0 and (exact_raw > 0 or llm_raw > 0):
                print(
                    f"[METRIC] Soft penalty applied: validity=0 -> "
                    f"Exact {exact_raw:.2f}->{exact:.2f}, LLM {llm_raw:.2f}->{llm_score:.2f}"
                )
            print(f"[METRIC] Validity: {validity:.1f}, Exact: {exact:.1f}, LLM: {llm_score:.2f} -> Total: {total:.2f}")
        
        return total


class _TeeStream(io.TextIOBase):
    """Пишет одновременно в консоль и файл."""

    def __init__(self, *streams: io.TextIOBase):
        self._streams = streams

    def write(self, data: str) -> int:
        for stream in self._streams:
            stream.write(data)
            stream.flush()
        return len(data)

    def flush(self):
        for stream in self._streams:
            stream.flush()

    def isatty(self) -> bool:
        return any(getattr(stream, "isatty", lambda: False)() for stream in self._streams)


class DSPyCompiler:
    """
    Компилятор DSPy модулей. 
    Однопроходная оптимизация с Graceful Shutdown.
    """
    
    def __init__(self):
        self.dataset_loader = GoldenDatasetLoader(settings.golden_dataset_path / "examples.json")
        self.checkpoint_dir = settings.checkpoint_path.parent
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = self.checkpoint_dir / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.current_log_path: Optional[Path] = None
        self.current_validation_trace_path: Optional[Path] = None
        self.current_validation_summary_path: Optional[Path] = None
        self._interrupted = False
        self._setup_signal_handlers()
        
    def _setup_signal_handlers(self):
        """Устанавливает обработчики для Graceful Shutdown."""
        def handler(signum, frame):
            print(f"\n{'='*60}")
            print("[WARNING] Received interrupt signal!")
            print("[WARNING] MIPROv2 will complete current iteration,")
            print("          then save the best model found so far.")
            print(f"{'='*60}")
            self._interrupted = True
        
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
    
    def _configure_lm(self):
        """Конфигурирует языковую модель с ChatAdapter для совместимости с LM Studio."""
        ensure_no_proxy_for_llm(settings.llm_base_url)
        lm = dspy.LM(
            model=settings.llm_model,
            api_base=settings.llm_base_url,
            api_key=settings.llm_api_key,
            max_tokens=50000,
            temperature=0.1,
            cache=False
        )
        # ChatAdapter вместо JSONAdapter для совместимости с LM Studio
        dspy.configure(lm=lm, cache=False, adapter=dspy.ChatAdapter())
        return lm

    def _build_log_path(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.logs_dir / f"compile_{timestamp}_seed-none.log"

    def _build_validation_trace_path(self, log_path: Path) -> Path:
        return log_path.with_name(f"{log_path.stem}_validation_metrics.jsonl")

    def _build_validation_summary_path(self, log_path: Path) -> Path:
        return log_path.with_name(f"{log_path.stem}_validation_summary.json")

    @contextmanager
    def _compile_logging(self):
        """Дублирует stdout/stderr и logging в постоянный compile-log."""
        log_path = self._build_log_path()
        self.current_log_path = log_path
        self.current_validation_trace_path = self._build_validation_trace_path(log_path)
        self.current_validation_summary_path = self._build_validation_summary_path(log_path)
        root_logger = logging.getLogger()
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root_logger.addHandler(file_handler)
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        try:
            with log_path.open("a", encoding="utf-8") as log_fp:
                sys.stdout = _TeeStream(old_stdout, log_fp)
                sys.stderr = _TeeStream(old_stderr, log_fp)
                print(f"[INFO] Compile log: {log_path}")
                print(f"[INFO] Validation metric trace: {self.current_validation_trace_path}")
                print(f"[INFO] Validation metric summary: {self.current_validation_summary_path}")
                yield log_path
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            root_logger.removeHandler(file_handler)
            file_handler.close()

    def _print_compile_diagnostics(
        self,
        dataset_hash: str,
        trainset: list[dspy.Example],
        valset: list[dspy.Example],
        student: dspy.Module,
        actual_trials: int,
        actual_candidates: int,
        max_bootstrapped_demos: int,
        max_labeled_demos: int,
        actual_minibatch: int,
    ):
        print(f"\n{'='*60}")
        print("[CONFIG] MIPROv2 Parameters:")
        print(f"  num_trials: {actual_trials}")
        print(f"  num_candidates: {actual_candidates}")
        print(f"  max_bootstrapped_demos: {max_bootstrapped_demos}")
        print(f"  max_labeled_demos: {max_labeled_demos}")
        print(f"  minibatch_size: {actual_minibatch}")
        print(f"{'='*60}")
        print("[DIAGNOSTICS] Compile context:")
        print(f"  dataset_hash: {dataset_hash}")
        print(f"  train_examples: {len(trainset)}")
        print(f"  val_examples: {len(valset)}")
        print(f"  student_type: {type(student).__name__}")
        print(f"  student_repr: {student!r}")
        print(f"  compile_log_path: {self.current_log_path}")
        print(f"  validation_trace_path: {self.current_validation_trace_path}")
        print(f"  validation_summary_path: {self.current_validation_summary_path}")
        print(f"{'='*60}\n")

    def _save_checkpoint(self, module: dspy.Module, dataset_hash: str, status: str = "intermediate"):
        """Сохраняет чекпоинт модели."""
        if status == "intermediate":
            path = self.checkpoint_dir / f"intermediate_{datetime.now():%Y%m%d_%H%M%S}.pkl"
        else:
            path = settings.checkpoint_path.with_suffix('.pkl')
        
        module.save(str(path), save_program=False)
        
        metadata = {
            "version": "2.6",
            "type": status,
            "timestamp": datetime.now().isoformat(),
            "dataset_hash": dataset_hash,
            "model_name": settings.llm_model,
            "pkl_path": str(path),
            "interrupted": self._interrupted,
            "compile_log_path": str(self.current_log_path) if self.current_log_path else None,
            "validation_trace_path": str(self.current_validation_trace_path) if self.current_validation_trace_path else None,
            "validation_summary_path": str(self.current_validation_summary_path) if self.current_validation_summary_path else None,
        }
        
        meta_path = path.with_suffix('.json')
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"[INFO] Checkpoint saved: {path}")
        return path
    
    def _save_final(self, module: dspy.Module, dataset_hash: str):
        """Сохраняет финальный результат."""
        pkl_path = self._save_checkpoint(module, dataset_hash, status="final")
        
        # Пробуем сохранить полную программу (для DSPy 2.6.0+)
        full_dir = self.checkpoint_dir / "compiled_module_full"
        try:
            module.save(str(full_dir), save_program=True)
            print(f"[INFO] Full program saved to: {full_dir}")
        except Exception as e:
            print(f"[WARNING] Could not save full program: {e}")
        
        # JSON с метаданными
        metadata = {
            "version": "2.6",
            "type": "final",
            "timestamp": datetime.now().isoformat(),
            "dataset_hash": dataset_hash,
            "model_name": settings.llm_model,
            "pkl_path": str(pkl_path),
            "full_program_path": str(full_dir) if full_dir.exists() else None,
            "compile_log_path": str(self.current_log_path) if self.current_log_path else None,
            "validation_trace_path": str(self.current_validation_trace_path) if self.current_validation_trace_path else None,
            "validation_summary_path": str(self.current_validation_summary_path) if self.current_validation_summary_path else None,
        }
        
        with open(settings.checkpoint_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"[INFO] Final checkpoint: {settings.checkpoint_path}")
    
    def compile_module(
        self,
        num_trials: Optional[int] = 40,
        num_candidates: Optional[int] = 12,
        max_bootstrapped_demos: int = 8,
        max_labeled_demos: int = 7,
        minibatch_size: Optional[int] = 10,
        force: bool = False
    ) -> dspy.Module:
        """
        Однопроходная компиляция с полным контролем параметров.
        
        Args:
            num_trials: Количество итераций оптимизации (default: settings.mipro_max_iterations)
            num_candidates: Размер пула кандидатов (default: settings.mipro_num_candidates)
            max_bootstrapped_demos: Макс. bootstrapped примеров в few-shot
            max_labeled_demos: Макс. размеченных примеров
            minibatch_size: Размер минибатча для валидации
            force: Перезаписать существующий чекпоинт
        """
        
        # Проверяем существующий чекпоинт
        if not force and settings.checkpoint_path.exists():
            print(f"[INFO] Found existing checkpoint: {settings.checkpoint_path}")
            print("[INFO] Use --force to recompile or delete the file")
            return self.load_compiled_module()
        
        with self._compile_logging():
            # Загружаем датасет
            examples = self.dataset_loader.load()
            if len(examples) < 5:
                raise ValueError(f"Too few examples: {len(examples)}")
            
            split_idx = int(len(examples) * 0.8)
            trainset = examples[:split_idx]
            valset = examples[split_idx:]
            for idx, ex in enumerate(trainset):
                ex.dataset_split = "train"
                ex.dataset_index = idx
            for idx, ex in enumerate(valset):
                ex.dataset_split = "val"
                ex.dataset_index = split_idx + idx
            
            print(f"[INFO] Train: {len(trainset)}, Val: {len(valset)}")
            
            # Настройка LM
            lm = self._configure_lm()
            metric = SQLTranslationMetric(
                judge_lm=lm,
                validation_trace_path=self.current_validation_trace_path,
                validation_summary_path=self.current_validation_summary_path,
            )
            
            # Параметры из аргументов или конфига
            actual_trials = num_trials or settings.mipro_max_iterations
            actual_candidates = num_candidates or settings.mipro_num_candidates
            actual_minibatch = minibatch_size or max(1, len(valset) // 2)
            
            dataset_hash = self.dataset_loader.get_hash()
            student = VerticaToTrinoProgram()
            self._print_compile_diagnostics(
                dataset_hash=dataset_hash,
                trainset=trainset,
                valset=valset,
                student=student,
                actual_trials=actual_trials,
                actual_candidates=actual_candidates,
                max_bootstrapped_demos=max_bootstrapped_demos,
                max_labeled_demos=max_labeled_demos,
                actual_minibatch=actual_minibatch,
            )
            
            optimizer = MIPROv2(
                metric=metric,
                auto=None,  # Ручной режим - полный контроль параметров
                num_candidates=actual_candidates,
                init_temperature=1.0,
                verbose=True,
                num_threads=4
            )
            
            # Запускаем компиляцию
            try:
                compiled_module = optimizer.compile(
                    student=student,
                    trainset=trainset,
                    valset=valset,
                    num_trials=actual_trials,
                    max_bootstrapped_demos=max_bootstrapped_demos,
                    max_labeled_demos=max_labeled_demos,
                    minibatch_size=actual_minibatch,
                    minibatch_full_eval_steps=2
                )
                
                # Финальное сохранение
                self._save_final(compiled_module, dataset_hash)
                metric.flush_validation_summary()
                print(f"\n[SUCCESS] Compilation complete!")
                
                return compiled_module
                
            except KeyboardInterrupt:
                # Этот блок не сработает из-за signal handler, но оставим на всякий случай
                print("\n[WARNING] Interrupted by user")
                metric.flush_validation_summary()
                if 'compiled_module' in locals():
                    self._save_checkpoint(compiled_module, dataset_hash, "interrupted")
                raise
            except Exception as e:
                print(f"\n[ERROR] Compilation failed: {e}")
                metric.flush_validation_summary()
                # Пытаемся сохранить что есть
                if 'compiled_module' in locals():
                    self._save_checkpoint(compiled_module, dataset_hash, "error")
                raise
    
    def load_compiled_module(self) -> dspy.Module:
        """Загружает скомпилированный модуль."""
        self._configure_lm()
        # Сначала ищем .pkl
        pkl_path = settings.checkpoint_path.with_suffix('.pkl')
        
        if pkl_path.exists():
            print(f"[INFO] Loading from {pkl_path}")
            module = VerticaToTrinoProgram()
            module.load(str(pkl_path), allow_pickle=True)
            return module
        
        # Пробуем full program
        full_dir = self.checkpoint_dir / "compiled_module_full"
        if full_dir.exists():
            print(f"[INFO] Loading full program from {full_dir}")
            return dspy.load(str(full_dir))
        
        raise FileNotFoundError(
            f"No checkpoint found at {settings.checkpoint_path}. "
            f"Run: python -m dspy_modules.compiler"
        )


def main():
    """CLI entry point с полным контролем параметров."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Compile DSPy module with MIPROv2",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Основные параметры
    parser.add_argument(
        "--force", 
        action="store_true", 
        help="Force recompilation even if checkpoint exists"
    )
    parser.add_argument(
        "--dataset", 
        type=Path,
        default=None,
        help="Path to golden dataset JSON"
    )
    
    # Параметры MIPROv2 (переопределяют config.py)
    parser.add_argument(
        "-n", "--num-trials",
        type=int,
        default=None,
        help=f"Number of optimization trials (default: {settings.mipro_max_iterations})"
    )
    parser.add_argument(
        "-c", "--num-candidates",
        type=int,
        default=None,
        help=f"Number of candidates to generate (default: {settings.mipro_num_candidates})"
    )
    parser.add_argument(
        "--bootstrapped-demos",
        type=int,
        default=8,
        help="Max bootstrapped demonstrations in few-shot"
    )
    parser.add_argument(
        "--labeled-demos",
        type=int,
        default=12,
        help="Max labeled demonstrations"
    )
    parser.add_argument(
        "--minibatch-size",
        type=int,
        default=None,
        help="Minibatch size for validation (default: len(valset)//2)"
    )
    
    args = parser.parse_args()
    
    # Инициализация директорий
    settings.ensure_dirs()
    
    compiler = DSPyCompiler()
    if args.dataset:
        compiler.dataset_loader = GoldenDatasetLoader(args.dataset)
    
    try:
        module = compiler.compile_module(
            num_trials=args.num_trials,
            num_candidates=args.num_candidates,
            max_bootstrapped_demos=args.bootstrapped_demos,
            max_labeled_demos=args.labeled_demos,
            minibatch_size=args.minibatch_size,
            force=args.force
        )
        
        # Тест
        print("\n[Test] Running inference...")
        result = module(
            vertica_sql="SELECT * FROM test_table WHERE date = '2024-01-01'"
        )
        print(f"[Test] Output: {result.trino_sql[:100]}...")
        
    except Exception as e:
        print(f"[FATAL] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
