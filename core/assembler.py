"""Модуль сборки финального SQL файла."""

import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from config import settings
from core.state_manager import StateManager


class AssemblerError(Exception):
    """Исключение для ошибок сборки."""
    pass


class Assembler:
    """Сборщик финального SQL файла из последних версий частей."""
    
    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager
        self.query_name = state_manager.query_name
        self.work_dir = state_manager.work_dir
        
    def assemble_final(self) -> Path:
        """
        Собирает финальный SQL файл из всех частей.
        
        Берет последние доступные версии файлов частей.
        Сохраняет локальный assembled-артефакт в work_dir/final/{query_name}_final.sql.
        Перенос в done/review выполняется только на этапе finalize_workflow().
        
        Returns:
            Путь к финальному файлу
        """
        print(f"[Assembler] [{self.query_name}] Assembling final SQL file...")
        
        # Получаем список всех частей
        total_parts = self._get_total_parts()
        if total_parts == 0:
            raise AssemblerError("No parts found for assembly")
        
        # Собираем содержимое всех частей
        assembled_parts = []
        state = self.state_manager.load_state() or {}
        parts_map = state.get("parts_map", {})
        part_num_width = max(2, len(str(max(total_parts - 1, 0))))
        
        for part_num in range(total_parts):
            part_content = self._get_latest_version_content(part_num)
            if part_content is None:
                raise AssemblerError(f"Part {part_num} not found")

            if self._is_noop_part(part_content):
                print(f"[Assembler] Skipping no-op part {part_num}")
                continue
            
            # Гарантируем что часть заканчивается на ;
            part_content = self._ensure_semicolon(part_content)
            part_metadata = parts_map.get(f"part_{part_num}", {})
            part_boundary = self._generate_part_boundary(
                part_num=part_num,
                part_num_width=part_num_width,
                part_metadata=part_metadata,
            )
            assembled_parts.append(f"{part_boundary}\n\n{part_content}")
        
        # Объединяем части
        final_sql = "\n\n".join(assembled_parts)
        
        # Добавляем header с метаданными
        header = self._generate_file_header()
        final_sql = header + "\n\n" + final_sql
        
        # Сохраняем
        output_path = self.work_dir / "final" / f"{self.query_name}_final.sql"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        temp_path = output_path.with_suffix('.tmp')
        temp_path.write_text(final_sql, encoding='utf-8')
        temp_path.replace(output_path)
        
        print(f"[Assembler] Final SQL saved: {output_path}")
        return output_path

    @staticmethod
    def _is_noop_part(sql: str) -> bool:
        """True for legacy parts left after removing analyze_statistics/drop_partitions."""
        without_comments = re.sub(r'--.*?$', ' ', sql, flags=re.MULTILINE)
        without_comments = re.sub(r'/\*.*?\*/', ' ', without_comments, flags=re.DOTALL)
        return not without_comments.strip().strip(';').strip()
    
    def _ensure_semicolon(self, sql: str) -> str:
        """
        Гарантирует что SQL заканчивается на ';'.
        Убирает дублирование если их несколько.
        Учитывает комментарии в конце строки.
        """
        sql = sql.rstrip()
        
        if not sql:
            return sql
        
        # Проверяем заканчивается ли на ;
        if sql.endswith(';'):
            # Убираем лишние ; в конце (;; -> ;)
            while sql.endswith(';'):
                sql = sql[:-1]
            sql = sql.rstrip() + ';'
            return sql
        
        # Проверяем есть ли комментарий в конце последней строки
        lines = sql.split('\n')
        last_line = lines[-1]
        
        # Однострочный комментарий -- 
        comment_match = re.match(r'(.*?)(\s*--.*)$', last_line)
        if comment_match:
            code_part = comment_match.group(1).rstrip()
            comment_part = comment_match.group(2)
            if not code_part.endswith(';'):
                lines[-1] = code_part + ';' + comment_part
                return '\n'.join(lines)
            return sql
        
        # Многострочный комментарий /* */ в конце
        # Ищем /* без закрывающего */ в последней строке (маловероятно, но возможно)
        if '/*' in last_line and '*/' not in last_line:
            # Сложный случай - комментарий занимает несколько строк
            # Добавляем ; перед началом комментария
            sql = sql.rstrip() + ';'
            return sql
        
        # Просто добавляем ; в конец
        return sql + ';'
    
    def finalize_workflow(self, move_to: str = "done") -> None:
        """
        Перемещает файлы в финальные директории (done или review).
        
        Args:
            move_to: "done" или "review"
        """
        if move_to not in ("done", "review"):
            raise ValueError(f"Invalid destination: {move_to}")
        
        base_path = settings.done_path if move_to == "done" else settings.review_path
        
        # Копируем оригинал Vertica
        vertica_src = settings.in_queue_path / f"{self.query_name}.sql"
        vertica_dst = base_path / "vertica" / f"{self.query_name}.sql"
        if vertica_src.exists():
            vertica_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(vertica_src, vertica_dst)
        
        # Копируем финальный Trino (если есть)
        trino_src = self.work_dir / "final" / f"{self.query_name}_final.sql"
        if not trino_src.exists():
            # Пробуем собрать если нет
            try:
                trino_src = self.assemble_final()
            except:
                pass
        
        if trino_src and trino_src.exists():
            trino_dst = base_path / "trino" / f"{self.query_name}_trino.sql"
            trino_dst.parent.mkdir(parents=True, exist_ok=True)
            if trino_src.resolve() != trino_dst.resolve():
                shutil.copy2(trino_src, trino_dst)
            else:
                print(f"   (файл уже на месте, пропускаем копирование)")
        
        # Обновляем статус
        self.state_manager.mark_final_status(
            "completed" if move_to == "done" else "review",
            None if move_to == "done" else "Moved to review"
        )
        
        print(f"[Assembler] Workflow finalized: moved to {move_to}")
    
    def _get_total_parts(self) -> int:
        """Возвращает общее количество частей из state."""
        state = self.state_manager.load_state()
        if not state:
            return 0
        return state.get("total_parts", 0)
    
    def _get_latest_version_content(self, part_num: int) -> Optional[str]:
        """Возвращает содержимое последней доступной версии части."""
        path = self.state_manager.get_latest_version_path(part_num)
        if path is None:
            return None
        return path.read_text(encoding='utf-8')
    
    def _generate_file_header(self) -> str:
        """Генерирует header для финального файла."""
        timestamp = datetime.now().isoformat()
        return f"""-- ==========================================
-- Auto-generated Trino SQL
-- Original: {self.query_name}.sql
-- Generated: {timestamp}
-- Tool: Ver2Tri Migration Agent
-- =========================================="""

    def _generate_part_boundary(
        self,
        part_num: int,
        part_num_width: int,
        part_metadata: Optional[Dict[str, object]] = None,
    ) -> str:
        """Генерирует компактный разделитель части с номером и именем таблицы."""
        part_metadata = part_metadata or {}
        table_name = part_metadata.get("table_name")
        part_label = f"Part {part_num:0{part_num_width}d}"
        if isinstance(table_name, str) and table_name.strip():
            part_label = f"{part_label} {table_name.strip()}"

        border = "-- ================"
        return f"{border}\n-- {part_label}\n{border}"
