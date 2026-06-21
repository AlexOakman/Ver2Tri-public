"""
Модуль разбиения SQL Vertica на атомарные части с использованием sqlparse.
Упрощенная версия: sqlparse.parse() делает всю работу по разбиению.
"""

import re
import time
from typing import Any, Dict, List, Optional, Set
from datetime import datetime

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Parenthesis, Token
from sqlparse.tokens import DML, Keyword

from core.state_manager import StateManager


class SplitError(Exception):
    """Исключение для ошибок разбиения."""
    def __init__(self, message: str, error_type: str = "split"):
        super().__init__(message)
        self.error_type = error_type


class SQLSplitter:
    """
    Разбивает монолитный SQL-файл Vertica на части для последовательного перевода.
    
    Стратегия: sqlparse.parse() разбивает на операторы, каждый оператор = отдельная часть.
    Фильтруем analyze_statistics и drop_partitions.
    """
    
    # Функции для пропуска
    _SKIP_PATTERNS = ('analyze_statistics(', 'drop_partitions(')
    
    # Для анализа INTERPOLATE - улучшенные паттерны как в старой версии
    _INTERPOLATE_PATTERN = re.compile(
        r'INTERPOLATE\s+(?:PREVIOUS|NEXT)\s+VALUE', 
        re.IGNORECASE
    )
    
    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager
        self.query_name = state_manager.query_name
        self.work_dir = state_manager.work_dir
        self.vertica_parts_path = state_manager.vertica_parts_path
        
    def should_skip(self) -> bool:
        """Проверяет, были ли уже созданы части."""
        if not self.vertica_parts_path.exists():
            return False
            
        existing_parts = list(self.vertica_parts_path.glob(f"{self.query_name}_part_*.sql"))
        if not existing_parts:
            return False
            
        state = self.state_manager.load_state()
        if state and state.get("split_completed"):
            print(f"[{self.query_name}] Parts already exist, skipping split")
            return True
            
        return False
    
    def _should_skip_statement(self, stmt_str: str) -> bool:
        """Проверяет, нужно ли пропустить оператор."""
        lower_str = stmt_str.lower()
        if any(pattern in lower_str for pattern in self._SKIP_PATTERNS):
            return True

        return self._is_noop_statement(stmt_str)

    @staticmethod
    def _is_noop_statement(stmt_str: str) -> bool:
        """Возвращает True для пустых операторов, которые не должны становиться part."""
        without_comments = re.sub(r'--.*?$', ' ', stmt_str, flags=re.MULTILINE)
        without_comments = re.sub(r'/\*.*?\*/', ' ', without_comments, flags=re.DOTALL)
        return not without_comments.strip().strip(';').strip()
    
    def _get_table_name(self, stmt_str: str) -> Optional[str]:
        """Извлекает имя создаваемой таблицы из CREATE TABLE."""
        match = re.search(
            r'CREATE\s+(?:LOCAL\s+)?(?:TEMP\s+)?(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?((?:\w+\.)?\w+)',
            stmt_str,
            re.IGNORECASE
        )
        return match.group(1) if match else None
    
    def _analyze_dependencies(self, stmt_str: str) -> Dict[str, List[str]]:
        """Анализирует создание и использование таблиц."""
        creates = []
        uses = []
        
        created = self._get_table_name(stmt_str)
        if created:
            creates.append(created)
        
        # Используем sqlparse для точного извлечения таблиц
        try:
            uses = self._extract_tables_from_sql(stmt_str)
        except Exception as e:
            # Fallback на старый метод при ошибке парсинга
            print(f"[WARNING] sqlparse failed for dependency analysis: {e}, using fallback")
            patterns = [
                (r'FROM\s+((?:\w+\.)?\w+)', uses),
                (r'JOIN\s+((?:\w+\.)?\w+)', uses),
                (r'INTO\s+((?:\w+\.)?\w+)', uses),
                (r'UPDATE\s+((?:\w+\.)?\w+)', uses),
            ]
            
            for pattern, target_list in patterns:
                for match in re.finditer(pattern, stmt_str, re.IGNORECASE):
                    table = match.group(1)
                    if table.upper() not in ('SELECT',):
                        target_list.append(table)
        
        creates = list(dict.fromkeys(creates))
        uses = list(dict.fromkeys(uses))
        
        return {"creates": creates, "uses": uses, "external_deps": []}
    
    def split(self, sql_content: str) -> List[Dict[str, Any]]:
        """
        Разбивает SQL на части. Каждый оператор sqlparse = отдельная часть.
        """
        start_time = time.time()
        print(f"[{self.query_name}] Starting SQL split with sqlparse...")

        if not sql_content or not sql_content.strip():
            raise SplitError("SQL content is empty", "validation")

        parts = []
        part_num = 0

        for stmt in sqlparse.parse(sql_content):
            if stmt.is_whitespace or not str(stmt).strip():
                continue
            
            stmt_str = str(stmt).strip()

            if self._should_skip_statement(stmt_str):
                print(f"[{self.query_name}] Skipping: {stmt_str[:60]}...")
                continue
            
            table_name = self._get_table_name(stmt_str)

            upper_str = stmt_str.upper()

            if '@HEADER' in upper_str:
                part_type = "header"
            elif 'CREATE' in upper_str and ('TEMP' in upper_str or 'TEMPORARY' in upper_str):
                part_type = "temp_table"
            elif 'INSERT' in upper_str and 'sandbox' in upper_str:
                part_type = "dml_block"
            else:
                part_type = "other"

            deps = self._analyze_dependencies(stmt_str)

            parts.append({
                "part_num": part_num,
                "part_type": part_type,
                "content": stmt_str,
                "table_name": table_name,
                "dependencies": deps,
                "interpolate_sources": [],
                "interpolate_consumers": []
            })

            print(f"[{self.query_name}] Part {part_num}: {part_type} {table_name or ''}")
            part_num += 1

        self._resolve_cross_part_dependencies(parts)
        self._detect_interpolate_relationships(parts)

        elapsed = time.time() - start_time
        print(f"[{self.query_name}] Split completed: {len(parts)} parts in {elapsed:.2f}s")

        return parts
    
    def _resolve_cross_part_dependencies(self, parts: List[Dict]) -> None:
        """Определяет зависимости между частями."""
        table_to_part = {}
        for part in parts:
            for table in part["dependencies"]["creates"]:
                table_to_part[table.upper()] = part["part_num"]
        
        for part in parts:
            external = []
            for used in part["dependencies"]["uses"]:
                creator = table_to_part.get(used.upper())
                if creator is not None and creator < part["part_num"]:
                    external.append({
                        "table": used,
                        "created_in_part": creator
                    })
            part["dependencies"]["external_deps"] = external

    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """
        Извлекает имена таблиц из SQL запроса используя sqlparse.
        Обрабатывает: обычные таблицы, CTE, подзапросы, алиасы, кавычки.
        
        Returns:
            Список уникальных имен таблиц в формате 'schema.table' или 'table'
        """
        tables = set()
        cte_names = set()  # Имена CTE (их не считаем внешними таблицами)
        
        try:
            parsed = sqlparse.parse(sql)
            for stmt in parsed:
                if not stmt.tokens:
                    continue
                
                # Сначала собираем имена CTE (они определены в WITH)
                self._collect_cte_names(stmt, cte_names)
                
                # Затем собираем все таблицы из FROM и JOIN
                self._collect_tables_recursive(stmt, tables, cte_names, in_from_clause=False)
                
        except Exception as e:
            print(f"[WARNING] Error parsing SQL for table extraction: {e}")
            return list(tables)
        
        return list(tables)
    
    def _collect_cte_names(self, token: Token, cte_names: Set[str]) -> None:
        """Собирает имена CTE из WITH clause."""
        if not hasattr(token, 'tokens'):
            return
        
        # Ищем WITH keyword
        is_with_clause = False
        for sub_token in token.tokens:
            if sub_token.is_keyword and sub_token.normalized == 'WITH':
                is_with_clause = True
                continue
            
            if is_with_clause:
                # После WITH идет Identifier или IdentifierList (CTE definitions)
                if isinstance(sub_token, Identifier):
                    # Обычный CTE: WITH cte_name AS (...)
                    cte_name = self._get_identifier_name(sub_token)
                    if cte_name:
                        cte_names.add(cte_name.upper())
                elif isinstance(sub_token, IdentifierList):
                    # Несколько CTE: WITH cte1 AS (...), cte2 AS (...)
                    for ident in sub_token.get_identifiers():
                        if isinstance(ident, Identifier):
                            cte_name = self._get_identifier_name(ident)
                            if cte_name:
                                cte_names.add(cte_name.upper())
                elif sub_token.ttype in (Keyword, DML) and sub_token.normalized != 'AS':
                    # Если встретили другой ключевой токен (SELECT, INSERT и т.д.), 
                    # то WITH clause закончился
                    if sub_token.normalized in ('SELECT', 'INSERT', 'UPDATE', 'DELETE'):
                        is_with_clause = False
        
        # Рекурсивно проверяем вложенные токены
        if hasattr(token, 'tokens'):
            for sub_token in token.tokens:
                self._collect_cte_names(sub_token, cte_names)
    
    def _collect_tables_recursive(self, token: Token, tables: Set[str], 
                                 cte_names: Set[str], in_from_clause: bool = False) -> None:
        """
        Рекурсивно собирает таблицы из токена.
        
        Args:
            token: Текущий токен
            tables: Множество для сбора имен таблиц
            cte_names: Множество имен CTE (исключаем их)
            in_from_clause: Флаг, что мы находимся внутри FROM/JOIN clause
        """
        if not hasattr(token, 'tokens'):
            return
        
        prev_token = None
        for i, sub_token in enumerate(token.tokens):
            # Определяем, начинается ли FROM/JOIN clause
            is_from_or_join = (
                sub_token.is_keyword and 
                sub_token.normalized in ('FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 
                                        'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN', 
                                        'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 
                                        'FULL OUTER JOIN', 'STRAIGHT_JOIN')
            )
            
            if is_from_or_join:
                # Следующий токен должен содержать таблицу(ы)
                if i + 1 < len(token.tokens):
                    next_token = token.tokens[i + 1]
                    self._process_table_token(next_token, tables, cte_names)
                in_from_clause = True
            elif in_from_clause:
                # Проверяем, не закончилась ли секция FROM (встретили WHERE, GROUP, etc.)
                if sub_token.is_keyword and sub_token.normalized in (
                    'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'UNION', 
                    'INTERSECT', 'EXCEPT', 'ON', 'USING'
                ):
                    in_from_clause = False
                elif sub_token.ttype in (Keyword, DML) and sub_token.normalized in (
                    'SELECT', 'INSERT', 'UPDATE', 'DELETE'
                ):
                    in_from_clause = False
                else:
                    # Проверяем, является ли токен таблицей (для случаев с запятыми)
                    self._process_table_token(sub_token, tables, cte_names)
            
            # Рекурсивный обход с учетом контекста
            # Если это подзапрос (Parenthesis) или CTE, обрабатываем внутри отдельно
            if isinstance(sub_token, Parenthesis):
                # Это подзапрос - обрабатываем рекурсивно, но CTE внутри подзапроса
                # не видны снаружи, поэтому передаем копию cte_names или новое множество
                self._collect_tables_recursive(sub_token, tables, cte_names, in_from_clause=False)
            else:
                # Передаем текущий контекст in_from_clause только если это не Parenthesis
                self._collect_tables_recursive(sub_token, tables, cte_names, in_from_clause)
            
            prev_token = sub_token
    
    def _process_table_token(self, token: Token, tables: Set[str], cte_names: Set[str]) -> None:
        """
        Обрабатывает токен, который может содержать имя таблицы.
        """
        if token is None or token.is_whitespace:
            return
        
        # Identifier - одиночная таблица с возможным алиасом
        if isinstance(token, Identifier):
            table_name = self._extract_table_from_identifier(token)
            if table_name and table_name.upper() not in cte_names:
                tables.add(table_name)
        
        # IdentifierList - список таблиц (через запятую)
        elif isinstance(token, IdentifierList):
            for ident in token.get_identifiers():
                if isinstance(ident, Identifier):
                    table_name = self._extract_table_from_identifier(ident)
                    if table_name and table_name.upper() not in cte_names:
                        tables.add(table_name)
        
        # Parenthesis - подзапрос (обрабатываем отдельно в _collect_tables_recursive)
        elif isinstance(token, Parenthesis):
            pass  # Уже обрабатывается в рекурсии
        
        # Обычный токен - возможно имя таблицы без алиаса (например, после FROM)
        elif hasattr(token, 'ttype') and token.ttype:
            # Проверяем, является ли токен именем (NAME) или строкой (STRING)
            if token.ttype in (sqlparse.tokens.Name, sqlparse.tokens.String.Single):
                table_name = token.value.strip("'\"`")
                if table_name and table_name.upper() not in cte_names:
                    tables.add(table_name)
    
    def _extract_table_from_identifier(self, identifier: Identifier) -> Optional[str]:
        """
        Извлекает полное имя таблицы из Identifier (учитывает schema.table).
        """
        if not identifier:
            return None
        
        # Получаем все части идентификатора
        parts = []
        for token in identifier.tokens:
            if token.is_whitespace:
                continue
            # Если это разделитель (точка), пропускаем
            if token.ttype in (sqlparse.tokens.Punctuation,):
                continue
            # Если это имя таблицы/схемы
            if hasattr(token, 'ttype') and token.ttype in (
                sqlparse.tokens.Name, 
                sqlparse.tokens.String.Single,
                sqlparse.tokens.String.Symbol
            ):
                parts.append(token.value.strip("'\"`"))
            # Если это Keyword (например, имя таблицы совпадает с ключевым словом)
            elif hasattr(token, 'ttype') and token.ttype in (Keyword,):
                parts.append(token.value.strip("'\"`"))
            # Если это токен с подчеркиванием или цифрами
            elif hasattr(token, 'value') and not token.is_keyword:
                val = token.value.strip("'\"`")
                if val and not val.isspace():
                    parts.append(val)
        
        # Формируем полное имя (schema.table)
        if len(parts) >= 2:
            # Проверяем, не является ли последняя часть алиасом (AS alias или просто alias)
            # Простая эвристика: если последняя часть - одно слово без точек, и 
            # перед ним есть еще части, то это может быть алиас
            # Более надежно: проверить наличие ключевого слова AS
            has_alias = False
            for i, token in enumerate(identifier.tokens):
                if token.is_keyword and token.normalized == 'AS':
                    has_alias = True
                    break
            
            if has_alias and len(parts) > 1:
                # Последняя часть - алиас, убираем её
                return '.'.join(parts[:-1])
            else:
                # Проверяем, есть ли в identifier явный алиас (последнее слово отдельно)
                # Если у нас 3 части: schema table alias -> берем schema.table
                # Если 2 части: schema table -> это schema.table
                # Если 2 части: table alias -> это table
                if len(parts) == 2:
                    # Проверяем, есть ли в исходном идентификаторе точка между частями
                    full_str = str(identifier)
                    if '.' in full_str.split()[-1] or '.' in full_str.split()[0]:
                        return '.'.join(parts)
                    else:
                        # Возможно это "table alias", возвращаем первую часть
                        return parts[0]
                else:
                    return '.'.join(parts[:2])  # Берем только schema.table, остальное - алиасы
        
        elif len(parts) == 1:
            return parts[0]
        
        return None
    
    def _get_identifier_name(self, identifier: Identifier) -> Optional[str]:
        """Получает имя из Identifier (для CTE)."""
        if not identifier:
            return None
        
        # Берем первую непустую часть
        for token in identifier.tokens:
            if token.is_whitespace:
                continue
            if hasattr(token, 'value'):
                return token.value.strip("'\"`")
        return None

    def _extract_table_aliases(self, sql: str) -> Dict[str, str]:
        """
        Извлекает соответствия alias -> table для FROM/JOIN секций.

        Нужно для INTERPOLATE-анализа: в выражении используется alias источника,
        а связать надо с реальной таблицей/частью, которая этот alias представляет.
        """
        alias_to_table: Dict[str, str] = {}
        pattern = re.compile(
            r'\b(?:FROM|JOIN)\s+((?:\w+\.)?\w+)(?:\s+(?:AS\s+)?(\w+))?',
            re.IGNORECASE
        )

        for match in pattern.finditer(sql):
            table_name = match.group(1)
            alias = match.group(2)

            if not table_name:
                continue

            table_upper = table_name.upper()
            alias_to_table[table_upper] = table_upper

            base_name = table_name.split('.')[-1].upper()
            alias_to_table[base_name] = table_upper

            if alias:
                alias_to_table[alias.upper()] = table_upper

        return alias_to_table
    
    def _detect_interpolate_relationships(self, parts: List[Dict]) -> None:
        """
        Определяет связи INTERPOLATE между частями с использованием sqlparse.
        Улучшенная версия: корректно обрабатывает многострочные запросы, 
        алиасы с кавычками, подзапросы и CTE.
        """
        table_to_part = {}
        for part in parts:
            for table in part["dependencies"]["creates"]:
                table_to_part[table.upper()] = part["part_num"]

        print(f"[{self.query_name}] Detected tables: {list(table_to_part.keys())}")

        for part in parts:
            content = part["content"]
            part_num = part["part_num"]

            if not self._INTERPOLATE_PATTERN.search(content):
                continue
            
            print(f"[{self.query_name}] Part {part_num}: Found INTERPOLATE pattern")

            alias_to_table = self._extract_table_aliases(content)
            source_tables = set()

            # Для Vertica-конструкции:
            #   consumer_alias.column INTERPOLATE PREVIOUS VALUE source_alias.column
            # источником диапазона является правая часть после VALUE.
            interpolate_table_pattern = r'(\w+\.\w+)\s+INTERPOLATE\s+(?:PREVIOUS|NEXT)\s+VALUE\s+(\w+\.\w+)'
            for match in re.finditer(interpolate_table_pattern, content, re.IGNORECASE):
                left_side = match.group(1)  # acd.event_date
                right_side = match.group(2)  # uic.act_date

                # Извлекаем имена таблиц из alias.column
                left_table = left_side.split('.')[0]
                right_table = right_side.split('.')[0]
                source_table = alias_to_table.get(right_table.upper(), right_table.upper())

                source_tables.add(source_table)
                print(
                    f"[{self.query_name}] Part {part_num}: INTERPOLATE between "
                    f"{left_table} and {right_table} -> source table {source_table}"
                )

            # Для каждой реально интерполируемой source-таблицы проверяем, создана ли она в другой части
            part["interpolate_sources"] = []

            for table in source_tables:
                if table in table_to_part:
                    source_part = table_to_part[table]
                    if source_part != part_num:
                        interp_info = {
                            "source_part": source_part,
                            "source_table": table,
                            "consumer_part": part_num
                        }
                        part["interpolate_sources"].append(interp_info)

                        print(f"[{self.query_name}] Link: Part {part_num} consumes Part {source_part} (table: {table})")

                        # Отмечаем в источнике, что он используется для INTERPOLATE
                        source_part_obj = next((p for p in parts if p["part_num"] == source_part), None)
                        if source_part_obj:
                            if "interpolate_consumers" not in source_part_obj:
                                source_part_obj["interpolate_consumers"] = []

                            consumer_info = {
                                "consumer_part": part_num,
                                "consumer_table": table
                            }

                            # Проверяем дубликаты
                            if not any(c["consumer_part"] == part_num for c in source_part_obj["interpolate_consumers"]):
                                source_part_obj["interpolate_consumers"].append(consumer_info)
                                print(f"[{self.query_name}] Marked Part {source_part} as INTERPOLATE source for Part {part_num}")
    
    def save_parts(self, parts: List[Dict[str, Any]]) -> None:
        """Сохраняет части на диск и обновляет StateManager."""
        self.vertica_parts_path.mkdir(parents=True, exist_ok=True)
        
        for part in parts:
            file_path = self.vertica_parts_path / f"{self.query_name}_part_{part['part_num']}.sql"
            
            temp_path = file_path.with_suffix('.tmp')
            temp_path.write_text(part["content"], encoding='utf-8')
            temp_path.replace(file_path)
            
            print(f"[{self.query_name}] Saved: {file_path.name}")
        
        # Обновляем metadata с полем status как в старой версии
        parts_map = {
            f"part_{p['part_num']}": {
                "type": p["part_type"],
                "table_name": p.get("table_name"),
                "dependencies": p["dependencies"],
                "status": "split",  # <-- добавлено поле status
                "interpolate_sources": p.get("interpolate_sources", []),
                "interpolate_consumers": p.get("interpolate_consumers", [])
            }
            for p in parts
        }
        
        self.state_manager.update_state({
            "total_parts": len(parts),
            "split_completed": True,
            "parts_map": parts_map,
            "status": "translating"
        })
        
        print(f"[{self.query_name}] State updated: {len(parts)} parts")
    
    def analyze_interpolate_for_existing_parts(self) -> None:
        """Восстановление INTERPOLATE связей для существующих частей."""
        state = self.state_manager.load_state()
        if not state:
            return
        
        parts_map = state.get("parts_map", {})
        total_parts = state.get("total_parts", 0)
        
        print(f"[{self.query_name}] Восстановление INTERPOLATE связей для {total_parts} частей...")
        
        parts = []
        for i in range(total_parts):
            file_path = self.vertica_parts_path / f"{self.query_name}_part_{i}.sql"
            if file_path.exists():
                content = file_path.read_text(encoding='utf-8')
                part_info = parts_map.get(f"part_{i}", {})
                parts.append({
                    "part_num": i,
                    "content": content,
                    "dependencies": part_info.get("dependencies", {"creates": [], "uses": []}),
                    "part_type": part_info.get("type", "dml_block"),
                    "table_name": part_info.get("table_name"),
                    "interpolate_sources": [],
                    "interpolate_consumers": []
                })
        
        self._detect_interpolate_relationships(parts)
        
        updated = False
        for p in parts:
            part_key = f"part_{p['part_num']}"
            if part_key in parts_map:
                old_sources = parts_map[part_key].get("interpolate_sources", [])
                old_consumers = parts_map[part_key].get("interpolate_consumers", [])
                new_sources = p.get("interpolate_sources", [])
                new_consumers = p.get("interpolate_consumers", [])
                
                if old_sources != new_sources or old_consumers != new_consumers:
                    updated = True
                    parts_map[part_key]["interpolate_sources"] = new_sources
                    parts_map[part_key]["interpolate_consumers"] = new_consumers
        
        if updated:
            self.state_manager.update_state({
                "parts_map": parts_map,
                "interpolate_analyzed_at": datetime.utcnow().isoformat()
            })
            print(f"[{self.query_name}] ✓ INTERPOLATE связи восстановлены и сохранены")
        else:
            print(f"[{self.query_name}] INTERPOLATE связи уже были в metadata")
