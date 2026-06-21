"""
Client adapter for the external SQL validation API.

Responsibilities:
- submit assembled SQL to a compatible validation service;
- poll the generated validation report;
- parse API/HTML response into structured validation errors.

This module does not modify SQL parts. Fix orchestration belongs to
core.api_validation_fixer.
"""

import requests
import time
import re
import os
import ipaddress
from bs4 import BeautifulSoup
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


@dataclass
class ValidationError:
    """Структура для хранения данных об ошибке"""
    title: str
    comment: str
    examples: List[str]
    metadata: Optional[Dict[str, Any]] = None
    
    def __repr__(self):
        return f"ValidationError({self.title})"


class SQLValidator:
    """
    Класс для валидации SQL файлов через внешний сервис.
    Парсит все типы ошибок в отдельные атрибуты.
    """
    
    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url.rstrip()
        self.session = requests.Session()
        self.task_id: Optional[str] = None
        self.report_url: Optional[str] = None
        self.passed_count: int = 0
        self.logs: str = ""
        
        # Инициализация сессии
        self._ensure_no_proxy_for_base_url()
        self._init_session()
        
        # Переменные для ошибок (все из вашего списка)
        self.old_table: Optional[ValidationError] = None                    # Блокировка старых витрин      #searh in list replace _trino
        self.tbl_name_part_0: Optional[ValidationError] = None              # Snake_case колонки            #part_0
        
        # Шапка и параметры
        self.no_header: Optional[ValidationError] = None                     # Шапка не найдена           #part_0
        self.header_required_attrs: Optional[ValidationError] = None        # Обязательные атрибуты       #part_0
        self.header_param_types: Optional[ValidationError] = None           # Типы параметров             #part_0
        self.header_param_fix: Optional[ValidationError] = None             # Присвоение значений         #part_0
        self.header_params_usage: Optional[ValidationError] = None          # Использование параметров    #part_0
        self.header_scheduled_check: Optional[ValidationError] = None       # Атрибут scheduled           #part_0
        self.header_engine_fix: Optional[ValidationError] = None            # Поле engine                 #part_0 
        self.header_keys_check: Optional[ValidationError] = None            # Параметр keys               #part_0 
        self.header_type_check: Optional[ValidationError] = None            # Параметр type               #part_0 
        self.header_days_col: Optional[ValidationError] = None              # Параметр days_col           #part_0 
        self.header_days_to_keep: Optional[ValidationError] = None          # Параметр days_to_keep       #part_0 

        # SQL и DDL
        self.sql_table_n: List[ValidationError] = []                        # SQL синтаксис (список!)       #fuzzy_search
        self.ddl_check: Optional[ValidationError] = None                    # DDL синтаксис                 #part_0 
        
        # Партиции и данные
        self.dml_last_part: Optional[ValidationError] = None                # Безопасное обновление         #search sandbox and insert
        self.full_refresh_part: Optional[ValidationError] = None            # Партицирование
        
        # Колонки и типы
        self.col_varchar_numeric_size: Optional[ValidationError] = None     # Размерность varchar/numeric   #part_0
        self.col_decimal_size_limit: Optional[ValidationError] = None       # Ограничение Decimal           #part_0
        self.col_launch_id_first: Optional[ValidationError] = None          # launch_id первой              #part_0
        self.col_key_exists: Optional[ValidationError] = None               # Колонки ключа                 #part_0
        
        # Безопасность и схемы
        self.tbl_delete_check: Optional[ValidationError] = None             # Удаляемые таблицы
        self.attrs_logical_vertical: Optional[ValidationError] = None       # logical_category/vertical
        self.schema_allowed_check: Optional[ValidationError] = None         # Разрешенные схемы
        self.dict_repository_exists: Optional[ValidationError] = None       # Справочники в репозитории     #fuzzy_search
        
    def _init_session(self):
        """Инициализация сессии (получение кук)"""
        try:
            self.session.get(f"{self.base_url}/test", timeout=10)
        except requests.RequestException as e:
            print(f"⚠️ Предупреждение при инициализации сессии: {e}")

    def _ensure_no_proxy_for_base_url(self):
        """
        Добавляет host из base_url в NO_PROXY/no_proxy для локальных/приватных/внутренних
        адресов, чтобы requests не ходил через HTTP(S)_PROXY.
        """
        host = urlparse(self.base_url).hostname
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
    
    def validate(self, sql_file_path: str, max_wait_seconds: int = 60, poll_interval: int = 3) -> 'SQLValidator':
        """
        Основной метод валидации.
        
        Args:
            sql_file_path: Путь к SQL файлу
            max_wait_seconds: Максимальное время ожидания отчета
            poll_interval: Интервал проверки готовности (сек)
            
        Returns:
            self (для chaining: validator.validate("file.sql").has_errors())
        """
        # 1. Читаем файл
        sql_path = Path(sql_file_path)
        if not sql_path.exists():
            raise FileNotFoundError(f"Файл не найден: {sql_file_path}")
            
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_code = f.read().strip()
            
        # 2. Отправляем на проверку
        print(f"📤 Отправка файла {sql_path.name} на проверку...")
        self._submit_code(sql_code)
        
        if not self.task_id:
            raise ValueError("Не удалось получить task_id от сервера")
            
        print(f"⏳ Ожидание готовности отчета (task_id: {self.task_id})...")
        
        # 3. Ждем готовности отчета с поллингом (с проверкой содержимого!)
        self._wait_for_report(max_wait_seconds, poll_interval)
        
        # 4. Парсим результат
        print("🔍 Парсинг результатов...")
        self._parse_report()
        
        print(f"✅ Готово! Найдено ошибок: {self.total_errors}, Успешно: {self.passed_count}")
        
        return self
    
    def _submit_code(self, sql_code: str):
        """Отправка SQL кода на сервер"""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        
        payload = {"new_code": sql_code}
        
        response = self.session.post(
            f"{self.base_url}/test",
            json=payload,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        self.task_id = data.get("task_id")
        self.report_url = f"{self.base_url}/report/{self.task_id}" if self.task_id else None
        
    def _is_report_ready(self, response_text: str) -> bool:
        """
        Проверяет, готов ли отчет (загрузилась ли страница с результатами)
        """
        # Если есть "waiting" в title - страница еще грузится
        if "waiting" in response_text.lower() or "ждите" in response_text.lower():
            return False
            
        # Проверяем наличие блоков с результатами (failed-test или passed-test)
        soup = BeautifulSoup(response_text, 'html.parser')
        
        # Ищем блоки с ошибками или успешными тестами
        failed_blocks = soup.find_all('div', class_='failed-test')
        passed_blocks = soup.find_all('div', class_='passed-test')
        
        # Если нашли хотя бы один блок - отчет готов
        if failed_blocks or passed_blocks:
            return True
            
        # Проверяем наличие текста "ошибок" или "успешно" в любом виде
        text_lower = response_text.lower()
        indicators = ['failed-test', 'passed-test', 'ошибк', 'успешн', 'error', 'success']
        if any(ind in text_lower for ind in indicators):
            return True
            
        return False
    
    def _wait_for_report(self, max_seconds: int, interval: int):
        """Ожидание готовности отчета с поллингом"""
        elapsed = 0
        last_response = None
        
        while elapsed < max_seconds:
            try:
                response = self.session.get(self.report_url, timeout=10)
                if response.status_code == 200:
                    # Проверяем, действительно ли отчет готов (не waiting...)
                    if self._is_report_ready(response.text):
                        self._last_response = response
                        print(f"   ✅ Отчет готов (загружено за {elapsed} сек)")
                        return
                    else:
                        print(f"   ⏳ Загрузка... ({elapsed} сек)")
                        
            except requests.RequestException as e:
                print(f"   ⚠️ Ошибка запроса: {e}")
                
            time.sleep(interval)
            elapsed += interval
            
        raise TimeoutError(f"Отчет не готов за {max_seconds} секунд. Проверьте ссылку: {self.report_url}")
    
    def _parse_report(self):
        """Парсинг HTML отчета"""
        if not hasattr(self, '_last_response'):
            raise ValueError("Нет данных для парсинга. Сначала вызовите _wait_for_report.")
            
        response = self._last_response
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Сброс предыдущих значений
        self.sql_table_n = []
        self.passed_count = 0
        self.logs = ""
        
        # Парсим failed блоки
        failed_blocks = soup.find_all('div', class_='failed-test')
        
        for block in failed_blocks:
            title_elem = block.find('span', class_='test-title text')
            if not title_elem:
                continue
                
            title = title_elem.get_text(strip=True)
            
            comment_elem = block.find('div', class_='failed-comment text')
            comment = comment_elem.get_text(strip=True) if comment_elem else ""
            
            # Примеры (pre теги)
            examples = []
            pre_elems = block.find_all('pre', class_='failed-example text')
            for pre in pre_elems:
                # Очистка ANSI кодов
                text = pre.get_text(strip=True)
                text = text.replace('\x1b', '').replace('[0m', '').replace('[4m', '')
                examples.append(text)
            
            metadata = self._extract_error_metadata(title=title, comment=comment, examples=examples)
            error = ValidationError(title=title, comment=comment, examples=examples, metadata=metadata)
            self._route_error(error)
        
        # Считаем успешные тесты
        passed_blocks = soup.find_all('div', class_='passed-test')
        for block in passed_blocks:
            summary_elem = block.find('span', class_='test-title text')
            if summary_elem and summary_elem.get_text(strip=True) != "Логи":
                self.passed_count += 1
        
        # Логи
        log_container = soup.find('div', class_='warned-comment text')
        if log_container:
            self.logs = log_container.get_text(strip=True)
    
    def _route_error(self, error: ValidationError):
        """Распределение ошибок по переменным"""
        title = error.title

        # Витрины и таблицы
        if "Блокировка использования витрин старых версий" in title:
            self.old_table = error
        elif "Все имена колонок должны быть форматированы в snake_case" in title:
            self.tbl_name_part_0 = error 

        # Шапка и параметры
        elif "Парсинг шапки" in title:
            self.no_header = error
        elif "В шапке присутствовать обязательные атрибуты" in title:
            self.header_required_attrs = error
        elif "Значения параметров в шапке должны быть определенного для них типа" in title:
            self.header_param_types = error
        elif "Параметрам должно быть присвоено значение" in title:
            self.header_param_fix = error
        elif "Все параметры из шапки должны использоваться в коде" in title:
            self.header_params_usage = error
        elif "Проверка атрибута scheduled" in title:
            self.header_scheduled_check = error
        elif "Проверка заполненности поля engine" in title:
            self.header_engine_fix = error
        elif "Проверка правильности заполнения параметра keys" in title:
            self.header_keys_check = error
        elif "Проверка правильности заполнения параметра type для витрин" in title:
            self.header_type_check = error
        elif "Параметр days_col должен соответствовать требованиям" in title:
            self.header_days_col = error
        elif "Параметр days_to_keep должен соответствовать требованиям" in title:
            self.header_days_to_keep = error

        # SQL и DDL
        elif "Проверка sql синтаксиса кода витрины" in title:
            self.sql_table_n.append(error)
        elif "Проверка DDL синтаксиса создания таблицы" in title:
            self.ddl_check = error

        # Партиции и данные
        elif "Проверка безопасного обновления данных" in title:
            self.dml_last_part = error
        elif "FULL_REFRESH витрины должны быть партицированы" in title:
            self.full_refresh_part = error

        # Колонки и типы
        elif "Для колонок типов varchar и numeric всегда должна быть указана размерность" in title:
            self.col_varchar_numeric_size = error
        elif "Для колонок типов varchar и numeric размерность не должна быть слишком большой" in title:
            self.col_decimal_size_limit = error
        elif "В новых витринах launch_id должен быть первой колонкой" in title:
            self.col_launch_id_first = error
        elif "Колонки из ключа витрины должны существовать в таблице" in title:
            self.col_key_exists = error

        # Безопасность и схемы
        elif "Проверка таблиц, которые удаляются в коде" in title:
            self.tbl_delete_check = error
        elif "В витринах нежелательно хранить атрибуты logical_category" in title:
            self.attrs_logical_vertical = error
        elif "В запросах должны использоваться только разрешенные схемы" in title:
            self.schema_allowed_check = error
        elif "Проверка на существование справочников в репозитории" in title:
            self.dict_repository_exists = error
        else:
            print(f"⚠️ Неизвестный тип ошибки: {title}")

    def _extract_error_metadata(self, title: str, comment: str, examples: List[str]) -> Dict[str, Any]:
        """Извлекает структурированные данные из текста ошибки API."""
        metadata: Dict[str, Any] = {}

        if "Проверка sql синтаксиса кода витрины" not in title:
            return metadata

        source_text = "\n".join([comment] + examples).strip()
        if not source_text:
            return metadata

        clean_text = self._strip_ansi(source_text)

        part_match = re.search(r'--\s*Part\s+(\d+)\b', clean_text, re.IGNORECASE)
        if part_match:
            metadata["part_num"] = int(part_match.group(1))

        query_match = re.search(
            r'QUERY:\s*(.*?)(?=\n\s*ERRORS?:|\Z)',
            clean_text,
            re.IGNORECASE | re.DOTALL,
        )
        if query_match:
            metadata["query"] = query_match.group(1).strip()

        errors_match = re.search(
            r'ERRORS?:\s*(.*)\Z',
            clean_text,
            re.IGNORECASE | re.DOTALL,
        )
        if errors_match:
            raw_errors = errors_match.group(1).strip()
            metadata["raw_errors"] = raw_errors
            metadata["error_messages"] = self._split_error_messages(raw_errors)

        identifiers = set()
        for text in filter(None, [metadata.get("query"), metadata.get("raw_errors"), clean_text]):
            identifiers.update(
                token
                for token in re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', text)
                if "_" in token
            )
        if identifiers:
            metadata["identifiers"] = sorted(identifiers)

        return metadata

    @staticmethod
    def _strip_ansi(text: str) -> str:
        """Удаляет ANSI escape sequences из текста."""
        return re.sub(r'\x1b\[[0-9;]*[A-Za-z]', '', text)

    @staticmethod
    def _split_error_messages(raw_errors: str) -> List[str]:
        """Нормализует блок ERRORS в список сообщений."""
        messages: List[str] = []

        for line in raw_errors.splitlines():
            normalized = line.strip()
            if not normalized:
                continue
            if normalized.startswith("^"):
                continue
            messages.append(normalized)

        return messages
    
    @property
    def total_errors(self) -> int:
        """Общее количество ошибок"""
        singles = [
            self.old_table, self.tbl_name_part_0,
            self.no_header,
            self.header_required_attrs, self.header_param_types, self.header_param_fix,
            self.header_params_usage, self.header_scheduled_check, self.header_engine_fix,
            self.header_keys_check, self.header_type_check,
            self.header_days_col,      # ← ДОБАВЛЕН
            self.header_days_to_keep,  # ← ДОБАВЛЕН
            self.ddl_check, self.dml_last_part, self.full_refresh_part,
            self.col_varchar_numeric_size, self.col_decimal_size_limit, 
            self.col_launch_id_first, self.col_key_exists,
            self.tbl_delete_check, self.attrs_logical_vertical, 
            self.schema_allowed_check, self.dict_repository_exists
        ]
        return sum(1 for x in singles if x is not None) + len(self.sql_table_n)
    
    def has_errors(self) -> bool:
        """Есть ли ошибки?"""
        return self.total_errors > 0
    
    def get_all_errors(self) -> Dict[str, Any]:
        """Возвращает словарь со всеми найденными ошибками (только не-None)"""
        errors = {}
        
        if self.old_table:
            errors['old_table'] = self.old_table
        if self.tbl_name_part_0:
            errors['tbl_name_part_0'] = self.tbl_name_part_0
        if self.no_header:
            errors['no_header'] = self.no_header
        if self.header_required_attrs:
            errors['header_required_attrs'] = self.header_required_attrs
        if self.header_param_types:
            errors['header_param_types'] = self.header_param_types
        if self.header_param_fix:
            errors['header_param_fix'] = self.header_param_fix
        if self.header_params_usage:
            errors['header_params_usage'] = self.header_params_usage
        if self.header_scheduled_check:
            errors['header_scheduled_check'] = self.header_scheduled_check
        if self.header_engine_fix:
            errors['header_engine_fix'] = self.header_engine_fix
        if self.header_keys_check:
            errors['header_keys_check'] = self.header_keys_check
        if self.header_type_check:
            errors['header_type_check'] = self.header_type_check
        if self.header_days_col:      # ← ДОБАВЛЕН
            errors['header_days_col'] = self.header_days_col
        if self.header_days_to_keep:  # ← ДОБАВЛЕН
            errors['header_days_to_keep'] = self.header_days_to_keep
        if self.sql_table_n:
            errors['sql_table_n'] = self.sql_table_n
        if self.ddl_check:
            errors['ddl_check'] = self.ddl_check
        if self.dml_last_part:
            errors['dml_last_part'] = self.dml_last_part
        if self.full_refresh_part:
            errors['full_refresh_part'] = self.full_refresh_part
        if self.col_varchar_numeric_size:
            errors['col_varchar_numeric_size'] = self.col_varchar_numeric_size
        if self.col_decimal_size_limit:
            errors['col_decimal_size_limit'] = self.col_decimal_size_limit
        if self.col_launch_id_first:
            errors['col_launch_id_first'] = self.col_launch_id_first
        if self.col_key_exists:
            errors['col_key_exists'] = self.col_key_exists
        if self.tbl_delete_check:
            errors['tbl_delete_check'] = self.tbl_delete_check
        if self.attrs_logical_vertical:
            errors['attrs_logical_vertical'] = self.attrs_logical_vertical
        if self.schema_allowed_check:
            errors['schema_allowed_check'] = self.schema_allowed_check
        if self.dict_repository_exists:
            errors['dict_repository_exists'] = self.dict_repository_exists
            
        return errors
    
    def print_report(self):
        """Красивый вывод отчета в консоль"""
        print("\n" + "="*70)
        print(f"ОТЧЕТ О ВАЛИДАЦИИ")
        print(f"URL: {self.report_url}")
        print("="*70)
        print(f"✅ Успешных тестов: {self.passed_count}")
        print(f"❌ Ошибок найдено: {self.total_errors}")
        
        if not self.has_errors():
            print("\n🎉 Все проверки пройдены успешно!")
            return
            
        errors = self.get_all_errors()
        
        for name, error in errors.items():
            if isinstance(error, list):
                print(f"\n🔴 {name}: {len(error)} шт.")
                for i, err in enumerate(error, 1):
                    print(f"   [{i}] {err.title}")
                    if err.comment:
                        print(f"       {err.comment[:100]}...")
            else:
                print(f"\n🔴 {name}: {error.title}")
                if error.comment:
                    print(f"   Описание: {error.comment[:150]}...")
                if error.examples:
                    print(f"   Примеры: {len(error.examples)} шт.")
                    
        print("\n" + "="*70)


# ==================== ПРИМЕР ИСПОЛЬЗОВАНИЯ ====================

if __name__ == "__main__":
    # Создаем валидатор
    validator = SQLValidator()
    
    # Валидируем файл
    try:
        validator.validate(
            sql_file_path="workflow/done/trino/sample_orders_monthly_trino.sql",
            max_wait_seconds=60,  # Увеличил до 60 сек
            poll_interval=3       # Проверяем каждые 3 секунды
        )
        
        # Проверяем конкретные ошибки
        if validator.no_header:
            print(f"\n⚠️ Шапка не найдена: {validator.no_header.comment}")
            
        if validator.header_param_fix:
            print(f"\n⚠️ Проблема с параметрами: {validator.header_param_fix.comment}")
            
        if validator.sql_table_n:
            print(f"\n⚠️ SQL ошибки: {len(validator.sql_table_n)} шт.")
            for err in validator.sql_table_n:
                print(f"   - {err.comment[:100]}...")
        
        # Получаем все ошибки как словарь для дальнейшей обработки
        all_errors = validator.get_all_errors()
        
        # Красивый вывод
        validator.print_report()
        
    except Exception as e:
        print(f"❌ Ошибка валидации: {e}")
