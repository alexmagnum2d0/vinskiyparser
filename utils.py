"""
Утилитарные классы для парсера форума Винского - Оптимизированная версия.
"""

import logging
import re
import time
import random
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any, Callable, Union
from functools import wraps, lru_cache
from collections import OrderedDict
import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import html2text

from config import Config
from models import (
    PageNotFoundError, LogLevel, RetryConfig, LogContext, CacheEntry, 
    PerformanceMetrics, ErrorContext, RetryableError, NonRetryableError
)


class EnhancedLogger:
    """Оптимизированная система логирования с lazy evaluation и контекстом."""
    
    _loggers: Dict[str, logging.Logger] = {}
    _lock = threading.Lock()
    _global_log_level: Optional[str] = None  # Глобальный уровень логирования
    
    def __init__(self, name: str, context: Optional[LogContext] = None, log_level: Optional[str] = None):
        """Инициализация логгера с контекстом.
        
        Args:
            name: Имя логгера
            context: Контекст логирования
            log_level: Уровень логирования (переопределяет Config.LOG_LEVEL)
        """
        self.name = name
        self.context = context or LogContext(operation="general")
        # Используем переданный уровень, затем глобальный, затем из конфига
        effective_level = log_level or self._global_log_level or Config.LOG_LEVEL
        self._logger = self._get_or_create_logger(name, effective_level)
    
    @classmethod
    def set_global_log_level(cls, log_level: str) -> None:
        """Устанавливает глобальный уровень логирования для всех новых логгеров."""
        cls._global_log_level = log_level
    
    @classmethod
    def _get_or_create_logger(cls, name: str, log_level: Optional[str] = None) -> logging.Logger:
        """Получает или создает логгер (thread-safe)."""
        # Создаем уникальный ключ для логгера с учетом уровня
        effective_level = log_level or cls._global_log_level or Config.LOG_LEVEL
        logger_key = f"{name}_{effective_level}"
        
        if logger_key not in cls._loggers:
            with cls._lock:
                if logger_key not in cls._loggers:
                    logger = logging.getLogger(logger_key)
                    if not logger.handlers:
                        handler = logging.StreamHandler()
                        formatter = logging.Formatter(Config.LOG_FORMAT)
                        handler.setFormatter(formatter)
                        logger.addHandler(handler)
                        
                        log_levels = {
                            LogLevel.DEBUG.value: logging.DEBUG,
                            LogLevel.INFO.value: logging.INFO,
                            LogLevel.WARNING.value: logging.WARNING,
                            LogLevel.ERROR.value: logging.ERROR
                        }
                        # Используем эффективный уровень
                        logger.setLevel(log_levels.get(effective_level.upper(), logging.INFO))
                    
                    cls._loggers[logger_key] = logger
        
        return cls._loggers[logger_key]
    
    def with_context(self, **kwargs) -> 'EnhancedLogger':
        """Создает новый логгер с дополнительным контекстом."""
        new_context = LogContext(
            operation=self.context.operation,
            url=kwargs.get('url', self.context.url),
            page_num=kwargs.get('page_num', self.context.page_num),
            topic_title=kwargs.get('topic_title', self.context.topic_title),
            additional_data={**self.context.additional_data, **kwargs}
        )
        # Получаем текущий уровень логирования из существующего логгера
        current_level = None
        for level_name, level_value in {
            LogLevel.DEBUG.value: logging.DEBUG,
            LogLevel.INFO.value: logging.INFO,
            LogLevel.WARNING.value: logging.WARNING,
            LogLevel.ERROR.value: logging.ERROR
        }.items():
            if self._logger.level == level_value:
                current_level = level_name
                break
        
        return EnhancedLogger(self.name, new_context, current_level)
    
    def _log_with_context(self, level: int, msg_func: Callable[[], str], **kwargs):
        """Логирует сообщение с контекстом (lazy evaluation)."""
        if self._logger.isEnabledFor(level):
            context = {**self.context.to_dict(), **kwargs}
            context_str = " | ".join(f"{k}={v}" for k, v in context.items() if v is not None)
            message = msg_func() if callable(msg_func) else str(msg_func)
            self._logger.log(level, f"[{context_str}] {message}")
    
    def debug(self, msg_func: Union[Callable[[], str], str], **kwargs):
        """Debug логирование с lazy evaluation."""
        self._log_with_context(logging.DEBUG, msg_func, **kwargs)
    
    def info(self, msg_func: Union[Callable[[], str], str], **kwargs):
        """Info логирование с lazy evaluation."""
        self._log_with_context(logging.INFO, msg_func, **kwargs)
    
    def warning(self, msg_func: Union[Callable[[], str], str], **kwargs):
        """Warning логирование с lazy evaluation."""
        self._log_with_context(logging.WARNING, msg_func, **kwargs)
    
    def error(self, msg_func: Union[Callable[[], str], str], **kwargs):
        """Error логирование с lazy evaluation."""
        self._log_with_context(logging.ERROR, msg_func, **kwargs)


class RetryManager:
    """Менеджер retry операций с различными стратегиями."""
    
    def __init__(self, config: RetryConfig, logger: EnhancedLogger):
        """Инициализация менеджера retry.
        
        Args:
            config: Конфигурация retry
            logger: Логгер для записи попыток
        """
        self.config = config
        self.logger = logger
    
    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """Выполняет функцию с retry механизмом.
        
        Args:
            func: Функция для выполнения
            *args: Позиционные аргументы
            **kwargs: Именованные аргументы
            
        Returns:
            Результат выполнения функции
            
        Raises:
            Exception: Последнее исключение после всех попыток
        """
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    self.logger.info(lambda: f"Операция успешна после {attempt + 1} попыток")
                return result
                
            except self.config.retryable_exceptions as e:
                last_exception = e
                if attempt == self.config.max_attempts - 1:
                    break
                
                delay = self._calculate_delay(attempt)
                self.logger.warning(
                    lambda: f"Попытка {attempt + 1} неудачна, ждем {delay:.2f}с: {e}"
                )
                time.sleep(delay)
                
            except Exception as e:
                # Не повторяем для неожиданных исключений
                self.logger.error(lambda: f"Неповторяемая ошибка: {e}")
                raise NonRetryableError(f"Неповторяемая ошибка: {e}") from e
        
        raise last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        """Вычисляет задержку для retry."""
        if self.config.strategy == "exponential_backoff":
            delay = min(
                self.config.base_delay * (self.config.backoff_factor ** attempt),
                self.config.max_delay
            )
        elif self.config.strategy == "linear_backoff":
            delay = min(
                self.config.base_delay * (attempt + 1),
                self.config.max_delay
            )
        else:  # FIXED_DELAY
            delay = self.config.base_delay
        
        if self.config.jitter:
            jitter = random.uniform(-0.1, 0.1) * delay
            delay = max(0, delay + jitter)
        
        return delay


class MemoryCache:
    """Быстрый in-memory кэш с TTL и LRU eviction."""
    
    def __init__(self, max_size: int = Config.CACHE_MAX_SIZE, default_ttl: float = Config.CACHE_TTL):
        """Инициализация кэша.
        
        Args:
            max_size: Максимальный размер кэша
            default_ttl: TTL по умолчанию
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._data: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        """Получает значение из кэша."""
        with self._lock:
            if key in self._data:
                entry = self._data[key]
                if entry.is_expired():
                    del self._data[key]
                    return None
                
                # Перемещаем в конец (LRU)
                self._data.move_to_end(key)
                return entry.data
        return None
    
    def put(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Помещает значение в кэш."""
        with self._lock:
            # Удаляем expired записи
            self._cleanup_expired()
            
            # Удаляем старые записи при превышении размера
            while len(self._data) >= self.max_size:
                self._data.popitem(last=False)
            
            entry = CacheEntry(
                data=value,
                timestamp=time.time(),
                ttl=ttl or self.default_ttl
            )
            self._data[key] = entry
    
    def _cleanup_expired(self) -> None:
        """Очищает expired записи."""
        current_time = time.time()
        expired_keys = [
            key for key, entry in self._data.items()
            if current_time - entry.timestamp > entry.ttl
        ]
        for key in expired_keys:
            del self._data[key]
    
    def clear(self) -> None:
        """Очищает кэш."""
        with self._lock:
            self._data.clear()


class OptimizedHttpClient:
    """Оптимизированный HTTP клиент с connection pooling, retry и кэшированием."""
    
    def __init__(self, base_url: str = Config.BASE_URL):
        """Инициализация HTTP клиента.
        
        Args:
            base_url: Базовый URL для запросов
        """
        self.base_url = base_url
        self.logger = EnhancedLogger(self.__class__.__name__, LogContext(operation="http_request"))
        self.metrics = PerformanceMetrics()
        
        # Настройка сессии с connection pooling
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': Config.USER_AGENT})
        
        if Config.ENABLE_COMPRESSION:
            self.session.headers.update({'Accept-Encoding': 'gzip, deflate'})
        
        # Настройка retry на уровне urllib3
        retry_strategy = Retry(
            total=0,  # Мы управляем retry сами
            connect=2,
            read=2,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1
        )
        
        # HTTP адаптер с connection pooling
        adapter = HTTPAdapter(**Config.get_http_adapter_config())
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Кэш и retry менеджер
        self.cache = MemoryCache() if Config.ENABLE_URL_CACHE else None
        self.retry_manager = RetryManager(Config.get_retry_config(), self.logger)
        
        # Предкомпилированные regex для скорости
        if Config.PRECOMPILE_REGEX:
            self._url_hash_regex = re.compile(r'[^\w\-._~]')
    
    def get_page(self, url: str, timeout: int = Config.REQUEST_TIMEOUT, 
                 use_cache: bool = True) -> Optional[BeautifulSoup]:
        """Получает страницу с retry, кэшированием и метриками.
        
        Args:
            url: URL страницы
            timeout: Таймаут запроса
            use_cache: Использовать ли кэш
            
        Returns:
            BeautifulSoup объект или None в случае ошибки
        """
        start_time = time.time()
        from_cache = False
        
        try:
            # Проверяем кэш
            cache_key = self._get_cache_key(url)
            if use_cache and self.cache:
                cached_result = self.cache.get(cache_key)
                if cached_result:
                    from_cache = True
                    self.logger.debug(lambda: f"Страница получена из кэша: {url}")
                    self.metrics.add_request(True, time.time() - start_time, True)
                    return cached_result
            
            # Выполняем запрос с retry
            soup = self.retry_manager.execute_with_retry(
                self._make_request, url, timeout
            )
            
            # Сохраняем в кэш
            if use_cache and self.cache and soup:
                self.cache.put(cache_key, soup)
            
            self.metrics.add_request(True, time.time() - start_time, from_cache)
            return soup
            
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при получении страницы {url}: {e}")
            self.metrics.add_request(False, time.time() - start_time, from_cache)
            return None
    
    def _make_request(self, url: str, timeout: int) -> BeautifulSoup:
        """Выполняет HTTP запрос."""
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            
            # Оптимизированный парсинг
            soup = BeautifulSoup(
                response.content, 
                Config.HTML_PARSER,
                parse_only=None  # Парсим всё, но можно ограничить при необходимости
            )
            
            self.logger.debug(lambda: f"Страница успешно получена: {url}")
            return soup
            
        except requests.exceptions.Timeout:
            raise RetryableError(f"Таймаут при получении страницы: {url}")
        except requests.exceptions.ConnectionError as e:
            raise RetryableError(f"Ошибка соединения: {url}: {e}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [429, 500, 502, 503, 504]:
                raise RetryableError(f"HTTP ошибка {e.response.status_code}: {url}")
            else:
                raise NonRetryableError(f"HTTP ошибка {e.response.status_code}: {url}")
        except Exception as e:
            raise NonRetryableError(f"Неожиданная ошибка при получении {url}: {e}")
    
    def _get_cache_key(self, url: str) -> str:
        """Генерирует ключ кэша для URL."""
        if Config.PRECOMPILE_REGEX:
            # Быстрое хэширование
            return hashlib.md5(url.encode()).hexdigest()
        else:
            return url
    
    def get_metrics(self) -> PerformanceMetrics:
        """Возвращает метрики производительности."""
        return self.metrics
    
    def clear_cache(self) -> None:
        """Очищает кэш."""
        if self.cache:
            self.cache.clear()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.session.close()


class OptimizedMarkdownConverter:
    """Оптимизированный конвертер HTML в Markdown с кэшированием."""
    
    def __init__(self):
        """Инициализация конвертера."""
        self.logger = EnhancedLogger(self.__class__.__name__, LogContext(operation="markdown_convert"))
        self._setup_html2text()
        
        # Кэш для повторяющихся конверсий
        self.cache = MemoryCache(max_size=500, default_ttl=600.0) if Config.ENABLE_PARSING_CACHE else None
        
        # Предкомпилированные regex для производительности
        if Config.PRECOMPILE_REGEX:
            self._cleanup_patterns = [
                (re.compile(r'\n{3,}'), '\n\n'),
                (re.compile(r'\\-'), '-'),
                (re.compile(r'\[([^\]]+)\]\([^)]+\s+"[^"]*"\)'), r'(\1)'),
            ]
            self._ad_pattern = re.compile(r'ads-\d+')
    
    def _setup_html2text(self) -> None:
        """Настраивает конвертер html2text."""
        self.h = html2text.HTML2Text()
        self.h.ignore_links = False
        self.h.ignore_images = False
        self.h.ignore_emphasis = False
        self.h.body_width = 0
        self.h.escape_snob = False
        self.h.escape_all = False
    
    def convert(self, html_content: str, use_cache: bool = True) -> str:
        """Конвертирует HTML в markdown с кэшированием.
        
        Args:
            html_content: HTML содержимое
            use_cache: Использовать ли кэш
            
        Returns:
            Markdown содержимое
        """
        try:
            # Проверяем кэш
            if use_cache and self.cache:
                cache_key = hashlib.md5(html_content.encode()).hexdigest()
                cached_result = self.cache.get(cache_key)
                if cached_result:
                    return cached_result
            
            # Очищаем HTML
            clean_html = self._clean_html_optimized(html_content)
            
            # Конвертируем в markdown
            markdown = self.h.handle(clean_html)
            
            # Постобработка markdown
            result = self._postprocess_markdown_optimized(markdown)
            
            # Сохраняем в кэш
            if use_cache and self.cache:
                self.cache.put(cache_key, result)
            
            return result
            
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при конвертации в markdown: {e}")
            return html_content
    
    def _clean_html_optimized(self, html_content: str) -> str:
        """Оптимизированная очистка HTML."""
        soup = BeautifulSoup(html_content, Config.HTML_PARSER)
        
        # Удаляем рекламные блоки (быстрее через предкомпилированный regex)
        if Config.PRECOMPILE_REGEX:
            for ad in soup.find_all('div', {'id': self._ad_pattern}):
                ad.decompose()
        else:
            for ad in soup.find_all('div', {'id': re.compile(r'ads-\d+')}):
                ad.decompose()
        
        # Удаляем скрипты
        for script in soup.find_all('script'):
            script.decompose()
            
        return str(soup)
    
    def _postprocess_markdown_optimized(self, markdown: str) -> str:
        """Оптимизированная постобработка markdown."""
        if Config.PRECOMPILE_REGEX:
            # Используем предкомпилированные паттерны
            for pattern, replacement in self._cleanup_patterns:
                markdown = pattern.sub(replacement, markdown)
        else:
            # Обычная обработка
            markdown = re.sub(r'\n{3,}', '\n\n', markdown)
            markdown = re.sub(r'\\-', '-', markdown)
            markdown = re.sub(r'\[([^\]]+)\]\([^)]+\s+"[^"]*"\)', r'(\1)', markdown)
        
        return markdown.strip()


class OptimizedFileWriter:
    """Оптимизированный класс для работы с файлами."""
    
    def __init__(self):
        """Инициализация файлового писателя."""
        self.logger = EnhancedLogger(self.__class__.__name__, LogContext(operation="file_write"))
        
        # Буферизация для больших файлов
        self._write_buffer = {}
        self._buffer_size = Config.BUFFER_SIZE
    
    def create_safe_filename(self, title: str, max_length: int = Config.MAX_FILENAME_LENGTH) -> str:
        """Создает безопасное имя файла."""
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', title)
        return safe_filename[:max_length]
    
    def ensure_directory_exists(self, directory: Path) -> None:
        """Создает директорию если она не существует."""
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при создании директории {directory}: {e}")
            raise
    
    def create_forum_file(self, forum_title: str, forum_url: str, 
                         output_dir: str = Config.OUTPUT_DIR) -> Optional[Path]:
        """Создает файл форума и записывает заголовок."""
        try:
            output_path = Path(output_dir)
            self.ensure_directory_exists(output_path)
            
            safe_filename = self.create_safe_filename(forum_title)
            filepath = output_path / f"{safe_filename}.md"
            
            with filepath.open('w', encoding='utf-8', buffering=self._buffer_size) as f:
                f.write(f"# {forum_title}\n{forum_url}\n\n")
            
            self.logger.debug(lambda: f"Создан файл: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при создании файла: {e}")
            return None
    
    def write_topic_header(self, filepath: Path, topic_title: str, 
                          topic_url: str, add_spacing: bool = True) -> None:
        """Записывает заголовок темы в файл."""
        try:
            with filepath.open('a', encoding='utf-8', buffering=self._buffer_size) as f:
                if add_spacing:
                    f.write("\n\n\n")
                f.write(f"## {topic_title}\n{topic_url}\n\n")
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при записи заголовка темы: {e}")
            raise
    
    def write_post(self, filepath: Path, post) -> None:
        """Записывает пост в файл."""
        try:
            with filepath.open('a', encoding='utf-8', buffering=self._buffer_size) as f:
                f.write(f"### {post.author}, {post.date}\n\n{post.content_markdown}\n\n\n")
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при записи поста: {e}")
            raise
    
    def save_topic_to_markdown(self, topic_title: str, forum_title: str, 
                              posts, forum_url: str = "", 
                              topic_url: str = "", 
                              output_dir: str = Config.TOPICS_OUTPUT_DIR) -> None:
        """Сохраняет тему в отдельный файл markdown."""
        try:
            output_path = Path(output_dir)
            self.ensure_directory_exists(output_path)
            
            safe_filename = self.create_safe_filename(topic_title)
            filepath = output_path / f"{safe_filename}.md"
            
            with filepath.open('w', encoding='utf-8', buffering=self._buffer_size) as f:
                f.write(f"# {forum_title}\n")
                if forum_url:
                    f.write(f"{forum_url}\n")
                f.write(f"\n\n\n## {topic_title}\n")
                if topic_url:
                    f.write(f"{topic_url}\n")
                f.write("\n")
                
                for post in posts:
                    f.write(f"### {post.author}, {post.date}\n\n{post.content_markdown}\n\n\n")
            
            self.logger.info(lambda: f"Тема сохранена в файл: {filepath}")
            
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при сохранении темы: {e}")
            raise


def sleep_with_jitter(base_delay: float, jitter_factor: float = 0.1) -> None:
    """Засыпает на случайное время с базовой задержкой и джиттером."""
    jitter = random.uniform(-jitter_factor, jitter_factor)
    actual_delay = base_delay * (1 + jitter)
    time.sleep(max(0, actual_delay))


# Backward compatibility
Logger = EnhancedLogger
HttpClient = OptimizedHttpClient
MarkdownConverter = OptimizedMarkdownConverter
FileWriter = OptimizedFileWriter 