"""
Конфигурация для парсера форума Винского.
"""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class LogLevel(Enum):
    """Уровни логирования."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class RetryStrategy(Enum):
    """Стратегии повторных попыток."""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    FIXED_DELAY = "fixed_delay"
    LINEAR_BACKOFF = "linear_backoff"


class CacheStrategy(Enum):
    """Стратегии кэширования."""
    MEMORY = "memory"
    DISABLED = "disabled"


@dataclass(frozen=True)
class Config:
    """Конфигурация приложения."""
    
    # Основные URL и настройки сети
    BASE_URL: str = "https://forum.awd.ru/"
    USER_AGENT: str = (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    )
    REQUEST_TIMEOUT: int = 10
    
    # Лимиты парсинга
    MAX_PAGES: int = 50
    POSTS_PER_PAGE: int = 15
    TOPICS_PER_PAGE: int = 131
    
    # Задержки между запросами (в секундах)
    DELAY_BETWEEN_REQUESTS: float = 1.0
    DELAY_BETWEEN_TOPICS: float = 2.0
    
    # Директории для сохранения
    OUTPUT_DIR: str = "parsed_forums"
    TOPICS_OUTPUT_DIR: str = "parsed_topics"
    
    # Настройки файлов
    MAX_FILENAME_LENGTH: int = 100
    
    # Настройки логирования
    LOG_LEVEL: str = LogLevel.INFO.value
    LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_CONTEXT_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - [%(operation)s] %(message)s'
    
    # Настройки retry механизма
    RETRY_MAX_ATTEMPTS: int = 3
    RETRY_STRATEGY: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    RETRY_BASE_DELAY: float = 1.0
    RETRY_MAX_DELAY: float = 30.0
    RETRY_JITTER: bool = True
    RETRY_BACKOFF_FACTOR: float = 2.0
    
    # Настройки кэширования
    CACHE_STRATEGY: CacheStrategy = CacheStrategy.MEMORY
    CACHE_TTL: float = 300.0  # 5 минут
    CACHE_MAX_SIZE: int = 1000
    ENABLE_URL_CACHE: bool = True
    ENABLE_PARSING_CACHE: bool = True
    
    # Настройки connection pooling
    CONNECTION_POOL_SIZE: int = 10
    CONNECTION_POOL_MAXSIZE: int = 20
    CONNECTION_POOL_BLOCK: bool = False
    KEEP_ALIVE_TIMEOUT: int = 30
    
    # Настройки производительности
    ENABLE_COMPRESSION: bool = True
    CHUNK_SIZE: int = 8192
    BUFFER_SIZE: int = 65536
    CONCURRENT_REQUESTS: int = 1  # Пока оставляем 1, чтобы не перегружать сервер
    
    # Настройки HTML парсинга
    HTML_PARSER: str = "html.parser"  # Можно изменить на "lxml" для скорости
    ENABLE_HTML_CACHE: bool = True
    PRECOMPILE_REGEX: bool = True
    
    # Настройки обработки ошибок
    ENABLE_ERROR_RECOVERY: bool = True
    SKIP_FAILED_PAGES: bool = True
    SKIP_FAILED_TOPICS: bool = True
    SKIP_FAILED_POSTS: bool = True
    MAX_CONSECUTIVE_FAILURES: int = 5
    
    # Настройки метрик
    ENABLE_PERFORMANCE_METRICS: bool = True
    LOG_METRICS_INTERVAL: int = 100  # Логировать метрики каждые N операций
    
    @classmethod
    def get_output_path(cls) -> Path:
        """Возвращает путь к директории вывода как Path объект."""
        return Path(cls.OUTPUT_DIR)
    
    @classmethod  
    def get_topics_output_path(cls) -> Path:
        """Возвращает путь к директории тем как Path объект."""
        return Path(cls.TOPICS_OUTPUT_DIR)
    
    @classmethod
    def get_retry_config(cls):
        """Возвращает конфигурацию retry механизма."""
        # Импортируем только при необходимости, чтобы избежать циклических импортов
        from models import RetryConfig
        return RetryConfig(
            max_attempts=cls.RETRY_MAX_ATTEMPTS,
            strategy=cls.RETRY_STRATEGY,
            base_delay=cls.RETRY_BASE_DELAY,
            max_delay=cls.RETRY_MAX_DELAY,
            jitter=cls.RETRY_JITTER,
            backoff_factor=cls.RETRY_BACKOFF_FACTOR
        )
    
    @classmethod
    def get_http_adapter_config(cls) -> dict:
        """Возвращает конфигурацию для HTTP адаптера."""
        return {
            'pool_connections': cls.CONNECTION_POOL_SIZE,
            'pool_maxsize': cls.CONNECTION_POOL_MAXSIZE,
            'pool_block': cls.CONNECTION_POOL_BLOCK,
        } 