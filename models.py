"""
Модели данных и исключения для парсера форума Винского.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Callable
from enum import Enum
import time


# Custom Exceptions
class ForumParsingError(Exception):
    """Базовое исключение для ошибок парсинга форума."""
    pass


class PageNotFoundError(ForumParsingError):
    """Исключение для случаев, когда страница не найдена."""
    pass


class InvalidUrlError(ForumParsingError):
    """Исключение для некорректных URL."""
    pass


class ContentExtractionError(ForumParsingError):
    """Исключение для ошибок извлечения контента."""
    pass


class RetryableError(ForumParsingError):
    """Исключение для ошибок, которые можно повторить."""
    pass


class NonRetryableError(ForumParsingError):
    """Исключение для ошибок, которые нельзя повторить."""
    pass


# Enums
class LogLevel(Enum):
    """Уровни логирования."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


# Data Models
@dataclass
class Post:
    """Модель поста форума."""
    author: str
    date: str
    content_html: str
    content_markdown: str
    post_url: str


@dataclass
class Topic:
    """Модель темы форума."""
    title: str
    url: str
    posts: List[Post]


@dataclass
class ForumData:
    """Модель данных форума."""
    title: str
    url: str
    topics: List[Topic]
    total_posts: int


@dataclass
class ParseResult:
    """Результат парсинга форума."""
    forum_title: str
    forum_url: str
    total_topics: int
    total_posts: int
    filepath: str
    success: bool = True
    error_message: str = ""
    processing_time: float = 0.0
    cached_hits: int = 0


# Enhanced Models for Error Handling and Performance
@dataclass
class RetryConfig:
    """Конфигурация для retry механизма."""
    max_attempts: int = 3
    strategy: str = "exponential_backoff"  # Используем строку вместо enum для избежания циклических импортов
    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter: bool = True
    backoff_factor: float = 2.0
    retryable_exceptions: tuple = (
        PageNotFoundError, 
        ConnectionError, 
        TimeoutError,
        RetryableError
    )


@dataclass
class LogContext:
    """Контекст для логирования."""
    operation: str
    url: Optional[str] = None
    page_num: Optional[int] = None
    topic_title: Optional[str] = None
    additional_data: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует контекст в словарь для логирования."""
        context = {
            'operation': self.operation,
            **self.additional_data
        }
        if self.url:
            context['url'] = self.url
        if self.page_num:
            context['page_num'] = self.page_num
        if self.topic_title:
            context['topic_title'] = self.topic_title
        return context


@dataclass
class CacheEntry:
    """Запись в кэше."""
    data: Any
    timestamp: float
    ttl: float = 300.0  # 5 минут по умолчанию
    
    def is_expired(self) -> bool:
        """Проверяет, истек ли срок действия записи."""
        return time.time() - self.timestamp > self.ttl


@dataclass
class PerformanceMetrics:
    """Метрики производительности."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    cached_requests: int = 0
    total_processing_time: float = 0.0
    average_request_time: float = 0.0
    retry_attempts: int = 0
    
    def add_request(self, success: bool, processing_time: float, from_cache: bool = False):
        """Добавляет метрику запроса."""
        self.total_requests += 1
        self.total_processing_time += processing_time
        
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            
        if from_cache:
            self.cached_requests += 1
            
        self.average_request_time = self.total_processing_time / self.total_requests
    
    def add_retry(self):
        """Добавляет метрику повторной попытки."""
        self.retry_attempts += 1


@dataclass
class ErrorContext:
    """Контекст ошибки для централизованной обработки."""
    exception: Exception
    operation: str
    url: Optional[str] = None
    retry_count: int = 0
    recoverable: bool = True
    context_data: Dict[str, Any] = field(default_factory=dict) 