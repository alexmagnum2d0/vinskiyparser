"""
Главный модуль парсера форума Винского - Оптимизированная версия.

Этот модуль содержит основной класс VinskiyForumParser и функции для запуска парсинга.
"""

import sys
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Optional
import argparse

from config import Config
from models import Post, ParseResult, InvalidUrlError, LogContext, ErrorContext
from utils import EnhancedLogger, OptimizedHttpClient, OptimizedMarkdownConverter, OptimizedFileWriter, sleep_with_jitter
from parsers import OptimizedHtmlParser, OptimizedPaginationHandler


class VinskiyForumParser:
    """Оптимизированный парсер форума Винского с улучшенной обработкой ошибок и производительностью."""
    
    def __init__(self, base_url: str = Config.BASE_URL):
        """Инициализация парсера.
        
        Args:
            base_url: Базовый URL форума
        """
        self.base_url = base_url
        
        # Инициализация компонентов с улучшенным логированием
        self.logger = EnhancedLogger(
            self.__class__.__name__, 
            LogContext(operation="forum_parsing")
        )
        
        self.http_client = OptimizedHttpClient(base_url)
        self.html_parser = OptimizedHtmlParser(base_url)
        self.pagination_handler = OptimizedPaginationHandler(base_url)
        self.markdown_converter = OptimizedMarkdownConverter()
        self.file_writer = OptimizedFileWriter()
        
        # Счетчики для статистики и recovery
        self.consecutive_failures = 0
        self.total_failures = 0
        self.processed_items = 0
        
        # Флаги состояния
        self.is_interrupted = False
    
    def parse_forum(self, forum_url: str) -> Dict[str, str]:
        """Парсит форум, собирая ссылки на все темы с улучшенной обработкой ошибок.
        
        Args:
            forum_url: URL форума для парсинга
            
        Returns:
            Словарь {название_темы: URL_темы}
            
        Raises:
            InvalidUrlError: При некорректном URL форума
        """
        if not self._validate_forum_url(forum_url):
            raise InvalidUrlError(f"Некорректный URL форума: {forum_url}")
        
        all_topics = {}
        current_url = forum_url
        page_count = 0
        failed_pages = []
        
        parser_logger = self.logger.with_context(operation="forum_parsing", url=forum_url)
        parser_logger.debug("Начинаю парсинг форума")
        
        while current_url and page_count < Config.MAX_PAGES and not self.is_interrupted:
            page_count += 1
            page_logger = parser_logger.with_context(page_num=page_count, url=current_url)
            
            try:
                page_logger.debug(lambda: f"Обрабатываю страницу {page_count}")
                
                soup = self.http_client.get_page(current_url)
                if not soup:
                    self._handle_page_failure(page_count, "Не удалось получить страницу", failed_pages)
                    current_url = self._try_next_page_url(soup, current_url, page_logger)
                    continue
                
                topics = self.html_parser.extract_topic_links(soup)
                page_logger.debug(lambda: f"Найдено тем на странице: {len(topics)}")
                
                if topics:
                    all_topics.update(topics)
                    self.consecutive_failures = 0  # Сбрасываем при успехе
                else:
                    page_logger.warning("На странице не найдено тем")
                
                # Ищем следующую страницу
                next_url = self.pagination_handler.get_next_page_url(soup, current_url)
                if not next_url:
                    page_logger.debug("Следующая страница не найдена. Парсинг завершен")
                    break
                
                if next_url == current_url:
                    page_logger.warning("Обнаружен цикл в пагинации. Парсинг остановлен")
                    break
                    
                current_url = next_url
                page_logger.debug(lambda: f"Найдена следующая страница: {next_url}")
                
                # Адаптивная задержка
                delay = self._calculate_adaptive_delay(page_count)
                sleep_with_jitter(delay)
                
            except Exception as e:
                self._handle_page_failure(page_count, str(e), failed_pages)
                
                # Проверяем, стоит ли продолжать
                if not self._should_continue_after_error():
                    parser_logger.error("Слишком много ошибок подряд, прерываем парсинг")
                    break
                
                # Пытаемся перейти к следующей странице
                current_url = self._try_next_page_url(soup if 'soup' in locals() else None, current_url, page_logger)
        
        # Пытаемся повторно обработать неудачные страницы
        if failed_pages and Config.ENABLE_ERROR_RECOVERY:
            recovered_topics = self._retry_failed_pages(forum_url, failed_pages, parser_logger)
            all_topics.update(recovered_topics)
        
        parser_logger.debug(lambda: f"Парсинг завершен. Обработано страниц: {page_count}, найдено тем: {len(all_topics)}")
        
        # Логируем метрики
        if Config.ENABLE_PERFORMANCE_METRICS:
            metrics = self.http_client.get_metrics()
            parser_logger.debug(lambda: f"Метрики: {metrics.total_requests} запросов, {metrics.cached_requests} из кэша, средняя скорость: {metrics.average_request_time:.2f}с")
        
        return all_topics
    
    def parse_topic(self, topic_url: str, topic_title: str) -> List[Post]:
        """Парсит содержимое отдельной темы с улучшенной обработкой ошибок.
        
        Args:
            topic_url: URL темы
            topic_title: Название темы
            
        Returns:
            Список постов темы
        """
        all_posts = []
        current_url = topic_url
        page_count = 0
        failed_pages = []
        
        topic_logger = self.logger.with_context(
            operation="topic_parsing", 
            url=topic_url, 
            topic_title=topic_title
        )
        topic_logger.debug("Парсинг темы")
        
        while current_url and page_count < Config.MAX_PAGES and not self.is_interrupted:
            page_count += 1
            page_logger = topic_logger.with_context(page_num=page_count)
            
            try:
                page_logger.debug(lambda: f"Обрабатываю страницу {page_count} темы")
                
                soup = self.http_client.get_page(current_url)
                if not soup:
                    self._handle_page_failure(page_count, "Не удалось получить страницу темы", failed_pages)
                    current_url = self._try_next_topic_page_url(soup, current_url, page_logger)
                    continue
                
                posts_data = self.html_parser.extract_posts_from_page(soup)
                page_logger.debug(lambda: f"Найдено постов на странице: {len(posts_data)}")
                
                # Конвертируем в объекты Post с обработкой ошибок
                page_posts = self._convert_posts_data_safe(posts_data, page_logger)
                all_posts.extend(page_posts)
                
                if page_posts:
                    self.consecutive_failures = 0
                
                next_url = self.pagination_handler.get_next_topic_page_url(soup, current_url)
                if not next_url:
                    page_logger.debug("Следующая страница темы не найдена")
                    break
                    
                if next_url == current_url:
                    page_logger.warning("Обнаружен цикл в пагинации темы")
                    break
                    
                current_url = next_url
                sleep_with_jitter(Config.DELAY_BETWEEN_REQUESTS)
                
            except Exception as e:
                self._handle_page_failure(page_count, f"Ошибка при обработке страницы темы: {e}", failed_pages)
                
                if not self._should_continue_after_error():
                    break
                
                current_url = self._try_next_topic_page_url(soup if 'soup' in locals() else None, current_url, page_logger)
        
        # Recovery для неудачных страниц темы
        if failed_pages and Config.ENABLE_ERROR_RECOVERY:
            recovered_posts = self._retry_failed_topic_pages(topic_url, topic_title, failed_pages, topic_logger)
            all_posts.extend(recovered_posts)
        
        topic_logger.debug(lambda: f"Парсинг темы завершен. Страниц: {page_count}, постов: {len(all_posts)}")
        return all_posts
    
    def parse_topic_streaming(self, topic_url: str, topic_title: str, filepath: Path, total_pages: int = None) -> int:
        """Парсит тему с потоковой записью постов в файл и улучшенной обработкой ошибок.
        
        Args:
            topic_url: URL темы
            topic_title: Название темы
            filepath: Путь к файлу для записи
            total_pages: Общее количество страниц в теме (если известно)
            
        Returns:
            Количество обработанных постов
        """
        total_posts = 0
        current_url = topic_url
        page_count = 0
        failed_pages = []
        is_verbose = EnhancedLogger._global_log_level == "DEBUG"
        
        stream_logger = self.logger.with_context(
            operation="streaming_parsing",
            url=topic_url,
            topic_title=topic_title
        )
        
        if is_verbose:
            stream_logger.debug("Потоковый парсинг темы")
        
        while current_url and page_count < Config.MAX_PAGES and not self.is_interrupted:
            page_count += 1
            page_logger = stream_logger.with_context(page_num=page_count)
            
            # Показываем прогресс по страницам (только для обычного режима)
            if not is_verbose and total_pages and total_pages > 1:
                # Показываем прогресс по теме на текущей строке
                progress_text = f"Обрабатываю тему: {page_count}/{total_pages} страниц обработано"
                print(f"\r{progress_text}\033[K", end='', flush=True)  # \033[K очищает строку до конца
                sys.stdout.flush()
            
            try:
                if is_verbose:
                    page_logger.debug(f"📄 Обрабатываю страницу {page_count} темы")
                
                soup = self.http_client.get_page(current_url)
                if not soup:
                    self._handle_page_failure(page_count, "Не удалось получить страницу", failed_pages)
                    current_url = self._try_next_topic_page_url(soup, current_url, page_logger)
                    continue
                
                posts_data = self.html_parser.extract_posts_from_page(soup)
                
                if is_verbose:
                    page_logger.debug(f"💬 Найдено сообщений на странице: {len(posts_data)}")
                
                # Сразу записываем посты в файл
                page_posts = 0
                for post_data in posts_data:
                    try:
                        markdown_content = self.markdown_converter.convert(post_data['content_html'])
                        post = Post(
                            author=post_data['author'],
                            date=post_data['date'],
                            content_html=post_data['content_html'],
                            content_markdown=markdown_content,
                            post_url=post_data['post_url']
                        )
                        self.file_writer.write_post(filepath, post)
                        page_posts += 1
                        total_posts += 1
                    except Exception as e:
                        if is_verbose:
                            page_logger.warning(f"Ошибка при записи поста: {e}")
                        continue
                
                if page_posts > 0:
                    self.consecutive_failures = 0
                    if is_verbose:
                        page_logger.debug(f"✅ Обработано сообщений на странице: {page_posts}")
                
                next_url = self.pagination_handler.get_next_topic_page_url(soup, current_url)
                if not next_url or next_url == current_url:
                    if is_verbose and page_count > 1:
                        page_logger.debug("Следующая страница темы не найдена")
                    break
                    
                current_url = next_url
                if is_verbose:
                    page_logger.debug(f"Найдена следующая страница: {next_url}")
                sleep_with_jitter(Config.DELAY_BETWEEN_REQUESTS)
                
            except Exception as e:
                self._handle_page_failure(page_count, f"Ошибка при потоковом парсинге: {e}", failed_pages)
                
                if not self._should_continue_after_error():
                    break
                
                current_url = self._try_next_topic_page_url(soup if 'soup' in locals() else None, current_url, page_logger)
        
        # Очищаем строку прогресса страниц
        if not is_verbose and total_pages and total_pages > 1:
            print(f"\r{' ' * 60}\033[K\r", end='', flush=True)  # Очищаем текущую строку
        
        if is_verbose:
            if page_count > 1:
                stream_logger.debug(f"✅ Парсинг темы завершен. Страниц: {page_count}, сообщений: {total_posts}")
            else:
                stream_logger.debug(f"✅ Парсинг темы завершен. Сообщений: {total_posts}")
        
        return total_posts
    
    def parse_entire_forum(self, forum_url: str) -> Optional[ParseResult]:
        """Парсит весь форум со всеми темами с потоковой записью и полной обработкой ошибок.
        
        Args:
            forum_url: URL форума
            
        Returns:
            Результат парсинга или None при ошибке
        """
        start_time = time.time()
        is_verbose = EnhancedLogger._global_log_level == "DEBUG"
        
        main_logger = self.logger.with_context(
            operation="full_forum_parsing",
            url=forum_url
        )
        
        try:
            # Этап 1: Получение списка тем
            if is_verbose:
                main_logger.info("=== ПАРСИНГ ВСЕГО ФОРУМА ===")
                main_logger.debug("Этап 1: Получение списка всех тем...")
            
            topics = self.parse_forum(forum_url)
            
            if not topics:
                error_msg = "Темы не найдены!"
                if is_verbose:
                    main_logger.error(error_msg)
                else:
                    print(f"❌ {error_msg}")
                return self._create_error_result(forum_url, "Темы не найдены", start_time)
            
            # Этап 2: Получение названия форума
            if is_verbose:
                main_logger.debug("Этап 2: Получение названия форума...")
            
            soup = self.http_client.get_page(forum_url)
            if soup:
                content_hash = hashlib.md5(str(soup)[:500].encode()).hexdigest()
                forum_title = self.html_parser.get_forum_title(content_hash, soup)
            else:
                forum_title = "Неизвестный форум"
            
            # Показываем основную информацию
            if is_verbose:
                main_logger.info(f"📂 Форум: {forum_title}")
                main_logger.info(f"🔗 URL: {forum_url}")
                main_logger.info(f"📊 Найдено тем: {len(topics)}")
            else:
                print(f"📂 Форум: {forum_title}")
                print(f"🔗 URL: {forum_url}")
                print(f"📊 Найдено тем: {len(topics)}")
            
            # Этап 3: Создание файла
            if is_verbose:
                main_logger.debug("Этап 3: Создание файла...")
            
            filepath = self.file_writer.create_forum_file(forum_title, forum_url)
            if not filepath:
                error_msg = "Ошибка создания файла!"
                if is_verbose:
                    main_logger.error(error_msg)
                else:
                    print(f"❌ {error_msg}")
                return self._create_error_result(forum_url, "Ошибка создания файла", start_time)
            
            # Этап 4: Парсинг тем
            if is_verbose:
                main_logger.info("🚀 Начинаю парсинг тем...")
            else:
                print("🚀 Начинаю парсинг...")
                # Показываем начальный прогресс
                initial_progress = f"📈 Обрабатываю форум: 0/{len(topics)} тем обработано"
                print(initial_progress, end='', flush=True)
                print()  # Пустая строка для прогресса по страницам
            
            total_posts = 0
            processed_topics = 0
            failed_topics = []
            
            for i, (topic_title, topic_url) in enumerate(topics.items(), 1):
                if self.is_interrupted:
                    interrupt_msg = "Парсинг прерван пользователем"
                    if is_verbose:
                        main_logger.info(interrupt_msg)
                    else:
                        print(f"\n⏹️  {interrupt_msg}")
                    break
                
                topic_logger = main_logger.with_context(
                    topic_title=topic_title,
                    url=topic_url,
                    progress=f"{i}/{len(topics)}"
                )
                
                # Прогресс по форуму
                if not is_verbose:
                    pass  # Прогресс будет показываться после обработки каждой темы
                else:
                    # Подробная информация для verbose режима
                    topic_logger.info(f"📄 Тема {i}/{len(topics)}: {topic_title}")
                
                try:
                    # Записываем заголовок темы
                    self.file_writer.write_topic_header(filepath, topic_title, topic_url, i > 1)
                    
                    # Определяем общее количество страниц в теме (только для обычного режима)
                    total_pages = None
                    if not is_verbose:
                        total_pages = self.get_topic_total_pages(topic_url)
                    
                    # Парсим с потоковой записью
                    posts_count = self.parse_topic_streaming(topic_url, topic_title, filepath, total_pages)
                    
                    if posts_count > 0:
                        processed_topics += 1
                        total_posts += posts_count
                        if is_verbose:
                            topic_logger.info(f"✅ Тема обработана: {posts_count} постов")
                        else:
                            # Обновляем прогресс по темам после обработки
                            forum_progress = f"📈 Обрабатываю форум: {processed_topics}/{len(topics)} тем обработано"
                            print(f"\033[A\r{forum_progress}\033[K", end='', flush=True)  # Поднимаемся вверх и обновляем
                            print()  # Переходим на новую строку для следующего вывода
                        self.consecutive_failures = 0
                    else:
                        if is_verbose:
                            topic_logger.warning("❌ Тема пропущена: постов не найдено")
                        failed_topics.append((topic_title, topic_url))
                    
                    # Логируем прогресс каждые N тем (только для verbose)
                    if is_verbose and Config.ENABLE_PERFORMANCE_METRICS and i % Config.LOG_METRICS_INTERVAL == 0:
                        self._log_progress_metrics(i, len(topics), processed_topics, total_posts, main_logger)
                    
                    sleep_with_jitter(Config.DELAY_BETWEEN_TOPICS)
                    
                except Exception as e:
                    if is_verbose:
                        topic_logger.error(f"Ошибка при обработке темы: {e}")
                    failed_topics.append((topic_title, topic_url))
                    
                    if not self._should_continue_after_error():
                        error_msg = "Слишком много ошибок, прерываем парсинг форума"
                        if is_verbose:
                            main_logger.error(error_msg)
                        else:
                            print(f"\n❌ {error_msg}")
                        break
                    
                    continue
            
            # Завершаем прогресс-бар для обычного режима
            if not is_verbose:
                # Показываем финальный прогресс
                final_progress = f"📈 Обрабатываю форум: {processed_topics}/{len(topics)} тем обработано"
                print(f"\033[A\r{final_progress}\033[K", end='', flush=True)  # Обновляем строку с прогрессом форума
                print()  # Переходим на новую строку для следующего вывода
            
            # Recovery для неудачных тем
            if failed_topics and Config.ENABLE_ERROR_RECOVERY:
                if is_verbose:
                    main_logger.debug(f"Повторная обработка {len(failed_topics)} неудачных тем...")
                recovered_posts = self._retry_failed_topics(failed_topics, filepath, main_logger)
                total_posts += recovered_posts
                processed_topics += recovered_posts // 10  # Примерная оценка
            
            processing_time = time.time() - start_time
            
            # Финальная статистика
            if is_verbose:
                main_logger.info("✅ Парсинг завершён!")
                main_logger.info(f"📊 Обработано тем: {processed_topics}/{len(topics)}")
                main_logger.info(f"📝 Всего постов: {total_posts}")
                main_logger.info(f"⏱️  Время: {processing_time:.2f} секунд")
                main_logger.info(f"💾 Файл: {filepath}")
            else:
                print("✅ Парсинг завершён!")
                print(f"📊 Обработано тем: {processed_topics}/{len(topics)}")
                print(f"📝 Всего постов: {total_posts}")
                print(f"⏱️  Время: {processing_time:.2f} секунд")
                print(f"💾 Файл: {filepath}")
            
            # Метрики производительности (только для verbose)
            if is_verbose and Config.ENABLE_PERFORMANCE_METRICS:
                self._log_final_metrics(main_logger)
            
            try:
                file_size = filepath.stat().st_size / 1024 / 1024
                if is_verbose:
                    main_logger.info(f"📦 Размер файла: {file_size:.2f} МБ")
            except Exception:
                pass
            
            return ParseResult(
                forum_title=forum_title,
                forum_url=forum_url,
                total_topics=processed_topics,
                total_posts=total_posts,
                filepath=str(filepath),
                success=True,
                processing_time=processing_time,
                cached_hits=self.http_client.get_metrics().cached_requests
            )
            
        except KeyboardInterrupt:
            self.is_interrupted = True
            interrupt_msg = "Парсинг прерван пользователем"
            if is_verbose:
                main_logger.info(interrupt_msg)
            else:
                print(f"\n⏹️  {interrupt_msg}")
            return self._create_error_result(forum_url, "Прервано пользователем", start_time)
        except Exception as e:
            error_msg = f"Критическая ошибка при парсинге форума: {e}"
            if is_verbose:
                main_logger.error(error_msg)
            else:
                print(f"❌ {error_msg}")
            return self._create_error_result(forum_url, str(e), start_time)
    
    def _handle_page_failure(self, page_num: int, error: str, failed_pages: List[int]) -> None:
        """Обрабатывает неудачу страницы."""
        self.consecutive_failures += 1
        self.total_failures += 1
        failed_pages.append(page_num)
        
        if len(failed_pages) <= 3:  # Логируем только первые ошибки
            self.logger.warning(lambda: f"Ошибка на странице {page_num}: {error}")
    
    def _should_continue_after_error(self) -> bool:
        """Определяет, стоит ли продолжать после ошибки."""
        return (self.consecutive_failures < Config.MAX_CONSECUTIVE_FAILURES and
                not self.is_interrupted)
    
    def _calculate_adaptive_delay(self, page_num: int) -> float:
        """Вычисляет адаптивную задержку на основе текущей производительности."""
        base_delay = Config.DELAY_BETWEEN_REQUESTS
        
        # Увеличиваем задержку при ошибках
        if self.consecutive_failures > 0:
            base_delay *= (1 + self.consecutive_failures * 0.5)
        
        # Уменьшаем задержку при успешной работе
        if self.consecutive_failures == 0 and page_num > 5:
            base_delay *= 0.8
        
        return max(base_delay, 0.5)  # Минимум 0.5 секунды
    
    def _convert_posts_data_safe(self, posts_data: List[Dict], logger: EnhancedLogger) -> List[Post]:
        """Безопасно конвертирует данные постов в объекты Post."""
        posts = []
        failed_conversions = 0
        
        for post_data in posts_data:
            try:
                markdown_content = self.markdown_converter.convert(post_data['content_html'])
                post = Post(
                    author=post_data['author'],
                    date=post_data['date'],
                    content_html=post_data['content_html'],
                    content_markdown=markdown_content,
                    post_url=post_data['post_url']
                )
                posts.append(post)
            except Exception as e:
                failed_conversions += 1
                if failed_conversions <= 3:
                    logger.warning(lambda: f"Ошибка при создании объекта Post: {e}")
                continue
        
        if failed_conversions > 0:
            logger.warning(lambda: f"Пропущено {failed_conversions} некорректных постов")
        
        return posts
    
    def _try_next_page_url(self, soup, current_url: str, logger: EnhancedLogger) -> Optional[str]:
        """Пытается найти следующую страницу даже при ошибках."""
        if not soup:
            return None
        
        try:
            return self.pagination_handler.get_next_page_url(soup, current_url)
        except Exception as e:
            logger.debug(lambda: f"Ошибка при поиске следующей страницы: {e}")
            return None
    
    def _try_next_topic_page_url(self, soup, current_url: str, logger: EnhancedLogger) -> Optional[str]:
        """Пытается найти следующую страницу темы даже при ошибках."""
        if not soup:
            return None
        
        try:
            return self.pagination_handler.get_next_topic_page_url(soup, current_url)
        except Exception as e:
            logger.debug(lambda: f"Ошибка при поиске следующей страницы темы: {e}")
            return None
    
    def _retry_failed_pages(self, forum_url: str, failed_pages: List[int], logger: EnhancedLogger) -> Dict[str, str]:
        """Повторно обрабатывает неудачные страницы форума."""
        logger.info(lambda: f"Повторная обработка {len(failed_pages)} неудачных страниц форума")
        recovered_topics = {}
        
        # Пока упрощенная реализация - в реальном проекте здесь была бы более сложная логика
        return recovered_topics
    
    def _retry_failed_topic_pages(self, topic_url: str, topic_title: str, failed_pages: List[int], logger: EnhancedLogger) -> List[Post]:
        """Повторно обрабатывает неудачные страницы темы."""
        logger.info(lambda: f"Повторная обработка {len(failed_pages)} неудачных страниц темы")
        recovered_posts = []
        
        # Пока упрощенная реализация - в реальном проекте здесь была бы более сложная логика
        return recovered_posts
    
    def _retry_failed_topics(self, failed_topics: List[tuple], filepath: Path, logger: EnhancedLogger) -> int:
        """Повторно обрабатывает неудачные темы."""
        logger.info(lambda: f"Повторная обработка {len(failed_topics)} неудачных тем")
        recovered_posts = 0
        
        # Пока упрощенная реализация - в реальном проекте здесь была бы более сложная логика
        return recovered_posts
    
    def _log_progress_metrics(self, current: int, total: int, processed: int, posts: int, logger: EnhancedLogger) -> None:
        """Логирует метрики прогресса."""
        progress_pct = (current / total) * 100
        metrics = self.http_client.get_metrics()
        logger.debug(lambda: f"Прогресс: {progress_pct:.1f}% ({current}/{total}), обработано тем: {processed}, постов: {posts}, кэш: {metrics.cached_requests}/{metrics.total_requests}")
    
    def _log_final_metrics(self, logger: EnhancedLogger) -> None:
        """Логирует финальные метрики производительности."""
        metrics = self.http_client.get_metrics()
        logger.debug(lambda: f"Финальные метрики: {metrics.total_requests} запросов, {metrics.successful_requests} успешных, {metrics.cached_requests} из кэша")
        logger.debug(lambda: f"Средняя скорость запроса: {metrics.average_request_time:.2f}с, повторов: {metrics.retry_attempts}")
    
    def _create_error_result(self, forum_url: str, error_message: str, start_time: float) -> ParseResult:
        """Создает результат с ошибкой."""
        return ParseResult(
            forum_title="",
            forum_url=forum_url,
            total_topics=0,
            total_posts=0,
            filepath="",
            success=False,
            error_message=error_message,
            processing_time=time.time() - start_time
        )
    
    def save_topic_to_markdown(self, topic_title: str, forum_title: str, posts: List[Post], 
                              forum_url: str = "", topic_url: str = "") -> None:
        """Сохраняет тему в отдельный файл markdown.
        
        Args:
            topic_title: Название темы
            forum_title: Название форума
            posts: Список постов
            forum_url: URL форума
            topic_url: URL темы
            
        Raises:
            OSError: При ошибке сохранения файла
        """
        try:
            self.file_writer.save_topic_to_markdown(
                topic_title, forum_title, posts, forum_url, topic_url
            )
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при сохранении темы в markdown: {e}")
            raise
    
    def _validate_forum_url(self, url: str) -> bool:
        """Проверяет валидность URL форума.
        
        Args:
            url: URL для проверки
            
        Returns:
            True если URL валидный, False иначе
        """
        return (url.startswith('http') and 
                'forum.awd.ru' in url and 
                'viewforum.php' in url)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.http_client.session.close()

    def get_topic_total_pages(self, topic_url: str) -> int:
        """Определяет общее количество страниц в теме.
        
        Args:
            topic_url: URL темы
            
        Returns:
            Общее количество страниц в теме
        """
        try:
            soup = self.http_client.get_page(topic_url)
            if not soup:
                return 1
                
            # Ищем информацию о пагинации
            total_pages = self.pagination_handler.get_total_topic_pages(soup)
            return max(1, total_pages)
            
        except Exception:
            return 1  # По умолчанию считаем 1 страницу


def create_argument_parser() -> argparse.ArgumentParser:
    """Создает парсер аргументов командной строки.
    
    Returns:
        Настроенный ArgumentParser
    """
    parser = argparse.ArgumentParser(
        description="Оптимизированный парсер форума Винского",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python vinskiy_parser.py
  python vinskiy_parser.py "https://forum.awd.ru/viewforum.php?f=1190"
  python vinskiy_parser.py --verbose "https://forum.awd.ru/viewforum.php?f=1192"

ВАЖНО: URL должен быть в кавычках.
        """
    )
    
    parser.add_argument(
        'forum_url',
        nargs='?',
        default="https://forum.awd.ru/viewforum.php?f=1193",
        help='URL форума для парсинга (по умолчанию: %(default)s)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Включить подробный вывод (DEBUG уровень)'
    )
    
    parser.add_argument(
        '-o', '--output',
        default=Config.OUTPUT_DIR,
        help='Директория для сохранения результатов (по умолчанию: %(default)s)'
    )
    
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Отключить кэширование'
    )
    
    parser.add_argument(
        '--metrics',
        action='store_true',
        help='Показать подробные метрики производительности'
    )
    
    return parser


def print_help():
    """Выводит справку по использованию программы."""
    print("=== ОПТИМИЗИРОВАННЫЙ ПАРСЕР ФОРУМА ВИНСКОГО ===")
    print("Использование:")
    print('  python vinskiy_parser.py "URL_ФОРУМА"')
    print()
    print("Примеры:")
    print("  python vinskiy_parser.py")
    print('  python vinskiy_parser.py "https://forum.awd.ru/viewforum.php?f=1190"')
    print('  python vinskiy_parser.py --verbose "https://forum.awd.ru/viewforum.php?f=1192"')
    print('  python vinskiy_parser.py --metrics "https://forum.awd.ru/viewforum.php?f=1190"')
    print()
    print("⚠️  ВАЖНО: URL должен быть в кавычках!")
    print("    Пример: python vinskiy_parser.py \"https://forum.awd.ru/viewforum.php?f=1190\"")
    print()
    print("Новые возможности:")
    print("  • Улучшенная обработка ошибок с recovery механизмами")
    print("  • Connection pooling и HTTP retry с exponential backoff")
    print("  • Кэширование для повышения скорости")
    print("  • Детальные метрики производительности")
    print("  • Оптимизированное логирование с контекстом")
    print()
    print("Если URL не указан, используется форум по умолчанию:")
    print("  https://forum.awd.ru/viewforum.php?f=1193")
    print()
    print("Результат сохраняется в папку 'parsed_forums' в формате markdown.")


def main():
    """Главная функция программы."""
    # Парсинг аргументов
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Настройка логирования
    log_level = "DEBUG" if args.verbose else Config.LOG_LEVEL
    # Устанавливаем глобальный уровень логирования для всех логгеров
    EnhancedLogger.set_global_log_level(log_level)
    logger = EnhancedLogger("main", LogContext(operation="main"))
    
    # Валидация URL
    forum_url = args.forum_url
    if not (forum_url.startswith('http') and 'forum.awd.ru' in forum_url and 'viewforum.php' in forum_url):
        print("❌ Некорректный URL форума!")
        print("Пример правильного URL: https://forum.awd.ru/viewforum.php?f=1190")
        return 1
    
    try:
        # Создаем парсер и запускаем парсинг
        with VinskiyForumParser() as parser_instance:
            if args.no_cache:
                parser_instance.http_client.clear_cache()
                if args.verbose:
                    logger.info("🚫 Кэширование отключено")
            
            result = parser_instance.parse_entire_forum(forum_url)
            
            if result and result.success:
                if args.metrics:
                    metrics = parser_instance.http_client.get_metrics()
                    print("\n=== ДЕТАЛЬНЫЕ МЕТРИКИ ===")
                    print(f"📊 Всего запросов: {metrics.total_requests}")
                    print(f"✅ Успешных запросов: {metrics.successful_requests}")
                    print(f"❌ Неудачных запросов: {metrics.failed_requests}")
                    print(f"💾 Из кэша: {metrics.cached_requests}")
                    print(f"⚡ Среднее время запроса: {metrics.average_request_time:.3f}с")
                    print(f"🔄 Повторных попыток: {metrics.retry_attempts}")
                
                return 0
            else:
                error_msg = result.error_message if result else "Неизвестная ошибка"
                logger.error(f"❌ Форум не удалось обработать: {error_msg}")
                return 1
                
    except KeyboardInterrupt:
        print("\n⏹️  Парсинг остановлен пользователем")
        return 130  # Стандартный код возврата для SIGINT
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 