"""
Оптимизированные классы парсинга для парсера форума Винского.
"""

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from bs4 import BeautifulSoup, SoupStrainer
from functools import lru_cache

from config import Config
from utils import EnhancedLogger, MemoryCache
from models import LogContext, ContentExtractionError, RetryableError


class OptimizedHtmlParser:
    """Оптимизированный парсер HTML контента с кэшированием и улучшенной обработкой ошибок."""
    
    def __init__(self, base_url: str = Config.BASE_URL):
        """Инициализация HTML парсера.
        
        Args:
            base_url: Базовый URL сайта
        """
        self.base_url = base_url
        self.logger = EnhancedLogger(self.__class__.__name__, LogContext(operation="html_parsing"))
        
        # Кэш для парсинга (короткий TTL, так как контент может меняться)
        self.cache = MemoryCache(max_size=200, default_ttl=60.0) if Config.ENABLE_PARSING_CACHE else None
        
        # Предкомпилированные селекторы для производительности
        if Config.PRECOMPILE_REGEX:
            self._date_patterns = [
                re.compile(r'»\s*(.+?\d{4},\s*\d{2}:\d{2})'),
                re.compile(r'»\s*(\d{2}\s+\w+\s+\d{4},\s*\d{2}:\d{2})'),
                re.compile(r'»\s*(.+?)(?:\s*$)')
            ]
            self._onclick_pattern = re.compile(r"window\.open\('([^']+)'")
            self._post_bg_pattern = re.compile(r'post\s+(bg1|bg2)')
        
        # Оптимизированные SoupStrainer для быстрого парсинга
        self._topic_strainer = SoupStrainer("a", class_="topictitle")
        self._post_strainer = SoupStrainer("div", class_=re.compile(r"post"))
    
    def extract_topic_links(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Извлекает ссылки на подтемы со страницы форума с кэшированием.
        
        Args:
            soup: BeautifulSoup объект страницы
            
        Returns:
            Словарь {название_темы: URL_темы}
        """
        # Генерируем ключ кэша на основе HTML контента
        cache_key = None
        if self.cache:
            import hashlib
            page_html = str(soup)[:1000]  # Используем первые 1000 символов для ключа
            cache_key = f"topics_{hashlib.md5(page_html.encode()).hexdigest()}"
            cached_result = self.cache.get(cache_key)
            if cached_result:
                self.logger.debug("Ссылки на темы получены из кэша")
                return cached_result
        
        topics = {}
        failed_extractions = 0
        
        try:
            # Используем оптимизированный поиск
            topic_links = soup.find_all('a', class_='topictitle')
            self.logger.debug(lambda: f"Найдено ссылок на темы: {len(topic_links)}")
            
            for link in topic_links:
                try:
                    result = self._extract_single_topic_link(link)
                    if result:
                        title, url = result
                        topics[title] = url
                except Exception as e:
                    failed_extractions += 1
                    if failed_extractions <= 3:  # Логируем только первые несколько ошибок
                        self.logger.warning(lambda: f"Ошибка при обработке ссылки на тему: {e}")
                    continue
            
            if failed_extractions > 0:
                self.logger.warning(lambda: f"Пропущено {failed_extractions} некорректных ссылок на темы")
            
            # Сохраняем в кэш
            if self.cache and cache_key:
                self.cache.put(cache_key, topics, ttl=30.0)  # Короткий TTL для тем
            
            return topics
            
        except Exception as e:
            self.logger.error(lambda: f"Критическая ошибка при извлечении ссылок на темы: {e}")
            raise ContentExtractionError(f"Ошибка извлечения тем: {e}") from e
    
    def _extract_single_topic_link(self, link) -> Optional[Tuple[str, str]]:
        """Извлекает одну ссылку на тему."""
        title = link.get_text(strip=True)
        href = link.get('href')
        
        if not href or not title:
            return None
        
        # Обработка внешних ссылок с onclick
        if href.startswith('http') and 'forum.awd.ru' not in href:
            real_href = self._extract_real_href_from_onclick(link)
            if real_href:
                full_url = urljoin(self.base_url, real_href)
                return title, full_url
            else:
                return None
        else:
            # Обычная ссылка
            full_url = urljoin(self.base_url, href)
            return title, full_url
    
    def _extract_real_href_from_onclick(self, link) -> Optional[str]:
        """Извлекает реальную ссылку из onclick атрибута."""
        onclick = link.get('onclick', '')
        if onclick:
            if Config.PRECOMPILE_REGEX:
                match = self._onclick_pattern.search(onclick)
            else:
                match = re.search(r"window\.open\('([^']+)'", onclick)
            
            if match:
                real_href = match.group(1)
                real_href = real_href.replace('&amp;', '&')
                if real_href.startswith('./'):
                    real_href = real_href[2:]
                return real_href
        return None
    
    def extract_posts_from_page(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Извлекает все посты со страницы темы с улучшенной обработкой ошибок.
        
        Args:
            soup: BeautifulSoup объект страницы
            
        Returns:
            Список словарей с данными постов
        """
        posts = []
        failed_extractions = 0
        consecutive_failures = 0
        
        try:
            # Оптимизированный поиск постов
            post_blocks = soup.find_all('div', class_='post')
            if not post_blocks:
                if Config.PRECOMPILE_REGEX:
                    post_blocks = soup.find_all('div', {'class': self._post_bg_pattern})
                else:
                    post_blocks = soup.find_all('div', {'class': re.compile(r'post\s+(bg1|bg2)')})
            
            self.logger.debug(lambda: f"Найдено блоков постов: {len(post_blocks)}")
            
            for i, post_block in enumerate(post_blocks):
                post_id = post_block.get('id', f'post_{i}')
                
                # Пропускаем блоки без корректного ID
                if not post_id.startswith('p') and 'post_' not in post_id:
                    continue
                    
                try:
                    post_data = self._extract_post_data_safe(post_block)
                    if post_data:
                        posts.append(post_data)
                        consecutive_failures = 0  # Сбрасываем счетчик при успехе
                    else:
                        consecutive_failures += 1
                        
                except Exception as e:
                    failed_extractions += 1
                    consecutive_failures += 1
                    
                    if failed_extractions <= 3:  # Логируем только первые ошибки
                        self.logger.warning(lambda: f"Ошибка при извлечении поста {post_id}: {e}")
                    
                    # Прерываем при слишком многих подряд идущих ошибках
                    if consecutive_failures >= Config.MAX_CONSECUTIVE_FAILURES:
                        self.logger.error(lambda: f"Слишком много ошибок подряд ({consecutive_failures}), прерываем парсинг постов")
                        break
                    
                    continue
            
            if failed_extractions > 0:
                self.logger.warning(lambda: f"Пропущено {failed_extractions} некорректных постов")
            
            return posts
            
        except Exception as e:
            self.logger.error(lambda: f"Критическая ошибка при извлечении постов: {e}")
            if Config.ENABLE_ERROR_RECOVERY and posts:
                self.logger.info(lambda: f"Возвращаем частично извлеченные посты: {len(posts)}")
                return posts
            raise ContentExtractionError(f"Ошибка извлечения постов: {e}") from e
    
    def _extract_post_data_safe(self, post_block) -> Optional[Dict[str, str]]:
        """Безопасно извлекает данные из отдельного поста."""
        try:
            author_info = self._extract_author_and_date_optimized(post_block)
            if not author_info:
                return None
            
            content = self._extract_post_content_optimized(post_block)
            if not content:
                return None
            
            post_id = post_block.get('id', '')
            post_url = ""
            if post_id.startswith('p'):
                post_url = f"{self.base_url}viewtopic.php?p={post_id[1:]}#{post_id}"
            
            return {
                'author': author_info['author'],
                'date': author_info['date'],
                'content_html': content,
                'post_url': post_url
            }
            
        except Exception as e:
            self.logger.debug(lambda: f"Ошибка при извлечении данных поста: {e}")
            return None
    
    def _extract_author_and_date_optimized(self, post_block) -> Optional[Dict[str, str]]:
        """Оптимизированное извлечение автора и даты из поста."""
        try:
            author_block = post_block.find('p', class_='author')
            if not author_block:
                return None
            
            # Быстрое извлечение имени автора
            author_link = (
                author_block.find('a', class_='username-coloured') or
                (author_block.find('strong') and author_block.find('strong').find('a'))
            )
                    
            if not author_link:
                return None
                
            author = author_link.get_text(strip=True)
            
            # Оптимизированное извлечение даты
            author_text = author_block.get_text()
            date = self._extract_date_from_text_optimized(author_text)
            
            return {'author': author, 'date': date}
            
        except Exception as e:
            self.logger.debug(lambda: f"Ошибка при извлечении автора и даты: {e}")
            return None
    
    def _extract_date_from_text_optimized(self, text: str) -> str:
        """Оптимизированное извлечение даты из текста."""
        if Config.PRECOMPILE_REGEX:
            # Используем предкомпилированные паттерны
            for pattern in self._date_patterns:
                match = pattern.search(text)
                if match:
                    date = match.group(1).strip()
                    return re.sub(r'\s+', ' ', date)
        else:
            # Обычные паттерны
            date_patterns = [
                r'»\s*(.+?\d{4},\s*\d{2}:\d{2})',
                r'»\s*(\d{2}\s+\w+\s+\d{4},\s*\d{2}:\d{2})',
                r'»\s*(.+?)(?:\s*$)'
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, text)
                if match:
                    date = match.group(1).strip()
                    return re.sub(r'\s+', ' ', date)
        
        return "Дата не найдена"
    
    def _extract_post_content_optimized(self, post_block) -> Optional[str]:
        """Оптимизированное извлечение содержимого поста."""
        try:
            content_block = post_block.find('div', class_='content')
            if not content_block:
                return None
            return str(content_block)
        except Exception:
            return None
    
    @lru_cache(maxsize=32)
    def get_forum_title(self, page_content_hash: str, soup: BeautifulSoup) -> str:
        """Извлекает название форума со страницы с кэшированием.
        
        Args:
            page_content_hash: Хэш содержимого страницы для кэширования
            soup: BeautifulSoup объект страницы
            
        Returns:
            Название форума
        """
        try:
            # Способ 1: H2 элементы
            h2_elements = soup.find_all('h2', limit=5)  # Ограничиваем поиск
            for h2 in h2_elements:
                h2_text = h2.get_text(strip=True)
                if h2_text and h2_text.lower() not in ['сортировать по', 'темы']:
                    return h2_text
            
            # Способ 2: Навигационные крошки
            breadcrumbs = soup.find('div', class_='navbar')
            if breadcrumbs:
                links = breadcrumbs.find_all('a', limit=10)
                forum_links = [link for link in links if 'viewforum' in link.get('href', '')]
                if forum_links:
                    return forum_links[-1].get_text(strip=True)
            
            # Способ 3: Title страницы
            title_elem = soup.find('title')
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                if '•' in title_text:
                    return title_text.split('•')[0].strip()
                return title_text
            
            return "Неизвестный форум"
            
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при извлечении названия форума: {e}")
            return "Неизвестный форум"


class OptimizedPaginationHandler:
    """Оптимизированный обработчик пагинации с кэшированием."""
    
    def __init__(self, base_url: str = Config.BASE_URL):
        """Инициализация обработчика пагинации.
        
        Args:
            base_url: Базовый URL сайта
        """
        self.base_url = base_url
        self.logger = EnhancedLogger(self.__class__.__name__, LogContext(operation="pagination"))
        
        # Кэш для URL пагинации
        self.url_cache = MemoryCache(max_size=100, default_ttl=120.0) if Config.ENABLE_URL_CACHE else None
        
        # Предкомпилированные паттерны
        if Config.PRECOMPILE_REGEX:
            self._page_info_pattern = re.compile(r'Страница\s*(\d+)\s*из\s*(\d+)')
            self._start_param_pattern = re.compile(r'start=(\d+)')
    
    def get_next_page_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """Находит ссылку на следующую страницу форума с кэшированием.
        
        Args:
            soup: BeautifulSoup объект страницы
            current_url: URL текущей страницы
            
        Returns:
            URL следующей страницы или None
        """
        try:
            # Проверяем кэш
            cache_key = f"next_page_{hash(current_url)}"
            if self.url_cache:
                cached_url = self.url_cache.get(cache_key)
                if cached_url:
                    return cached_url
            
            # Способ 1: Ищем ссылку "След."
            next_url = self._find_next_link_optimized(soup)
            if next_url:
                if self.url_cache:
                    self.url_cache.put(cache_key, next_url, ttl=60.0)
                return next_url
            
            # Способ 2: Ищем по номеру в пагинации
            next_url = self._find_next_page_by_number_optimized(soup, current_url)
            if self.url_cache and next_url:
                self.url_cache.put(cache_key, next_url, ttl=60.0)
            
            return next_url
            
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при поиске следующей страницы: {e}")
            return None
    
    def _find_next_link_optimized(self, soup: BeautifulSoup) -> Optional[str]:
        """Оптимизированный поиск ссылки 'След.' на странице."""
        # Ищем только в области пагинации для скорости
        pagination = soup.find('div', class_='pagination')
        search_area = pagination if pagination else soup
        
        # Оптимизированный поиск с ограничением
        all_links = search_area.find_all('a', href=True, limit=20)
        
        for link in all_links:
            text = link.get_text(strip=True)
            href = link.get('href', '')
            
            if ((text.startswith('След') or '»' in text or 
                 text == 'Next' or text.lower() in ['следующая', 'далее', 'next']) and
                'viewforum.php' in href):
                return urljoin(self.base_url, href)
        return None
    
    def _find_next_page_by_number_optimized(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """Оптимизированный поиск следующей страницы по номеру."""
        pagination = soup.find('div', class_='pagination')
        if not pagination:
            return None
        
        current_page, total_pages = self._get_page_info_optimized(soup, current_url)
        
        if current_page >= total_pages:
            return None
            
        next_page = current_page + 1
        
        # Ограничиваем поиск в области пагинации
        pagination_links = pagination.find_all('a', href=True, limit=15)
        for link in pagination_links:
            text = link.get_text(strip=True)
            href = link.get('href', '')
            
            if (text.isdigit() and int(text) == next_page and 
                'viewforum.php' in href):
                return urljoin(self.base_url, href)
        
        return None
    
    def _get_page_info_optimized(self, soup: BeautifulSoup, current_url: str) -> Tuple[int, int]:
        """Оптимизированное определение номера текущей страницы и общего количества страниц."""
        pagination = soup.find('div', class_='pagination')
        if pagination:
            page_info = pagination.get_text()
            if Config.PRECOMPILE_REGEX:
                match = self._page_info_pattern.search(page_info)
            else:
                match = re.search(r'Страница\s*(\d+)\s*из\s*(\d+)', page_info)
            
            if match:
                return int(match.group(1)), int(match.group(2))
        
        # Из URL параметра start
        if Config.PRECOMPILE_REGEX:
            match = self._start_param_pattern.search(current_url)
        else:
            match = re.search(r'start=(\d+)', current_url)
        
        if match:
            start = int(match.group(1))
            current_page = (start // Config.TOPICS_PER_PAGE) + 1 if start > 0 else 1
            
            max_page = 1
            if pagination:
                # Быстрый поиск максимального номера страницы
                links = pagination.find_all('a', href=True, limit=10)
                for link in links:
                    text = link.get_text(strip=True)
                    if text.isdigit():
                        max_page = max(max_page, int(text))
            
            return current_page, max_page
        
        return 1, 1
    
    def get_next_topic_page_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """Находит ссылку на следующую страницу темы с оптимизацией."""
        try:
            # Сначала ищем прямую ссылку "След."
            pagination = soup.find('div', class_='pagination')
            search_area = pagination if pagination else soup
            
            all_links = search_area.find_all('a', href=True, limit=15)
            for link in all_links:
                text = link.get_text(strip=True)
                href = link.get('href', '')
                if ((text.startswith('След') or '»' in text or 
                     text == 'Next' or text.lower() in ['следующая', 'далее', 'next']) and
                    'viewtopic.php' in href):
                    return urljoin(self.base_url, href)
            
            # Ищем по номеру в пагинации
            return self._find_next_topic_page_by_number_optimized(soup, current_url)
            
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при поиске следующей страницы темы: {e}")
            return None
    
    def _find_next_topic_page_by_number_optimized(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """Оптимизированный поиск следующей страницы темы по номеру."""
        pagination = soup.find('div', class_='pagination')
        if not pagination:
            return None
        
        current_page, total_pages = self._get_topic_page_info_optimized(soup, current_url)
        
        if current_page >= total_pages:
            return None
            
        next_page = current_page + 1
        
        pagination_links = pagination.find_all('a', href=True, limit=10)
        for link in pagination_links:
            text = link.get_text(strip=True)
            href = link.get('href', '')
            
            if (text.isdigit() and int(text) == next_page and 
                'viewtopic.php' in href):
                return urljoin(self.base_url, href)
        
        return None
    
    def _get_topic_page_info_optimized(self, soup: BeautifulSoup, current_url: str) -> Tuple[int, int]:
        """Оптимизированное определение информации о странице темы."""
        pagination = soup.find('div', class_='pagination')
        if pagination:
            page_info = pagination.get_text()
            if Config.PRECOMPILE_REGEX:
                match = self._page_info_pattern.search(page_info)
            else:
                match = re.search(r'Страница\s*(\d+)\s*из\s*(\d+)', page_info)
            
            if match:
                return int(match.group(1)), int(match.group(2))
        
        if Config.PRECOMPILE_REGEX:
            match = self._start_param_pattern.search(current_url)
        else:
            match = re.search(r'start=(\d+)', current_url)
        
        if match:
            start = int(match.group(1))
            current_page = (start // Config.POSTS_PER_PAGE) + 1 if start > 0 else 1
            
            max_page = 1
            if pagination:
                links = pagination.find_all('a', href=True, limit=8)
                for link in links:
                    text = link.get_text(strip=True)
                    if text.isdigit():
                        max_page = max(max_page, int(text))
            
            return current_page, max_page
        
        return 1, 1
    
    def get_total_topic_pages(self, soup: BeautifulSoup) -> int:
        """Определяет общее количество страниц в теме.
        
        Args:
            soup: BeautifulSoup объект первой страницы темы
            
        Returns:
            Общее количество страниц в теме
        """
        try:
            pagination = soup.find('div', class_='pagination')
            if not pagination:
                return 1
            
            # Ищем информацию "Страница X из Y"
            page_info = pagination.get_text()
            if Config.PRECOMPILE_REGEX:
                match = self._page_info_pattern.search(page_info)
            else:
                match = re.search(r'Страница\s*(\d+)\s*из\s*(\d+)', page_info)
            
            if match:
                return int(match.group(2))
            
            # Альтернативно ищем максимальный номер страницы среди ссылок
            max_page = 1
            links = pagination.find_all('a', href=True, limit=10)
            for link in links:
                text = link.get_text(strip=True)
                if text.isdigit():
                    max_page = max(max_page, int(text))
            
            return max_page
            
        except Exception as e:
            self.logger.error(lambda: f"Ошибка при определении количества страниц темы: {e}")
            return 1


# Backward compatibility
HtmlParser = OptimizedHtmlParser
PaginationHandler = OptimizedPaginationHandler 