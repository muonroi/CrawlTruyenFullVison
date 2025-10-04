from __future__ import annotations

import asyncio
import os
import random
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from config.env_loader import (
    EnvironmentConfigurationError,
    get_bool,
    get_float,
    get_int,
    get_list,
    get_str,
)
from config.useragent_list import STATIC_USER_AGENTS


BASE_URL_ENV_MAP: Dict[str, str] = {
    "xtruyen": "BASE_XTRUYEN",
    "tangthuvien": "BASE_TANGTHUVIEN",
}


def _sanitize_base_url(raw_value: Optional[str], env_name: str) -> str:
    """Normalize BASE_URL style inputs to avoid malformed URLs."""

    if raw_value is None:
        raise EnvironmentConfigurationError(f"Missing environment variable {env_name}")

    candidate = raw_value.strip()
    if not candidate:
        raise EnvironmentConfigurationError(f"Environment variable {env_name} must not be empty")

    candidate = candidate.rstrip("!?#'\"")
    candidate = candidate.rstrip("/ \t\n\r")
    if not candidate:
        raise EnvironmentConfigurationError(f"Environment variable {env_name} must not be empty")

    parsed = urlparse(candidate)
    if not parsed.scheme:
        candidate = f"https://{candidate}"
        parsed = urlparse(candidate)
        if not parsed.scheme:
            raise EnvironmentConfigurationError(
                f"Environment variable {env_name} must include scheme (e.g. https://)."
            )
    return candidate


def _load_base_urls() -> Dict[str, str]:
    base_urls = {
        site_key: _sanitize_base_url(get_str(env_name, required=True), env_name)
        for site_key, env_name in BASE_URL_ENV_MAP.items()
    }

    enabled_site_keys = get_list("ENABLED_SITE_KEYS") or []
    if enabled_site_keys:
        missing_sites = [key for key in enabled_site_keys if key not in base_urls]
        if missing_sites:
            raise EnvironmentConfigurationError(
                "ENABLED_SITE_KEYS contains unsupported site(s): " + ", ".join(missing_sites)
            )
        base_urls = {key: base_urls[key] for key in enabled_site_keys}

    if not base_urls:
        raise EnvironmentConfigurationError("No BASE_URLS configured. Please check your environment settings.")

    return base_urls


def _normalize_optional_limit(value: Optional[int]) -> Optional[int]:
    if value is None or value <= 0:
        return None
    return value


def _ensure_directories(*paths: str) -> None:
    for path in paths:
        if path:
            os.makedirs(path, exist_ok=True)


def _load_settings() -> Dict[str, Any]:
    base_urls = _load_base_urls()

    request_delay = get_float("REQUEST_DELAY", required=True)
    timeout_request = get_int("TIMEOUT_REQUEST", required=True)
    retry_attempts = get_int("RETRY_ATTEMPTS", required=True)
    delay_on_retry = get_float("DELAY_ON_RETRY", required=True)

    data_folder = get_str("DATA_FOLDER", required=True)
    completed_folder = get_str("COMPLETED_FOLDER", required=True)
    backup_folder = get_str("BACKUP_FOLDER", required=True)
    state_folder = get_str("STATE_FOLDER", required=True)
    log_folder = get_str("LOG_FOLDER", required=True)
    _ensure_directories(
        data_folder or "",
        completed_folder or "",
        backup_folder or "",
        state_folder or "",
        log_folder or "",
    )

    category_snapshot_db = get_str("CATEGORY_SNAPSHOT_DB_PATH")
    if not category_snapshot_db:
        category_snapshot_db = os.path.join(state_folder, "category_snapshots.sqlite3")
    snapshot_dir = os.path.dirname(category_snapshot_db)
    if snapshot_dir:
        _ensure_directories(snapshot_dir)

    retry_genre_round_limit = get_int("RETRY_GENRE_ROUND_LIMIT", required=True)
    retry_sleep_seconds = get_int("RETRY_SLEEP_SECONDS", required=True)
    retry_failed_chapters_passes = get_int("RETRY_FAILED_CHAPTERS_PASSES", required=True)
    num_chapter_batches = get_int("NUM_CHAPTER_BATCHES", required=True)
    max_chapters_per_story = _normalize_optional_limit(get_int("MAX_CHAPTERS_PER_STORY"))
    retry_story_round_limit = get_int("RETRY_STORY_ROUND_LIMIT", required=True)
    skipped_stories_file = get_str("SKIPPED_STORIES_FILE", required=True)
    max_chapter_retry = get_int("MAX_CHAPTER_RETRY", required=True)

    kafka_topic = get_str("KAFKA_TOPIC", required=True)
    kafka_bootstrap_servers = get_str("KAFKA_BROKERS", required=True)
    kafka_group_id = get_str("KAFKA_GROUP_ID", required=True)
    kafka_bootstrap_max_retries = get_int("KAFKA_BOOTSTRAP_MAX_RETRIES", required=True)
    kafka_bootstrap_retry_delay = get_float("KAFKA_BOOTSTRAP_RETRY_DELAY", required=True)
    progress_topic = get_str("PROGRESS_TOPIC") or f"{kafka_topic}.progress"
    progress_group_id = get_str("PROGRESS_GROUP_ID") or f"{kafka_group_id}-progress"

    use_proxy = bool(get_bool("USE_PROXY", required=True))
    proxies_folder = get_str("PROXIES_FOLDER", required=True)
    proxies_file = get_str("PROXIES_FILE", required=True)
    global_proxy_username = get_str("PROXY_USER")
    global_proxy_password = get_str("PROXY_PASS")
    proxy_api_url = get_str("PROXY_API_URL")

    telegram_bot_token = get_str("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = get_str("TELEGRAM_CHAT_ID")
    telegram_thread_id = get_str("TELEGRAM_THREAD_ID")
    telegram_parse_mode = get_str("TELEGRAM_PARSE_MODE")
    telegram_disable_preview = get_bool("TELEGRAM_DISABLE_WEB_PAGE_PREVIEW")

    max_genres_to_crawl = _normalize_optional_limit(get_int("MAX_GENRES_TO_CRAWL"))
    max_stories_per_genre_page = _normalize_optional_limit(get_int("MAX_STORIES_PER_GENRE_PAGE"))
    max_stories_total_per_genre = _normalize_optional_limit(get_int("MAX_STORIES_TOTAL_PER_GENRE"))
    max_chapter_pages_to_crawl = _normalize_optional_limit(get_int("MAX_CHAPTER_PAGES_TO_CRAWL"))

    async_semaphore_limit = get_int("ASYNC_SEMAPHORE_LIMIT", required=True)
    genre_async_limit = get_int("GENRE_ASYNC_LIMIT", required=True)
    genre_batch_size = get_int("GENRE_BATCH_SIZE", required=True)
    story_async_limit = get_int("STORY_ASYNC_LIMIT", required=True)
    story_batch_size = get_int("STORY_BATCH_SIZE", required=True)

    failed_genres_file = get_str("FAILED_GENRES_FILE", required=True)
    pattern_file = get_str("PATTERN_FILE", required=True)
    anti_bot_pattern_file = get_str("ANTI_BOT_PATTERN_FILE", required=True)

    batch_size_override = get_int("BATCH_SIZE")

    ai_profiles_path = get_str("AI_PROFILES_PATH", required=True)
    ai_metrics_path = get_str("AI_METRICS_PATH", required=True)
    ai_model = get_str("AI_MODEL", required=True)
    ai_profile_ttl_hours = get_int("AI_PROFILE_TTL_HOURS", required=True)
    openai_base = get_str("OPENAI_BASE", required=True)
    openai_api_key = get_str("OPENAI_API_KEY")
    ai_trim_max_bytes = get_int("AI_TRIM_MAX_BYTES", required=True)
    ai_print_metrics = bool(get_bool("AI_PRINT_METRICS") or False)

    default_mode = get_str("MODE")
    default_crawl_mode = get_str("CRAWL_MODE")


    missing_timeout = get_int("MISSING_CRAWL_TIMEOUT_SECONDS", required=True)
    missing_timeout_per_chapter = get_float("MISSING_CRAWL_TIMEOUT_PER_CHAPTER", required=True)
    missing_timeout_max = get_int("MISSING_CRAWL_TIMEOUT_MAX", required=True)

    missing_warning_topic = get_str("MISSING_WARNING_TOPIC", required=True)
    missing_warning_group = get_str("MISSING_WARNING_GROUP", required=True)

    return {
        "BASE_URLS": base_urls,
        "ENABLED_SITE_KEYS": list(base_urls.keys()),
        "REQUEST_DELAY": request_delay,
        "TIMEOUT_REQUEST": timeout_request,
        "RETRY_ATTEMPTS": retry_attempts,
        "DELAY_ON_RETRY": delay_on_retry,
        "DATA_FOLDER": data_folder,
        "COMPLETED_FOLDER": completed_folder,
        "BACKUP_FOLDER": backup_folder,
        "STATE_FOLDER": state_folder,
        "LOG_FOLDER": log_folder,
        "CATEGORY_SNAPSHOT_DB_PATH": category_snapshot_db,
        "RETRY_GENRE_ROUND_LIMIT": retry_genre_round_limit,
        "RETRY_SLEEP_SECONDS": retry_sleep_seconds,
        "RETRY_FAILED_CHAPTERS_PASSES": retry_failed_chapters_passes,
        "NUM_CHAPTER_BATCHES": num_chapter_batches,
        "MAX_CHAPTERS_PER_STORY": max_chapters_per_story,
        "RETRY_STORY_ROUND_LIMIT": retry_story_round_limit,
        "SKIPPED_STORIES_FILE": skipped_stories_file,
        "MAX_CHAPTER_RETRY": max_chapter_retry,
        "KAFKA_TOPIC": kafka_topic,
        "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
        "KAFKA_GROUP_ID": kafka_group_id,
        "KAFKA_BOOTSTRAP_MAX_RETRIES": kafka_bootstrap_max_retries,
        "KAFKA_BOOTSTRAP_RETRY_DELAY": kafka_bootstrap_retry_delay,
        "PROGRESS_TOPIC": progress_topic,
        "PROGRESS_GROUP_ID": progress_group_id,
        "USE_PROXY": use_proxy,
        "PROXIES_FOLDER": proxies_folder,
        "PROXIES_FILE": proxies_file,
        "GLOBAL_PROXY_USERNAME": global_proxy_username,
        "GLOBAL_PROXY_PASSWORD": global_proxy_password,
        "PROXY_API_URL": proxy_api_url,
        "TELEGRAM_BOT_TOKEN": telegram_bot_token,
        "TELEGRAM_CHAT_ID": telegram_chat_id,
        "TELEGRAM_THREAD_ID": telegram_thread_id,
        "TELEGRAM_PARSE_MODE": telegram_parse_mode,
        "TELEGRAM_DISABLE_WEB_PAGE_PREVIEW": telegram_disable_preview,
        "MAX_GENRES_TO_CRAWL": max_genres_to_crawl,
        "MAX_STORIES_PER_GENRE_PAGE": max_stories_per_genre_page,
        "MAX_STORIES_TOTAL_PER_GENRE": max_stories_total_per_genre,
        "MAX_CHAPTER_PAGES_TO_CRAWL": max_chapter_pages_to_crawl,
        "ASYNC_SEMAPHORE_LIMIT": async_semaphore_limit,
        "GENRE_ASYNC_LIMIT": genre_async_limit,
        "GENRE_BATCH_SIZE": genre_batch_size,
        "STORY_ASYNC_LIMIT": story_async_limit,
        "STORY_BATCH_SIZE": story_batch_size,
        "FAILED_GENRES_FILE": failed_genres_file,
        "PATTERN_FILE": pattern_file,
        "ANTI_BOT_PATTERN_FILE": anti_bot_pattern_file,
        "BATCH_SIZE_OVERRIDE": batch_size_override,
        "AI_PROFILES_PATH": ai_profiles_path,
        "AI_METRICS_PATH": ai_metrics_path,
        "AI_MODEL": ai_model,
        "AI_PROFILE_TTL_HOURS": ai_profile_ttl_hours,
        "OPENAI_BASE": openai_base,
        "OPENAI_API_KEY": openai_api_key,
        "AI_TRIM_MAX_BYTES": ai_trim_max_bytes,
        "AI_PRINT_METRICS": ai_print_metrics,
        "DEFAULT_MODE": default_mode,
        "DEFAULT_CRAWL_MODE": default_crawl_mode,
        "MISSING_CRAWL_TIMEOUT_SECONDS": missing_timeout,
        "MISSING_CRAWL_TIMEOUT_PER_CHAPTER": missing_timeout_per_chapter,
        "MISSING_CRAWL_TIMEOUT_MAX": missing_timeout_max,
        "MISSING_WARNING_TOPIC": missing_warning_topic,
        "MISSING_WARNING_GROUP": missing_warning_group,
    }


def _apply_settings(settings: Dict[str, Any]) -> None:
    globals().update(settings)


def reload_from_env() -> None:
    """Reload configuration values from the current environment."""

    _apply_settings(_load_settings())


# Load initial configuration on module import.
_apply_settings(_load_settings())


ENABLED_SITE_KEYS: List[str] = list(BASE_URLS.keys())

HEADER_PATTERNS = [r"^nguồn:", r"^truyện:", r"^thể loại:", r"^chương:"]
HEADER_RE = re.compile("|".join(HEADER_PATTERNS), re.IGNORECASE)

SITE_SELECTORS: Dict[str, Any] = {}


LOADED_PROXIES: List[str] = []
LOCK = asyncio.Lock()


_MOBILE_USER_AGENT_KEYWORDS = (
    "android",
    "iphone",
    "ipad",
    "ipod",
    "iemobile",
    "mobile",
    "opera mini",
    "blackberry",
    "windows phone",
)


def _is_desktop_user_agent(user_agent: str) -> bool:
    lowered = user_agent.lower()
    return not any(keyword in lowered for keyword in _MOBILE_USER_AGENT_KEYWORDS)


async def _get_fake_user_agent() -> Optional[str]:
    global _UA_OBJ, _DISABLE_FAKE_UA
    if _DISABLE_FAKE_UA:
        return None

    loop = asyncio.get_event_loop()
    if _UA_OBJ is None:
        _UA_OBJ = await loop.run_in_executor(None, _init_user_agent)
        if _UA_OBJ is None:
            _DISABLE_FAKE_UA = True
            return None

    try:
        return await loop.run_in_executor(None, lambda: _UA_OBJ.random)  # type: ignore
    except Exception:
        _DISABLE_FAKE_UA = True
        return None


async def get_random_user_agent(desktop_only: bool = False) -> str:
    global _UA_OBJ, _DISABLE_FAKE_UA

    attempts = 5 if desktop_only else 1
    for _ in range(attempts):
        candidate = await _get_fake_user_agent()
        if candidate is None:
            candidate = random.choice(STATIC_USER_AGENTS)

        if not desktop_only or _is_desktop_user_agent(candidate):
            return candidate

    for ua in STATIC_USER_AGENTS:
        if not desktop_only or _is_desktop_user_agent(ua):
            return ua

    return random.choice(STATIC_USER_AGENTS)


_UA_OBJ = None
_DISABLE_FAKE_UA = False


def _init_user_agent():
    try:
        from fake_useragent import UserAgent

        return UserAgent(fallback=STATIC_USER_AGENTS[0])
    except Exception as e:
        print(f"Lỗi khi khởi tạo UserAgent: {e}")
        return None


async def get_random_headers(site_key, desktop_only: bool = False):
    ua_string = await get_random_user_agent(desktop_only=desktop_only)
    headers = {
        "User-Agent": ua_string,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5",
    }
    base_url = BASE_URLS.get(site_key)
    if base_url:
        headers["Referer"] = base_url.rstrip('/') + '/'
    return headers


def get_state_file(site_key: str) -> str:
    return os.path.join(STATE_FOLDER, f"crawl_state_{site_key}.json")


def load_blacklist_patterns(file_path):
    patterns, contains_list = [], []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith('^') or line.endswith('$') or re.search(r'[.*?|\[\]()\\]', line):
                patterns.append(re.compile(line, re.IGNORECASE))
            else:
                contains_list.append(line.lower())
    return patterns, contains_list
