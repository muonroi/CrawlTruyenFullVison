
import os
import json
import time
from typing import Dict, Any, Optional, List

from utils.domain_utils import get_site_key_from_url, is_url_for_site
from utils.io_utils import ensure_directory_exists
from utils.logger import logger
from utils.chapter_utils import slugify_title

# Assuming DATA_FOLDER is defined in config
from config.config import DATA_FOLDER


def get_story_folder(story_title: str, story_author: Optional[str] = None) -> str:
    """
    Determines the storage folder for a story based on its title and author.
    Currently uses just the title slug.
    """
    # This can be enhanced later to use author as well.
    slug = slugify_title(story_title)
    return os.path.join(DATA_FOLDER, slug)


def load_metadata(story_folder_path: str) -> Optional[Dict[str, Any]]:
    """Loads metadata.json from a story's folder."""
    metadata_file = os.path.join(story_folder_path, "metadata.json")
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load metadata from {metadata_file}: {e}")
            return None
    return None


def save_metadata(story_folder_path: str, metadata: Dict[str, Any], is_new: bool = False):
    """Saves metadata.json to a story's folder."""
    metadata_file = os.path.join(story_folder_path, "metadata.json")
    try:
        metadata.setdefault("metadata_updated_at", time.strftime("%Y-%m-%d %H:%M:%S"))
        if is_new:
            metadata.setdefault("crawled_at", time.strftime("%Y-%m-%d %H:%M:%S"))

        # Ensure we don't save chapter lists in the main metadata
        metadata_to_write = metadata.copy()
        metadata_to_write.pop("chapters", None)

        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata_to_write, f, ensure_ascii=False, indent=4)
    except IOError as e:
        logger.error(f"Failed to save metadata to {metadata_file}: {e}")


def sort_sources(sources):
    """Sorts a list of sources by priority."""
    return sorted(sources, key=lambda s: s.get("priority", 100))


def normalize_story_sources(
    story_data_item: Dict[str, Any],
    current_site_key: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Merge and normalize source list between story data and existing metadata.
    Returns True if the metadata was changed.
    """

    def _iter_raw_sources() -> List[Any]:
        raw: List[Any] = []
        if metadata and isinstance(metadata.get("sources"), list):
            raw.extend(metadata["sources"])
        if isinstance(story_data_item.get("sources"), list):
            raw.extend(story_data_item["sources"])
        primary_url = metadata.get("url") if metadata else None
        if primary_url:
            raw.append({"url": primary_url, "site_key": metadata.get("site_key")})
        current_url = story_data_item.get("url")
        if current_url:
            raw.append({"url": current_url, "site_key": current_site_key, "priority": 1})
        return raw

    raw_sources = _iter_raw_sources()
    normalized: List[Dict[str, Any]] = []
    seen: Dict[tuple[str, str], int] = {}

    def _upsert(url: Optional[str], site_key: Optional[str], priority: Any) -> None:
        if not url:
            return
        url = url.strip()
        if not url:
            return

        derived_site = get_site_key_from_url(url)
        site_candidate = site_key or derived_site or current_site_key
        if not site_candidate:
            return
        if derived_site and derived_site != site_candidate:
            site_candidate = derived_site
        if not is_url_for_site(url, site_candidate):
            return

        identity = (url, site_candidate)
        priority_val = priority if isinstance(priority, (int, float)) else None

        if identity in seen:
            existing = normalized[seen[identity]]
            if priority_val is not None:
                existing_priority = existing.get("priority")
                if (
                    not isinstance(existing_priority, (int, float))
                    or priority_val < existing_priority
                ):
                    existing["priority"] = priority_val
            return

        entry: Dict[str, Any] = {"url": url, "site_key": site_candidate}
        if priority_val is not None:
            entry["priority"] = priority_val
        normalized.append(entry)
        seen[identity] = len(normalized) - 1

    for source in raw_sources:
        if isinstance(source, str):
            _upsert(source, None, None)
        elif isinstance(source, dict):
            _upsert(source.get("url"), source.get("site_key") or source.get("site"), source.get("priority"))

    normalized_sorted = sort_sources(normalized)
    
    metadata_changed = False
    if metadata is not None:
        if metadata.get("sources") != normalized_sorted:
            metadata["sources"] = normalized_sorted
            metadata_changed = True
    
    # Also update the story_data_item in-memory for the current crawl session
    story_data_item["sources"] = normalized_sorted

    return metadata_changed
