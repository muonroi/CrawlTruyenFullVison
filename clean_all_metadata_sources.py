import json
import os
from config.config import DATA_FOLDER
from utils.logger import logger
from utils.domain_utils import get_site_key_from_url, is_url_for_site


def clean_all_metadata_sources():
    for folder in os.listdir(DATA_FOLDER):
        meta_path = os.path.join(DATA_FOLDER, folder, "metadata.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        before = len(metadata.get("sources", []))
        fixed_sources = []
        for src in metadata.get("sources", []):
            s_url = src.get("url") if isinstance(src, dict) else src
            s_key = get_site_key_from_url(s_url) or (src.get("site_key") if isinstance(src, dict) else None) or (src.get("site") if isinstance(src, dict) else None) or metadata.get("site_key")
            if s_url and s_key and is_url_for_site(s_url, s_key):
                fixed_sources.append(src)
            else:
                logger.warning(f"[CLEAN] Xoá source không đúng domain: {s_url} (site_key: {s_key}) - {meta_path}")
        metadata["sources"] = fixed_sources
        if before != len(fixed_sources):
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    clean_all_metadata_sources()
    print("Đã clean xong tất cả metadata sources.")