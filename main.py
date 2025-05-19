import asyncio
import sys
from adapters.factory import get_adapter
from core.state_service import merge_all_missing_workers_to_main
from pipelines.genre_pipeline import run_crawler
from pipelines.retry_pipeline import retry_failed_genres

if __name__ == '__main__':
    site_key = "metruyenfull"
    if len(sys.argv) > 1:
        site_key = sys.argv[1]
    print(f"[MAIN] Đang chạy crawler cho site: {site_key}")
    merge_all_missing_workers_to_main(site_key)
    adapter = get_adapter(site_key)
    asyncio.run(run_crawler(adapter, site_key))
    asyncio.run(retry_failed_genres(adapter, site_key))