# Optimization Plan for Crawler System

This document lists key areas to improve the performance and stability of the crawler.

## 1. Limit Playwright Usage
- Prefer standard HTTP requests for regular pages.
- Detect anti-bot or dynamic pages and fall back to Playwright only when necessary.
- Reuse browser instances and contexts instead of initializing them repeatedly.

## 2. Proxy Management
- Rotate proxy pool immediately on detection of blocks.
- Maintain blacklist by domain and error codes (e.g. 403, Cloudflare, captcha).
- Log detailed metrics for each proxy to measure success rate.
- Support dynamic loading or reloading of proxies without restarting the crawler.

## 3. Anti-bot Content Filters
- Identify chapters containing patterns like "Bạn đã bị chặn" or "Access Denied".
- Skip saving empty or anti-bot HTML files.
- Avoid excessive retries with proxies already blocked for the same pattern.

## 4. Batch and Async Optimization
- Abort a batch early if every request in the batch returns anti-bot responses.
- Sync crawl state after smaller batches to reduce risk of data loss.
- Reduce batch size dynamically when a site encounters heavy blocking.

## 5. Metadata Updates
- Update metadata only when content changes or timestamp on the site is newer.
- Check `total_chapters_on_site` using multiple sources if possible.
- Minimize frequent file writes to prevent I/O bottlenecks.

## 6. File and Chapter Handling
- Offload heavy file operations to background threads.
- Store corrupted or problematic chapters in a separate quarantine folder.

## 7. Retry and Error Queue
- Retry failed chapters using different proxies, headers and cookies.
- Report chapters frequently blocked to help debugging.
- Remove or stop retrying links confirmed as dead.

## 8. Logging and Monitoring
- Separate logs by module (chapter crawl, metadata, proxy, anti-bot, ...).
- Maintain dedicated logs for failed chapters, failed batches and blocked proxies.
- Implement periodic health checks per site and send alerts (e.g. Telegram) when error rates spike.

## 9. Reduce Duplicate Crawling
- Avoid re-crawling completed stories or ones with all chapters already downloaded.
- Synchronize completion and skip states to prevent repeated processing across workers.

## 10. Dynamic Configuration
- Allow runtime changes to settings such as proxies, batch size and retry counts.
- Apply per-site adjustments: increase delays or reduce batch sizes for heavy anti-bot sites.


## 11. Proxy Health Checks
- Periodically test proxies and remove ones with high failure rate.
- Apply cooldown delays before re-using a proxy that recently failed.

## 12. Error Classification
- Tag retries with categories such as anti-bot, 404, timeout or dead link.
- Tune retry counts per error class to avoid wasting requests.

## 13. File Operation Safety
- Wrap file move operations and retry on common filesystem errors.
- Log failed moves and keep partially moved data in a safe folder.

## 14. Reporting Dashboard
- Send overall crawl statistics and error summaries via Telegram.
- Optionally expose a CLI/WEB dashboard showing worker status and queue lengths.

## 15. Worker Management API
- Provide endpoints to start/stop batches or query current progress.
- Combine health check results across workers for a global view.

