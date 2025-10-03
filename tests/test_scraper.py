
import pytest
from unittest.mock import MagicMock, AsyncMock
from scraper import make_request

# A mock response object that simulates a real httpx.Response
class MockResponse:
    def __init__(self, text, status_code):
        self.text = text
        self.status_code = status_code

@pytest.mark.asyncio
async def test_make_request_falls_back_to_playwright(monkeypatch):
    """ 
    Tests that make_request() correctly falls back to Playwright when the initial
    httpx request is blocked by an anti-bot measure.
    """
    # 1. Mock the httpx fetch to return an anti-bot response
    # We make it an async mock because the original function is async
    mock_httpx_fetch = AsyncMock(
        return_value=MockResponse("Cloudflare challenge page", 200)
    )
    monkeypatch.setattr("scraper.fetch", mock_httpx_fetch)

    # 2. Mock the anti-bot check to reliably return True for our fake response
    # This is better than relying on the real implementation
    mock_is_anti_bot = MagicMock(return_value=True)
    monkeypatch.setattr("scraper.is_anti_bot_content", mock_is_anti_bot)

    # 3. Mock the Playwright function to just record the call, not run a real browser
    mock_playwright_request = AsyncMock(return_value=MockResponse("Success from Playwright", 200))
    monkeypatch.setattr("scraper._make_request_playwright", mock_playwright_request)

    # 4. Call the function under test
    await make_request("https://example.com", "test_site")

    # 5. Assertions
    # Check that the initial httpx fetch was called
    mock_httpx_fetch.assert_called_once()

    # Check that the anti-bot function was called with the text from the httpx response
    mock_is_anti_bot.assert_called_once_with("Cloudflare challenge page")

    # Crucially, check that the fallback to Playwright was triggered
    mock_playwright_request.assert_called_once()

@pytest.mark.asyncio
async def test_make_request_succeeds_with_httpx(monkeypatch):
    """
    Tests that make_request() does NOT fall back to Playwright if httpx succeeds.
    """
    # 1. Mock httpx to return a good response
    mock_httpx_fetch = AsyncMock(
        return_value=MockResponse("Successful content", 200)
    )
    monkeypatch.setattr("scraper.fetch", mock_httpx_fetch)

    # 2. Mock the anti-bot check to return False
    mock_is_anti_bot = MagicMock(return_value=False)
    monkeypatch.setattr("scraper.is_anti_bot_content", mock_is_anti_bot)

    # 3. Mock the Playwright function
    mock_playwright_request = AsyncMock()
    monkeypatch.setattr("scraper._make_request_playwright", mock_playwright_request)

    # 4. Call the function
    response = await make_request("https://example.com", "test_site")

    # 5. Assertions
    mock_httpx_fetch.assert_called_once()
    mock_is_anti_bot.assert_called_once_with("Successful content")

    # Assert that the fallback was NOT triggered
    mock_playwright_request.assert_not_called()

    # Assert we got the successful response from httpx
    assert response.text == "Successful content"


@pytest.mark.asyncio
async def test_make_request_handles_404_without_fallback(monkeypatch):
    """Ensure 404 responses are returned directly without triggering Playwright."""

    mock_httpx_fetch = AsyncMock(return_value=MockResponse("Missing", 404))
    monkeypatch.setattr("scraper.fetch", mock_httpx_fetch)

    mock_is_anti_bot = MagicMock(return_value=False)
    monkeypatch.setattr("scraper.is_anti_bot_content", mock_is_anti_bot)

    mock_playwright_request = AsyncMock()
    monkeypatch.setattr("scraper._make_request_playwright", mock_playwright_request)

    response = await make_request("https://example.com/missing", "test_site")

    assert response.status_code == 404
    assert response.text == "Missing"
    mock_playwright_request.assert_not_called()
