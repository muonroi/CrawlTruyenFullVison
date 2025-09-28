
import pytest
import json
from utils.state_utils import save_crawl_state, load_crawl_state, clear_specific_state_keys

@pytest.mark.asyncio
async def test_save_and_load_crawl_state(tmp_path):
    """
    Tests that saving a crawl state and loading it back yields the same data.
    """
    state_file = tmp_path / "state.json"
    sample_state = {
        "current_genre_url": "https://example.com/genre/action",
        "processed_stories": ["story1", "story2"]
    }

    # Save the state
    await save_crawl_state(sample_state, str(state_file))

    # Load the state
    loaded_state = await load_crawl_state(str(state_file), "test_site")

    # Assert the loaded state is the same as the original state
    assert loaded_state == sample_state

@pytest.mark.asyncio
async def test_load_non_existent_state_returns_empty(tmp_path):
    """
    Tests that loading a non-existent state file returns an empty dictionary.
    """
    state_file = tmp_path / "non_existent_state.json"
    
    loaded_state = await load_crawl_state(str(state_file), "test_site")
    
    assert loaded_state == {}

@pytest.mark.asyncio
async def test_clear_specific_state_keys(tmp_path):
    """
    Tests that specific keys can be cleared from the crawl state and the file is updated.
    """
    state_file = tmp_path / "state_to_clear.json"
    initial_state = {
        "current_genre_url": "https://example.com/genre/fantasy",
        "current_story_index_in_genre": 5,
        "to_keep": "this should remain"
    }

    # Save initial state
    await save_crawl_state(initial_state, str(state_file), debounce=0)

    # The state is modified in-place
    state_to_modify = initial_state.copy()
    keys_to_clear = ["current_genre_url", "current_story_index_in_genre"]
    
    await clear_specific_state_keys(state_to_modify, keys_to_clear, str(state_file), debounce=0)

    # Check in-memory modification
    assert "current_genre_url" not in state_to_modify
    assert "current_story_index_in_genre" not in state_to_modify
    assert "to_keep" in state_to_modify

    # Load the state from the file to ensure it was persisted
    with open(state_file, 'r', encoding='utf-8') as f:
        persisted_state = json.load(f)
    
    assert "current_genre_url" not in persisted_state
    assert "current_story_index_in_genre" not in persisted_state
    assert persisted_state["to_keep"] == "this should remain"
