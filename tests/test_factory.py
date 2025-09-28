
import pytest
from adapters.factory import get_adapter

def test_get_adapter_unknown_site_raises_error():
    """
    Tests that get_adapter raises a ValueError for an unknown site key.
    """
    with pytest.raises(ValueError, match="Unknown site: non_existent_site"):
        get_adapter("non_existent_site")

def test_get_adapter_known_site_returns_adapter():
    """
    Tests that get_adapter returns a valid adapter instance for a known site.
    """
    # This assumes 'xtruyen' is a known and implemented adapter
    adapter = get_adapter("xtruyen")
    assert adapter is not None
    # We can check if it's a subclass of a base adapter if one exists
    from adapters.base_site_adapter import BaseSiteAdapter
    assert isinstance(adapter, BaseSiteAdapter)
