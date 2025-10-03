import pytest

from adapters.base_site_adapter import BaseSiteAdapter
from adapters.factory import get_adapter


def test_get_adapter_unknown_site_raises_error():
    """Tests that get_adapter raises a ValueError for an unknown site key."""
    with pytest.raises(ValueError, match="Unknown site: non_existent_site"):
        get_adapter("non_existent_site")


@pytest.mark.parametrize("site_key", ["xtruyen", "tangthuvien"])
def test_get_adapter_known_site_returns_adapter(site_key):
    """Tests that get_adapter returns a valid adapter instance for a known site."""
    adapter = get_adapter(site_key)
    assert adapter is not None
    assert isinstance(adapter, BaseSiteAdapter)
    assert getattr(adapter, "site_key", None) == site_key
