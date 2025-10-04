"""Automatic discovery and registration for site adapters."""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from types import ModuleType
from typing import Dict, Iterable, Optional, Type

from adapters.base_site_adapter import BaseSiteAdapter

_ADAPTER_MODULE_PREFIX = "adapters."
_ADAPTER_SUFFIX = "_adapter"


class AdapterRegistry:
    """Registry that discovers and instantiates site adapters on demand."""

    def __init__(self) -> None:
        self._adapter_classes: Dict[str, Type[BaseSiteAdapter]] = {}
        self._is_discovered = False

    def register(self, adapter_cls: Type[BaseSiteAdapter]) -> None:
        """Register an adapter class provided by discovery or manual hooks."""

        if not inspect.isclass(adapter_cls) or not issubclass(adapter_cls, BaseSiteAdapter):
            raise TypeError("Adapter must be a subclass of BaseSiteAdapter")

        site_key = adapter_cls.get_site_key()
        if site_key in self._adapter_classes:
            existing = self._adapter_classes[site_key]
            if existing is adapter_cls:
                return
            raise ValueError(
                f"Duplicate adapter registration for site '{site_key}': "
                f"{existing.__module__}.{existing.__name__} already registered"
            )

        self._adapter_classes[site_key] = adapter_cls

    def available_site_keys(self) -> Iterable[str]:
        """Return the discovered site keys."""

        self._ensure_discovered()
        return sorted(self._adapter_classes.keys())

    def get(self, site_key: str) -> BaseSiteAdapter:
        """Instantiate the adapter registered for ``site_key``."""

        self._ensure_discovered()
        try:
            adapter_cls = self._adapter_classes[site_key]
        except KeyError as exc:
            raise ValueError(f"Unknown site: {site_key}") from exc
        return adapter_cls()

    # ------------------------------------------------------------------
    # Discovery helpers

    def _ensure_discovered(self) -> None:
        if not self._is_discovered:
            self._discover_adapters()
            self._is_discovered = True

    def _discover_adapters(self) -> None:
        """Find adapter implementations under the ``adapters`` package."""

        package = importlib.import_module("adapters")
        package_path = getattr(package, "__path__", [])

        for module_info in pkgutil.iter_modules(package_path, _ADAPTER_MODULE_PREFIX):
            module_name = module_info.name
            if not module_name.endswith(_ADAPTER_SUFFIX):
                continue
            module = self._import_module(module_name)
            if module is None:
                continue
            self._register_module_adapters(module)

    def _import_module(self, module_name: str) -> Optional[ModuleType]:
        try:
            return importlib.import_module(module_name)
        except Exception:
            # Import errors should not prevent other adapters from loading.
            return None

    def _register_module_adapters(self, module: ModuleType) -> None:
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if not issubclass(obj, BaseSiteAdapter) or obj is BaseSiteAdapter:
                continue
            if obj.__module__ != module.__name__:
                continue
            self.register(obj)


adapter_registry = AdapterRegistry()

__all__ = ["adapter_registry", "AdapterRegistry"]
