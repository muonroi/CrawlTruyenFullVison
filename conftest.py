import asyncio
import inspect
import pytest


# def pytest_configure(config: pytest.Config) -> None:
#     config.addinivalue_line(
#         "markers",
#         "asyncio: mark test to run inside an event loop without pytest-asyncio",
#     )


# @pytest.hookimpl(tryfirst=True)
# def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
#     if "asyncio" not in pyfuncitem.keywords:
#         return None
# 
#     test_func = pyfuncitem.obj
#     if not inspect.iscoroutinefunction(test_func):
#         return None
# 
#     sig = inspect.signature(test_func)
#     call_kwargs = {name: pyfuncitem.funcargs[name] for name in sig.parameters}
# 
#     loop = asyncio.new_event_loop()
#     try:
#         asyncio.set_event_loop(loop)
#         loop.run_until_complete(test_func(**call_kwargs))
#     finally:
#         asyncio.set_event_loop(None)
#         loop.close()
#     return True
