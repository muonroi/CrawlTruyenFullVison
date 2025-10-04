import os
from utils.logger import logger

def apply_env_overrides(config: dict, prefix: str = "[ENV]") -> None:
    """
    Dùng để override các biến môi trường tại runtime bằng dict từ Kafka message.
    config có thể chứa key `env_override`, là dict chứa các cặp key-value env.
    """
    overrides = config.get("env_override") or {}

    if not isinstance(overrides, dict):
        logger.warning(f"{prefix} Không có env_override hợp lệ trong config: {overrides}")
        return

    for key, val in overrides.items():
        if not isinstance(key, str):
            continue
        os.environ[key] = str(val)
        logger.info(f"{prefix} Gán ENV {key} = {val}")

    if overrides:
        from config import config as app_config
        import main

        app_config.reload_from_env()
        main.refresh_runtime_settings()
