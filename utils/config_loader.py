import tomllib
from pathlib import Path
from typing import Any

_config: dict[str, Any] | None = None


def _load_config() -> dict[str, Any]:
    global _config
    if _config is None:
        path = Path("env.toml")
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path, "rb") as f:
            _config = tomllib.load(f)

    return _config


def get_config(section: str, subsection: str, key: str) -> Any:
    config: dict[str, Any] = _load_config()

    try:
        return config[section][subsection][key]
    except (KeyError, TypeError):
        raise KeyError(f"Configuration key '{section}.{subsection}.{key}' not found")
