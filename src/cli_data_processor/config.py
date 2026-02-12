from pathlib import Path
import yaml

def load_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path.resolve()}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "transforms" not in data:
        raise ValueError("Config must contain 'transforms' list")
    return data
