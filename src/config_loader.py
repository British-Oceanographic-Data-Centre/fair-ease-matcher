import json
from pathlib import Path

with open(Path(__file__).parent / "app/config.json", "r", encoding="utf-8") as f:
    config = json.load(f)