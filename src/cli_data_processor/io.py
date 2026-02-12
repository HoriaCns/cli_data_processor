import csv
import json
from pathlib import Path

def read_records(path: Path, fmt: str) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    if fmt == "csv":
        with path.open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    if fmt == "jsonl":
        records: list[dict] = []
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records
    raise ValueError(f"Unsupported format: {fmt}")

def write_records(path: Path, records: list[dict], fmt: str) -> None:
    if fmt == "jsonl":
        with path.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        return

    if fmt == "csv":
        if not records:
            path.write_text("", encoding="utf-8")
            return
        fieldnames = list(records[0].keys())
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(records)
        return

    raise ValueError(f"Unsupported output format: {fmt}")
