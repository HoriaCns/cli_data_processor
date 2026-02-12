import argparse
import logging
from pathlib import Path

from .config import load_config
from .io import read_records, write_records
from .pipeline import build_pipeline

def main() -> int:
    parser = argparse.ArgumentParser(prog="cli-data-processor")
    parser.add_argument("--input", required=True, help="Input file path")
    parser.add_argument("--output", required=True, help="Output file path")
    parser.add_argument("--config", required=True, help="YAML config path")
    parser.add_argument("--format", choices=["csv", "jsonl"], required=True, help="Input format")
    parser.add_argument("--output-format", choices=["jsonl", "csv"], default="jsonl", help="Output format")
    parser.add_argument("--log-level", default="INFO", help="DEBUG|INFO|WARNING|ERROR")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger(__name__)

    in_path = Path(args.input)
    out_path = Path(args.output)
    cfg_path = Path(args.config)

    log.info("Loading config: %s", cfg_path)
    config = load_config(cfg_path)

    log.info("Reading input: %s (%s)", in_path, args.format)
    records = read_records(in_path, args.format)
    log.info("Loaded %d records", len(records))

    pipeline = build_pipeline(config)

    log.info("Running pipeline (%d steps)", len(config["transforms"]))
    out_records = pipeline(records)
    log.info("Output %d records", len(out_records))

    log.info("Writing output: %s", out_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_records(out_path, out_records, args.output_format)

    log.info("Done!")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())