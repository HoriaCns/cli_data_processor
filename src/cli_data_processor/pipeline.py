from collections.abc import Callable
from . import transforms as T
import logging

log = logging.getLogger(__name__)
Pipeline = Callable[[list[dict]], list[dict]]

def build_pipeline(config: dict) -> Pipeline:
    steps = []
    for item in config["transforms"]:
        t = item["type"]
        if t == "filter_eq":
            steps.append(T.filter_eq(item["field"], item["value"]))
        elif t == "select":
            steps.append(T.select(item["fields"]))
        elif t == "rename":
            steps.append(T.rename(item["mapping"]))
        elif t == "derive_concat":
            steps.append(
                T.derive_concat(
                    new_field=item["new_field"],
                    fields=item["fields"],
                    sep=item.get("sep", " "),
                )
            )
        else:
            raise ValueError(f"Unknown transform type: {t}")


    def run(records: list[dict]) -> list[dict]:
        for i, step in enumerate(steps, start=1):
            before = len(records)
            records = step(records)
            after = len(records)
            log.info("Step %d: %s (%d -> %d)", i, getattr(step, "__name__", "transform"), before, after)
        return records

    return run