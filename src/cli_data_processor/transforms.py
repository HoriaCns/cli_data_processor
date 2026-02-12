from collections.abc import Callable

Transform = Callable[[list[dict]], list[dict]]

def filter_eq(field: str, value: str) -> Transform:
    def _t(records: list[dict]) -> list[dict]:
        return [r for r in records if str(r.get(field, "")) == str(value)]
    _t.__name__ = f"filter_eq({field}={value})"
    return _t

def select(fields: list[str]) -> Transform:
    def _t(records: list[dict]) -> list[dict]:
        return [{k: r.get(k) for k in fields} for r in records]
    _t.__name__ = f"select({fields})"
    return _t

def rename(mapping: dict[str, str]) -> Transform:
    def _t(records: list[dict]) -> list[dict]:
        out: list[dict] = []
        for r in records:
            nr = {}
            for k, v in r.items():
                nr[mapping.get(k, k)] = v
            out.append(nr)
        return out
    _t.__name__ = f"rename({list(mapping.keys())}->{list(mapping.values())})"
    return _t

def derive_concat(new_field: str, fields: list[str], sep: str = " ") -> Transform:
    def _t(records: list[dict]) -> list[dict]:
        out: list[dict] = []
        for r in records:
            nr = dict(r)  # copy
            parts = [str(r.get(f, "") or "") for f in fields]
            nr[new_field] = sep.join(parts).strip()
            out.append(nr)
        return out
    _t.__name__ = f"derive_concat({new_field}<-{fields}, sep={sep!r})"
    return _t
