from cli_data_processor.pipeline import build_pipeline

def test_filter_eq_reduces_records():
    config = {"transforms": [{"type": "filter_eq", "field": "country", "value": "GB"}]}
    records = [
        {"country": "GB", "name": "A"},
        {"country": "RO", "name": "B"},
        {"country": "GB", "name": "C"},
    ]

    pipeline = build_pipeline(config)
    out = pipeline(records)

    assert len(out) == 2
    assert [r["name"] for r in out] == ["A", "C"]


def test_select_keeps_only_fields():
    config = {"transforms": [{"type": "select", "fields": ["a"]}]}
    records = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

    pipeline = build_pipeline(config)
    out = pipeline(records)

    assert out == [{"a": 1}, {"a": 3}]


def test_rename_changes_keys():
    config = {"transforms": [{"type": "rename", "mapping": {"first_name": "firstName"}}]}
    records = [{"first_name": "Horia", "last_name": "Cns"}]

    pipeline = build_pipeline(config)
    out = pipeline(records)

    assert out == [{"firstName": "Horia", "last_name": "Cns"}]


def test_derive_concat_adds_new_field():
    config = {
        "transforms": [
            {"type": "derive_concat", "new_field": "fullName", "fields": ["first", "last"], "sep": " "}
        ]
    }
    records = [{"first": "Horia", "last": "Cns"}, {"first": "Ana", "last": "Popescu"}]

    pipeline = build_pipeline(config)
    out = pipeline(records)

    assert out[0]["fullName"] == "Horia Cns"
    assert out[1]["fullName"] == "Ana Popescu"


def test_full_pipeline_end_to_end():
    config = {
        "transforms": [
            {"type": "filter_eq", "field": "country", "value": "GB"},
            {"type": "rename", "mapping": {"first_name": "firstName", "last_name": "lastName"}},
            {"type": "derive_concat", "new_field": "fullName", "fields": ["firstName", "lastName"], "sep": " "},
            {"type": "select", "fields": ["fullName", "country"]},
        ]
    }

    records = [
        {"first_name": "Horia", "last_name": "Cns", "country": "GB"},
        {"first_name": "Ana", "last_name": "Popescu", "country": "RO"},
    ]

    pipeline = build_pipeline(config)
    out = pipeline(records)

    assert out == [{"fullName": "Horia Cns", "country": "GB"}]
