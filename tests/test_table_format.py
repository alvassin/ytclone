"""Schema selection preserves values outside the compact Skiff scalar subset."""

import io
import json
import struct
from copy import deepcopy

import pytest
from yt import yson
from yt.wrapper.format import SkiffFormat

from ytclone.format import canonical_attributes
from ytclone.table_format import (
    ROW_FORMAT,
    make_table_format,
    table_format_name,
    validate_table_format,
)


def schema_attributes(columns, *, strict=True):
    schema = yson.YsonList(columns)
    schema.attributes = {b"strict": strict, b"unique_keys": False}
    return canonical_attributes({b"schema": schema})


def skiff_fields(spec):
    return spec["attributes"]["table_skiff_schemas"][0]["children"]


def test_yson_options_preserve_existing_representation_and_are_not_shared():
    spec = make_table_format("yson", b"")
    assert spec == {"name": "yson", "attributes": ROW_FORMAT}
    assert table_format_name(None) == "yson"
    assert validate_table_format(spec) == spec
    spec["attributes"]["format"] = "text"
    assert ROW_FORMAT["format"] == "binary"
    with pytest.raises(ValueError, match="YSON"):
        validate_table_format(spec)


@pytest.mark.parametrize(
    "logical_type,wire_type",
    [
        ("int64", "int64"),
        ("uint64", "uint64"),
        ("double", "double"),
        ("bool", "boolean"),
        ("string", "string32"),
        ("utf8", "string32"),
        ("yson", "yson32"),
    ],
)
@pytest.mark.parametrize("optional", [False, True])
def test_type_v3_scalar_mapping_retains_nullable_boundary(
    logical_type, wire_type, optional
):
    logical = logical_type.encode()
    if optional:
        logical = {b"type_name": b"optional", b"item": logical}
    attrs = schema_attributes([{b"name": b"value", b"type_v3": logical}])
    field, other = skiff_fields(make_table_format("skiff", attrs))
    if optional:
        assert field == {
            "name": "value",
            "wire_type": "variant8",
            "children": [{"wire_type": "nothing"}, {"wire_type": wire_type}],
        }
    else:
        assert field == {"name": "value", "wire_type": wire_type}
    assert other == {"name": "$other_columns", "wire_type": "yson32"}


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize(
    "logical_type",
    [
        {b"type_name": b"list", b"item": b"int64"},
        {b"type_name": b"struct", b"members": [{b"name": b"x", b"type": b"string"}]},
        {b"type_name": b"dict", b"key": b"string", b"value": b"int64"},
        {b"type_name": b"decimal", b"precision": 20, b"scale": 4},
        {
            b"type_name": b"optional",
            b"item": {b"type_name": b"optional", b"item": b"int64"},
        },
        b"null",
        b"uuid",
        b"date",
        b"future_type",
    ],
)
def test_complex_unknown_and_special_types_use_other_columns_even_for_strict_tables(
    strict, logical_type
):
    attrs = schema_attributes(
        [{b"name": b"value", b"type_v3": logical_type}], strict=strict
    )
    assert skiff_fields(make_table_format("skiff", attrs)) == [
        {"wire_type": "yson32", "name": "$other_columns"}
    ]
    assert yson.loads(attrs, encoding=None)[b"schema"][0][b"type_v3"] == logical_type


def test_opaque_and_reserved_column_names_stay_in_other_columns_without_reencoding():
    names = [b"\xff", b"$other_columns", b"$row_index", b"x\x00y"]
    attrs = schema_attributes(
        [{b"name": name, b"type": b"int64"} for name in names]
        + [{b"name": "счётчик".encode(), b"type": b"int64", b"required": True}]
    )
    spec = make_table_format("skiff", attrs)
    assert [field["name"] for field in skiff_fields(spec)] == [
        "счётчик",
        "$other_columns",
    ]
    assert json.loads(json.dumps(spec)) == spec
    assert [
        c[b"name"] for c in yson.loads(attrs, encoding=None)[b"schema"][:4]
    ] == names


def test_missing_schema_preserves_all_columns_instead_of_using_a_projection():
    spec = make_table_format("skiff", canonical_attributes({}))
    assert skiff_fields(spec) == [{"wire_type": "yson32", "name": "$other_columns"}]


def test_saved_schema_decodes_binary_unknown_values_uint64_and_optional_any():
    attrs = schema_attributes(
        [
            {b"name": b"id", b"type": b"uint64", b"required": True},
            {b"name": b"any", b"type": b"any", b"required": False},
            {
                b"name": b"complex",
                b"type_v3": {b"type_name": b"list", b"item": b"int64"},
            },
        ],
        strict=False,
    )
    spec = make_table_format("skiff", attrs)
    codec = SkiffFormat(attributes=spec["attributes"])
    values = [
        {
            "id": yson.YsonUint64(2**63 - 1),
            "any": None,
            "complex": [1, 2, 3],
            "extra": {"nested": [None, True, {"bytes": b"\x00\xff"}]},
            "decimal": b"\x80\x00\x00\x00\x00\x00\x00\x01",
            "max_unsigned": yson.YsonUint64(2**64 - 1),
            "$other_columns": {"literal_column_name": True},
        },
        {"id": yson.YsonUint64(0), "any": {"v": [None, 7]}, "extra": None},
    ]
    encoded = io.BytesIO()
    for value in values:
        # Construct the documented wire layout independently of our schema builder.
        # The native Python decoder cannot handle dense UINT64_MAX; server
        # export/restore integration tests cover that boundary without bindings.
        encoded.write(struct.pack("<HQ", 0, value["id"]))
        encoded.write(b"\x00" if value["any"] is None else b"\x01")
        if value["any"] is not None:
            payload = yson.dumps(value["any"], yson_format="binary")
            encoded.write(struct.pack("<I", len(payload)) + payload)
        payload = canonical_attributes(
            {key: item for key, item in value.items() if key not in ("id", "any")}
        )
        encoded.write(struct.pack("<I", len(payload)) + payload)
    decoded = list(codec.load_rows(io.BytesIO(encoded.getvalue())))
    assert len(decoded) == len(values)
    for expected, actual in zip(values, decoded):
        result = {key: actual[key] for key in expected}
        result["id"] = yson.YsonUint64(result["id"])
        assert canonical_attributes(result) == canonical_attributes(expected)


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        "skiff",
        {},
        {"name": "arrow", "attributes": {}},
        {"name": "yson", "attributes": {"format": "binary"}},
    ],
)
def test_rejects_unsupported_or_incomplete_format_descriptions(value):
    with pytest.raises(ValueError):
        validate_table_format(value)


@pytest.mark.parametrize(
    "change",
    [
        lambda spec: spec.update({"extra": True}),
        lambda spec: spec["attributes"].update({"enable_type_conversion": True}),
        lambda spec: spec["attributes"].update({"table_skiff_schemas": []}),
        lambda spec: skiff_fields(spec).clear(),
        lambda spec: skiff_fields(spec).append({"wire_type": "int64", "name": "late"}),
        lambda spec: skiff_fields(spec).insert(
            0, {"wire_type": "yson32", "name": "$other_columns"}
        ),
        lambda spec: skiff_fields(spec).insert(
            0, {"wire_type": "int64", "name": "\udcff"}
        ),
        lambda spec: skiff_fields(spec).insert(
            0, {"wire_type": "tuple", "name": "complex", "children": []}
        ),
        lambda spec: skiff_fields(spec).insert(
            0, {"wire_type": "int64", "name": "value", "optional": True}
        ),
        lambda spec: skiff_fields(spec).insert(
            0,
            {
                "wire_type": "variant8",
                "name": "v",
                "children": [{"wire_type": "nothing"}],
            },
        ),
        lambda spec: skiff_fields(spec).extend([skiff_fields(spec)[-1]]),
    ],
)
def test_untrusted_skiff_options_cannot_change_supported_layout_or_drop_fallback(
    change,
):
    spec = make_table_format("skiff", canonical_attributes({}))
    change(spec)
    with pytest.raises(ValueError):
        validate_table_format(spec)


def test_validation_returns_an_independent_schema():
    spec = make_table_format("skiff", canonical_attributes({}))
    saved = deepcopy(spec)
    checked = validate_table_format(spec)
    skiff_fields(checked).clear()
    assert spec == saved


def test_duplicate_dense_names_are_rejected_before_export():
    attrs = schema_attributes(
        [{b"name": b"x", b"type": b"int64"}, {b"name": b"x", b"type": b"string"}]
    )
    with pytest.raises(ValueError, match="Duplicate"):
        make_table_format("skiff", attrs)
