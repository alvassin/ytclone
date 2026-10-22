"""Portable table formats with an explicit, lossless Skiff fallback."""

from copy import deepcopy

from yt import yson

ROW_FORMAT = {
    "format": "binary",
    "complex_type_mode": "positional",
    "string_keyed_dict_mode": "positional",
    "decimal_mode": "binary",
    "time_mode": "binary",
    "uuid_mode": "binary",
}
TABLE_FORMATS = ("yson", "skiff")
_OTHER_COLUMNS = "$other_columns"
_SCALAR_TYPES = {
    "int64": "int64",
    "uint64": "uint64",
    "double": "double",
    "boolean": "boolean",
    "bool": "boolean",
    "string": "string32",
    "utf8": "string32",
    "any": "yson32",
    "yson": "yson32",
}
_WIRE_TYPES = frozenset(_SCALAR_TYPES.values())


def _text(value: object) -> str | None:
    """Decode names exactly; opaque byte names stay in the YSON fallback."""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return None
    if isinstance(value, str):
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            return None
        return str(value)
    return None


def _column_type(column: dict) -> tuple[str | None, bool]:
    """Recognize simple types without collapsing complex optional values."""
    if b"type_v3" in column:
        logical_type = column[b"type_v3"]
        required = True
        if isinstance(logical_type, dict):
            if logical_type.get(b"type_name") != b"optional":
                return None, False
            logical_type = logical_type.get(b"item")
            required = False
        return _text(logical_type), required
    required = column.get(b"required", False)
    if not isinstance(required, (bool, yson.YsonBoolean)):
        raise TypeError("Invalid required flag in table schema")
    return _text(column.get(b"type")), bool(required)


def make_table_format(name: str, attributes: bytes) -> dict:
    """Choose scalar Skiff fields and retain every other column as binary YSON."""
    if name == "yson":
        return {"name": "yson", "attributes": deepcopy(ROW_FORMAT)}
    if name != "skiff":
        raise ValueError(f"Unsupported table format: {name!r}")
    attrs = yson.loads(string=attributes, encoding=None)
    if not isinstance(attrs, dict):
        raise TypeError("Table attributes must be a YSON map")
    schema = attrs.get(b"schema", [])
    if not isinstance(schema, list):
        raise TypeError("Table schema must be a list")
    children = []
    seen = set()
    for column in schema:
        if not isinstance(column, dict):
            raise TypeError("Table schema columns must be maps")
        column_name = _text(column.get(b"name"))
        if column_name is None or not column_name or column_name.startswith("$"):
            continue
        if "\x00" in column_name:
            continue
        if column_name in seen:
            raise ValueError("Duplicate column name in table schema")
        seen.add(column_name)
        logical_type, required = _column_type(column)
        wire_type = _SCALAR_TYPES.get(logical_type)
        if wire_type is None:
            continue
        field = {"wire_type": wire_type, "name": column_name}
        if not required:
            field = {
                "wire_type": "variant8",
                "name": column_name,
                "children": [{"wire_type": "nothing"}, {"wire_type": wire_type}],
            }
        children.append(field)
    # Also required for strict schemas: complex declared columns use this field.
    children.append({"wire_type": "yson32", "name": _OTHER_COLUMNS})
    return validate_table_format(
        {
            "name": "skiff",
            "attributes": {
                "table_skiff_schemas": [{"wire_type": "tuple", "children": children}]
            },
        }
    )


def validate_table_format(value: dict) -> dict:
    """Validate saved format options without accepting projections or conversions."""
    if not isinstance(value, dict) or value.keys() != {"name", "attributes"}:
        raise ValueError("Invalid table format description")
    name, attributes = value["name"], value["attributes"]
    if name == "yson":
        if attributes != ROW_FORMAT:
            raise ValueError("Unsupported YSON format attributes")
        return {"name": "yson", "attributes": deepcopy(ROW_FORMAT)}
    if name != "skiff":
        raise ValueError("Unsupported table format")
    if not isinstance(attributes, dict) or attributes.keys() != {"table_skiff_schemas"}:
        raise ValueError("Unsupported Skiff format attributes")
    schemas = attributes["table_skiff_schemas"]
    if not isinstance(schemas, list) or len(schemas) != 1:
        raise ValueError("Expected one Skiff table schema")
    schema = schemas[0]
    if (
        not isinstance(schema, dict)
        or schema.keys() != {"wire_type", "children"}
        or schema["wire_type"] != "tuple"
    ):
        raise ValueError("Invalid Skiff table schema")
    children = schema["children"]
    other = {"wire_type": "yson32", "name": _OTHER_COLUMNS}
    if not isinstance(children, list) or not children or children[-1] != other:
        raise ValueError("Skiff schema must preserve other columns")
    seen = set()
    for field in children[:-1]:
        if not isinstance(field, dict):
            raise TypeError("Invalid Skiff field")
        field_name = field.get("name")
        if (
            not isinstance(field_name, str)
            or not field_name
            or _text(field_name) is None
            or field_name.startswith("$")
            or "\x00" in field_name
            or field_name in seen
        ):
            raise ValueError("Invalid or duplicate Skiff field name")
        seen.add(field_name)
        wire_type = field.get("wire_type")
        if isinstance(wire_type, str) and wire_type in _WIRE_TYPES:
            if field.keys() != {"name", "wire_type"}:
                raise ValueError("Unsupported Skiff field attributes")
        elif wire_type == "variant8":
            variants = field.get("children")
            if (
                field.keys() != {"name", "wire_type", "children"}
                or not isinstance(variants, list)
                or len(variants) != 2
                or variants[0] != {"wire_type": "nothing"}
                or not isinstance(variants[1], dict)
                or variants[1].keys() != {"wire_type"}
                or not isinstance(variants[1]["wire_type"], str)
                or variants[1]["wire_type"] not in _WIRE_TYPES
            ):
                raise ValueError("Unsupported optional Skiff field")
        else:
            raise ValueError("Unsupported Skiff wire type")
    return deepcopy(value)


def table_format_name(value: dict | None) -> str:
    """Treat missing format metadata as the legacy fixed binary YSON format."""
    return "yson" if value is None else validate_table_format(value)["name"]
