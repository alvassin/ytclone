"""Validate export exclusions and match whole source subtrees."""

from .format import path_components


def normalize_excludes(*, source: str, paths: tuple[str, ...]) -> tuple[str, ...]:
    """Resolve literal relative or absolute paths strictly beneath the source."""
    path_components(source)
    result = []
    for value in paths:
        path = value.rstrip("/")
        if not path or (path.startswith("/") and not path.startswith("//")):
            raise ValueError(f"Invalid exclusion path: {value!r}")
        if not path.startswith("//"):
            path = source + "/" + path
        path_components(path)
        if any(char in path for char in "\\[]{}<>&*@?"):
            raise ValueError(
                "Exclusions must be literal paths without selectors or wildcards"
            )
        if not path.startswith(source + "/"):
            raise ValueError(f"Exclusion must be beneath the source {source}: {value}")
        if path not in result:
            result.append(path)
    return tuple(result)


def is_excluded(*, path: str, excludes: tuple[str, ...]) -> bool:
    """Match the selected node and descendants, never similarly named siblings."""
    return any(path == prefix or path.startswith(prefix + "/") for prefix in excludes)
