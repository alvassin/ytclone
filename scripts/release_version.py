"""Derive a stable release version from a tag and all subsequent commits."""

import argparse
import re
import subprocess
from pathlib import Path

VERSION_PATTERN = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
DEFAULT_VERSION = "0.1.0"


class VersionError(ValueError):
    """The repository cannot provide a reliable release version."""


def git(*args: str, repo: Path, allowed_returncodes: tuple[int, ...] = (0,)) -> str:
    """Read Git metadata without changing the checkout."""
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode not in allowed_returncodes:
        raise VersionError(result.stderr.strip() or "Git metadata is unavailable")
    return result.stdout.strip()


def parse_version(value: str) -> tuple[int, int, int]:
    """Accept stable SemVer only, without prefixes or leading zeroes."""
    match = VERSION_PATTERN.fullmatch(value)
    if match is None:
        raise VersionError(f"Invalid stable version: {value!r}")
    return tuple(int(part) for part in match.groups())


def release_tags(*, repo: Path) -> dict[str, list[tuple[int, int, int]]]:
    """Map tagged objects to versions, peeling annotated tags completely."""
    refs: dict[str, str] = {}
    output = git(
        "show-ref", "--tags", "--dereference", repo=repo, allowed_returncodes=(0, 1)
    )
    for line in output.splitlines():
        object_id, ref = line.split()
        # A peeled annotated tag follows its tag object in show-ref's output.
        refs[ref.removesuffix("^{}")] = object_id

    versions: dict[str, list[tuple[int, int, int]]] = {}
    for ref, object_id in refs.items():
        tag = ref.removeprefix("refs/tags/")
        if not tag.startswith("v") or VERSION_PATTERN.fullmatch(tag[1:]) is None:
            continue
        version = parse_version(tag[1:])
        versions.setdefault(object_id, []).append(version)
    return versions


def release_version(
    *,
    repo: Path,
    ref: str = "HEAD",
    require_tag: bool = False,
    fallback_version: str = DEFAULT_VERSION,
) -> str:
    """Use the nearest first-parent release tag and count all new commits.

    A tag on a merged feature branch must not switch the release series.
    Multiple release tags on the selected baseline are rejected as ambiguous.
    Untagged checkouts use the initial first-parent commit as their baseline;
    publishing can require an explicit tag instead.
    """
    if git("rev-parse", "--is-shallow-repository", repo=repo) != "false":
        raise VersionError("Full Git history is required; fetch with fetch-depth: 0")
    commit = git(
        "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}", repo=repo
    )
    history = git("rev-list", "--first-parent", commit, repo=repo).splitlines()
    tags = release_tags(repo=repo)
    for baseline in history:
        if baseline in tags:
            candidates = tags[baseline]
            if len(candidates) != 1:
                raise VersionError(
                    f"Multiple release tags on baseline commit {baseline}"
                )
            major, minor, patch = candidates[0]
            break
    else:
        if require_tag:
            raise VersionError(
                "No vMAJOR.MINOR.PATCH release tag on the first-parent history"
            )
        baseline = history[-1]
        major, minor, patch = parse_version(fallback_version)

    distance = int(git("rev-list", "--count", f"{baseline}..{commit}", repo=repo))
    return f"{major}.{minor}.{patch + distance}"


def main() -> None:
    """Print only the calculated version, suitable for shell assignment."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--ref", default="HEAD")
    parser.add_argument("--require-tag", action="store_true")
    parser.add_argument("--fallback-version", default=DEFAULT_VERSION)
    args = parser.parse_args()
    try:
        version = release_version(
            repo=args.repo,
            ref=args.ref,
            require_tag=args.require_tag,
            fallback_version=args.fallback_version,
        )
    except (OSError, VersionError) as error:
        parser.exit(status=1, message=f"release_version: {error}\n")
    print(version)


if __name__ == "__main__":
    main()
