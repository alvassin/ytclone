"""Release versions follow mainline tags while counting merged commits too."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "release_version.py"


def git(repo, *args):
    """Create isolated test histories without depending on user Git settings."""
    return subprocess.run(
        ["git", "-c", "commit.gpgsign=false", *args],
        cwd=repo,
        env={
            **os.environ,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    """Start each test from one initial commit on main."""
    path = tmp_path / "repo with spaces"
    path.mkdir()
    git(path, "init", "--initial-branch=main")
    git(path, "config", "user.name", "Release test")
    git(path, "config", "user.email", "release@example.invalid")
    git(path, "commit", "--allow-empty", "-m", "initial")
    return path


def version(repo, *args, succeeds=True):
    """Exercise the public CLI used by GitHub Actions."""
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == (0 if succeeds else 1), result.stderr
    if succeeds:
        assert not result.stderr
        return result.stdout.strip()
    assert not result.stdout
    return result.stderr


@pytest.mark.parametrize("annotated", [False, True])
def test_release_tag_at_head(repo, annotated):
    """Lightweight and annotated tags both establish the exact base version."""
    extra = ["-a", "-m", "release"] if annotated else []
    git(repo, "tag", *extra, "v1.2.0")
    assert version(repo, "--require-tag") == "1.2.0"


def test_patch_adds_all_commits_to_tag_patch(repo):
    """Existing patch numbers are preserved when adding commit distance."""
    git(repo, "tag", "v1.2.7")
    for index in range(3):
        git(repo, "commit", "--allow-empty", "-m", f"change {index}")
    assert version(repo) == "1.2.10"


def test_merge_counts_side_branch_and_merge_commits(repo):
    """Two feature commits and their merge contribute three patch steps."""
    git(repo, "tag", "v1.2.0")
    git(repo, "checkout", "-b", "feature")
    git(repo, "commit", "--allow-empty", "-m", "first feature change")
    git(repo, "commit", "--allow-empty", "-m", "second feature change")
    git(repo, "checkout", "main")
    git(repo, "merge", "--no-ff", "feature", "-m", "merge feature")
    assert version(repo) == "1.2.3"


def test_tag_from_merged_branch_does_not_switch_release_line(repo):
    """A reachable feature tag must not override a first-parent release tag."""
    git(repo, "tag", "v1.2.0")
    git(repo, "checkout", "-b", "feature")
    git(repo, "commit", "--allow-empty", "-m", "feature change")
    git(repo, "tag", "v9.0.0")
    git(repo, "checkout", "main")
    git(repo, "commit", "--allow-empty", "-m", "main change")
    git(repo, "merge", "--no-ff", "feature", "-m", "merge feature")
    assert version(repo) == "1.2.3"


def test_nearest_mainline_tag_starts_new_series(repo):
    """The nearest mainline tag wins, regardless of tag sorting or timestamps."""
    git(repo, "tag", "v1.2.0")
    git(repo, "commit", "--allow-empty", "-m", "old series change")
    git(repo, "tag", "v1.3.0")
    git(repo, "commit", "--allow-empty", "-m", "new series change")
    assert version(repo) == "1.3.1"


def test_multiple_tags_on_same_commit_are_rejected(repo):
    """Release-series ambiguity cannot silently change a published version."""
    git(repo, "tag", "v1.2.9")
    git(repo, "tag", "v1.10.0")
    assert "Multiple release tags" in version(repo, succeeds=False)


@pytest.mark.parametrize(
    "tag",
    [
        "1.2.0",
        "v1.2",
        "v01.2.0",
        "v1.02.0",
        "v1.2.00",
        "v1.2.0-rc.1",
        "v1.2.0+build",
        "snapshot",
        "release-9.0.0",
    ],
)
def test_nonrelease_tags_are_ignored(repo, tag):
    """Only complete stable SemVer tags with a v prefix set release versions."""
    git(repo, "tag", "v0.1.0")
    git(repo, "commit", "--allow-empty", "-m", "change")
    git(repo, "tag", tag)
    assert version(repo) == "0.1.1"


def test_untagged_history_uses_initial_commit_as_baseline(repo):
    """CI can validate an untagged initial repository without publishing it."""
    assert version(repo) == "0.1.0"
    git(repo, "commit", "--allow-empty", "-m", "change")
    assert version(repo) == "0.1.1"
    assert version(repo, "--fallback-version", "2.3.4") == "2.3.5"


def test_publishing_requires_release_tag(repo):
    """An implicit fallback cannot accidentally establish a published series."""
    assert "No vMAJOR.MINOR.PATCH" in version(repo, "--require-tag", succeeds=False)


def test_invalid_fallback_version_is_rejected(repo):
    """Fallback versions obey the same stable SemVer syntax as release tags."""
    assert "Invalid stable version" in version(
        repo, "--fallback-version", "1.2", succeeds=False
    )


def test_explicit_ref_ignores_later_commits_and_tags(repo):
    """Build the selected commit even when the repository has newer history."""
    git(repo, "tag", "v1.2.0")
    old_head = git(repo, "rev-parse", "HEAD")
    git(repo, "commit", "--allow-empty", "-m", "change")
    git(repo, "tag", "v2.0.0")
    assert version(repo, "--ref", old_head) == "1.2.0"


def test_shallow_checkout_is_rejected(repo, tmp_path):
    """Missing ancestors must fail instead of producing a smaller patch."""
    git(repo, "tag", "v1.2.0")
    git(repo, "commit", "--allow-empty", "-m", "change")
    shallow = tmp_path / "shallow"
    git(tmp_path, "clone", "--depth", "1", repo.as_uri(), str(shallow))
    assert "Full Git history is required" in version(shallow, succeeds=False)
