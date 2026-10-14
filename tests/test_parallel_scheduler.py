"""Pure scheduling rules for changing discovery order and worker failures."""

from dataclasses import replace

import pytest

from ytclone.format import DiscoveredNode
from ytclone.parallel.scheduler import Job, Scheduler


def node(*, name: str, size: int | None, kind: str = "table") -> DiscoveredNode:
    """Build discovery metadata without any network access."""
    return DiscoveredNode(
        path=f"//source/{name}",
        kind=kind,
        node_id=f"id-{name}",
        estimated_size=size,
    )


def take(scheduler: Scheduler, *, complete: bool = True) -> Job | None:
    """Assign one available slot using the scheduler's current active count."""
    return scheduler.take(
        active=scheduler.inflight_count,
        capacity=100,
        discovery_complete=complete,
    )


def test_orders_directories_then_known_sizes_then_unknowns():
    """Unknown sizes must never be mistaken for empty tables."""
    scheduler = Scheduler(small_size=10)
    for name, size, kind in (
        ("unknown-first", None, "table"),
        ("large", 1000, "table"),
        ("small", 5, "file"),
        ("directory", None, "map_node"),
        ("empty", 0, "table"),
        ("unknown-last", None, "file"),
    ):
        scheduler.add(node=node(name=name, size=size, kind=kind))

    jobs = [take(scheduler) for _ in range(6)]

    assert [job.node.path for job in jobs] == [
        "//source/directory",
        "//source/empty",
        "//source/small",
        "//source/large",
        "//source/unknown-first",
        "//source/unknown-last",
    ]
    assert scheduler.pending_count == 0
    assert scheduler.inflight_count == 6
    assert scheduler.finished_count == 0
    assert take(scheduler) is None


def test_equal_sizes_preserve_discovery_order():
    """Equal priorities have deterministic order without comparing node objects."""
    scheduler = Scheduler(small_size=10)
    jobs = [scheduler.add(node=node(name=name, size=5)) for name in ("z", "a", "b")]

    assert [take(scheduler) for _ in jobs] == jobs


def test_new_small_node_jumps_ahead_without_interrupting_large_node():
    """Discovery can change pending order without revoking existing assignments."""
    scheduler = Scheduler(small_size=10)
    running = scheduler.add(node=node(name="running", size=1000))
    pending = scheduler.add(node=node(name="pending", size=2000))
    assert take(scheduler, complete=False) == running

    small = scheduler.add(node=node(name="arrived-later", size=1))

    assert take(scheduler, complete=False) == small
    assert take(scheduler, complete=False) == pending
    assert scheduler.complete(job_id=running.id) == running


@pytest.mark.parametrize("size", [11, None])
def test_reserves_last_slot_for_small_nodes_until_discovery_finishes(size):
    """Large and unknown nodes may use only unreserved capacity during scanning."""
    scheduler = Scheduler(small_size=10, reserved_slots=1)
    first = scheduler.add(node=node(name="first", size=size))
    second = scheduler.add(node=node(name="second", size=size))

    assert scheduler.take(active=0, capacity=2, discovery_complete=False) == first
    assert scheduler.take(active=1, capacity=2, discovery_complete=False) is None
    assert scheduler.pending_count == 1
    assert scheduler.inflight_count == 1

    small = scheduler.add(node=node(name="small", size=10))
    assert scheduler.take(active=1, capacity=2, discovery_complete=False) == small
    assert scheduler.take(active=2, capacity=2, discovery_complete=True) is None

    scheduler.complete(job_id=small.id)
    assert scheduler.take(active=1, capacity=2, discovery_complete=True) == second


def test_one_worker_waits_for_discovery_before_large_node():
    """A fully reserved one-slot pool is released when discovery reaches EOF."""
    scheduler = Scheduler(small_size=10)
    large = scheduler.add(node=node(name="large", size=100))

    assert scheduler.take(active=0, capacity=1, discovery_complete=False) is None
    assert scheduler.take(active=0, capacity=1, discovery_complete=True) == large


def test_directory_can_use_reserved_slot_despite_missing_size():
    """Directory metadata remains eligible even when its subtree size is unknown."""
    scheduler = Scheduler(small_size=0)
    directory = scheduler.add(node=node(name="directory", size=None, kind="map_node"))

    assert scheduler.take(active=0, capacity=1, discovery_complete=False) == directory


def test_reservation_can_be_disabled():
    """Zero reserved slots allows large tasks to consume every available worker."""
    scheduler = Scheduler(small_size=10, reserved_slots=0)
    large = scheduler.add(node=node(name="large", size=100))

    assert scheduler.take(active=0, capacity=1, discovery_complete=False) == large


def test_retry_keeps_id_and_restores_one_assignment():
    """Requeue only the failed owner's job and keep equal-size arrival order."""
    scheduler = Scheduler(small_size=10)
    failed = scheduler.add(node=node(name="failed", size=10))
    healthy = scheduler.add(node=node(name="healthy", size=10))
    waiting = scheduler.add(node=node(name="waiting", size=10))
    assert take(scheduler) == failed
    assert take(scheduler) == healthy

    scheduler.retry(job=failed)

    assert scheduler.pending_count == 2
    assert scheduler.inflight_count == 1
    assert scheduler.finished_count == 0
    assert take(scheduler) == failed
    assert take(scheduler) == waiting
    assert scheduler.complete(job_id=healthy.id) == healthy
    assert scheduler.complete(job_id=failed.id) == failed
    assert scheduler.finished_count == 2


@pytest.mark.parametrize("state", ["pending", "inflight", "finished"])
def test_duplicate_paths_rejected_in_every_state(state):
    """A changed identity at the same discovered path cannot create a second job."""
    scheduler = Scheduler(small_size=10)
    original = node(name="original", size=5)
    job = scheduler.add(node=original)
    if state != "pending":
        assert take(scheduler) == job
    if state == "finished":
        scheduler.complete(job_id=job.id)

    with pytest.raises(ValueError, match="already scheduled"):
        scheduler.add(node=replace(original, node_id="replacement"))


def test_rejects_invalid_transitions_without_losing_pending_or_active_jobs():
    """Invalid completion/retry calls cannot silently duplicate or erase work."""
    scheduler = Scheduler(small_size=10)
    job = scheduler.add(node=node(name="one", size=1))
    with pytest.raises(ValueError, match="not assigned"):
        scheduler.complete(job_id=job.id)
    with pytest.raises(ValueError, match="not assigned"):
        scheduler.retry(job=job)

    assert scheduler.pending_count == 1
    assert take(scheduler) == job
    with pytest.raises(ValueError, match="not assigned"):
        scheduler.retry(job=replace(job, node=node(name="other", size=1)))
    assert scheduler.inflight_count == 1

    scheduler.retry(job=job)
    with pytest.raises(ValueError, match="not assigned"):
        scheduler.retry(job=job)
    assert scheduler.pending_count == 1
    assert take(scheduler) == job
    scheduler.complete(job_id=job.id)

    with pytest.raises(ValueError, match="not assigned"):
        scheduler.complete(job_id=job.id)
    with pytest.raises(ValueError, match="not assigned"):
        scheduler.retry(job=job)
    assert scheduler.pending_count == scheduler.inflight_count == 0
    assert scheduler.finished_count == 1


def test_ids_are_monotonic_across_completions():
    """Completed job identifiers are not reused by later discoveries."""
    scheduler = Scheduler(small_size=10)
    first = scheduler.add(node=node(name="first", size=1))
    assert take(scheduler) == first
    scheduler.complete(job_id=first.id)
    second = scheduler.add(node=node(name="second", size=1))

    assert second.id == first.id + 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"small_size": -1},
        {"small_size": True},
        {"small_size": 0, "reserved_slots": -1},
        {"small_size": 0, "reserved_slots": False},
    ],
)
def test_invalid_settings_rejected(kwargs):
    """Invalid scheduling limits fail before any work can be queued."""
    with pytest.raises(ValueError):
        Scheduler(**kwargs)


@pytest.mark.parametrize("size", [-1, True, 1.5])
def test_invalid_estimated_size_rejected(size):
    """Bad metadata cannot give a node an accidentally elevated priority."""
    scheduler = Scheduler(small_size=10)

    with pytest.raises(ValueError, match="Estimated size"):
        scheduler.add(node=node(name="bad", size=size))
    assert scheduler.pending_count == 0


@pytest.mark.parametrize(
    "active, capacity", [(-1, 1), (0, 0), (0, -1), (True, 1), (0, True)]
)
def test_invalid_capacity_rejected(active, capacity):
    """Bad worker accounting is reported even if the pending queue is empty."""
    scheduler = Scheduler(small_size=10)

    with pytest.raises(ValueError):
        scheduler.take(active=active, capacity=capacity, discovery_complete=False)
