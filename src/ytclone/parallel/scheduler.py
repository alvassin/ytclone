"""Small-first scheduling without network or process management."""

import heapq
from dataclasses import dataclass

from ..format import DiscoveredNode


@dataclass(frozen=True)
class Job:
    """One source node with a stable identifier across retries."""

    id: int
    node: DiscoveredNode


class Scheduler:
    """Assign the smallest known nodes while reserving room during discovery."""

    def __init__(self, *, small_size: int, reserved_slots: int = 1) -> None:
        """Configure the small-node threshold and reserved worker slots."""
        if type(small_size) is not int or small_size < 0:
            raise ValueError("Small-node size must be a nonnegative integer")
        if type(reserved_slots) is not int or reserved_slots < 0:
            raise ValueError("Reserved slots must be a nonnegative integer")

        self.small_size = small_size
        self.reserved_slots = reserved_slots
        self._pending: list[tuple[int, int, int, Job]] = []
        self._inflight: dict[int, Job] = {}
        self._paths: set[str] = set()
        self._finished_count = 0
        self._next_id = 1

    @property
    def pending_count(self) -> int:
        """Count nodes waiting for a worker."""
        return len(self._pending)

    @property
    def inflight_count(self) -> int:
        """Count assigned nodes whose workers have not reported a result."""
        return len(self._inflight)

    @property
    def finished_count(self) -> int:
        """Count nodes whose final results have been recorded."""
        return self._finished_count

    def add(self, *, node: DiscoveredNode) -> Job:
        """Enqueue a newly discovered node, rejecting duplicate source paths."""
        if node.path in self._paths:
            raise ValueError(f"Source path already scheduled: {node.path}")
        size = node.estimated_size
        if size is not None and (type(size) is not int or size < 0):
            raise ValueError("Estimated size must be a nonnegative integer or None")

        job = Job(id=self._next_id, node=node)
        self._next_id += 1
        self._paths.add(node.path)
        self._push(job=job)
        return job

    def take(
        self, *, active: int, capacity: int, discovery_complete: bool
    ) -> Job | None:
        """Assign the smallest pending node if capacity and reservations allow it."""
        if type(capacity) is not int or capacity < 1:
            raise ValueError("Worker capacity must be a positive integer")
        if type(active) is not int or active < 0:
            raise ValueError("Active workers must be a nonnegative integer")
        if not self._pending or active >= capacity:
            return None

        job = self._pending[0][3]
        if (
            not discovery_complete
            and not self._is_small(job=job)
            and active >= max(0, capacity - self.reserved_slots)
        ):
            return None

        heapq.heappop(self._pending)
        self._inflight[job.id] = job
        return job

    def retry(self, *, job: Job) -> None:
        """Requeue an assigned node after its previous owning process has exited."""
        if self._inflight.get(job.id) != job:
            raise ValueError(f"Job {job.id} is not assigned")

        del self._inflight[job.id]
        self._push(job=job)

    def complete(self, *, job_id: int) -> Job:
        """Record an assigned node's final result, whether success or failure."""
        if job_id not in self._inflight:
            raise ValueError(f"Job {job_id} is not assigned")

        job = self._inflight.pop(job_id)
        self._finished_count += 1
        return job

    def _is_small(self, *, job: Job) -> bool:
        """Treat directories and known sizes below the threshold as small."""
        size = job.node.estimated_size
        return job.node.kind == "map_node" or (
            size is not None and size <= self.small_size
        )

    def _push(self, *, job: Job) -> None:
        """Insert a job with stable size ordering; unknown sizes come last."""
        size = job.node.estimated_size
        if job.node.kind == "map_node":
            priority = (0, 0, job.id, job)
        elif size is None:
            priority = (2, 0, job.id, job)
        else:
            priority = (1, size, job.id, job)

        heapq.heappush(self._pending, priority)
