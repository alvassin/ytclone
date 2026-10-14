"""Small, bounded bridges between async storage and synchronous codecs/SDKs."""

import asyncio
import concurrent.futures
import io
import threading
from collections.abc import AsyncIterator


class AsyncReader(io.RawIOBase):
    """Bridge an async byte stream to a worker with one chunk buffered."""

    def __init__(self, *, chunks: AsyncIterator[bytes], loop):
        """Bind an async byte source to a synchronous reader."""
        super().__init__()
        self._chunks = chunks.__aiter__()
        self._loop = loop
        self._buffer = memoryview(b"")
        self._pending = None
        self._lock = threading.Lock()
        self._cancelled = False
        self.exhausted = False

    def readable(self):
        """Advertise support for reading bytes."""
        return True

    def cancel(self):
        """Interrupt pending input and prevent further reads."""
        with self._lock:
            self._cancelled = True
            if self._pending is not None:
                self._pending.cancel()

    def _next(self):
        """Pull one async chunk from the event loop into the worker."""
        with self._lock:
            if self._cancelled:
                raise InterruptedError("Restore input cancelled")
            self._pending = asyncio.run_coroutine_threadsafe(
                coro=self._chunks.__anext__(), loop=self._loop
            )
            pending = self._pending

        try:
            return pending.result()
        except StopAsyncIteration:
            self.exhausted = True
            return b""
        except concurrent.futures.CancelledError as error:
            raise InterruptedError("Restore input cancelled") from error
        finally:
            with self._lock:
                self._pending = None

    def read(self, size=-1):
        """Return a bounded slice of the buffered input."""
        if size < 0:
            raise ValueError("Unbounded reads are not supported")
        if size == 0:
            return b""

        while not self._buffer and not self.exhausted:
            self._buffer = memoryview(self._next())

        result = bytes(self._buffer[:size])
        self._buffer = self._buffer[len(result) :]
        return result

    def readinto(self, buffer):
        """Fill the supplied buffer with available bytes."""
        data = self.read(size=len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def finish(self):
        """Reach source EOF so size/checksum validation cannot be bypassed."""
        if self._buffer or self.read(size=1):
            raise ValueError("Compressed input was not fully consumed")
