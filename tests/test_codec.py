import asyncio

import pytest

from ytclone.codec import AsyncReader


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=5))


def test_reader_bounds_and_eof_validation():
    async def main():
        called = []

        async def chunks():
            for value in (b"", b"abcdef", b"ghij"):
                called.append(value)
                yield value
            called.append("validated")

        reader = AsyncReader(chunks=chunks(), loop=asyncio.get_running_loop())
        assert await asyncio.to_thread(reader.read, 2) == b"ab"
        assert called == [b"", b"abcdef"]
        assert await asyncio.to_thread(reader.read, 100) == b"cdef"
        assert await asyncio.to_thread(reader.read, 100) == b"ghij"
        await asyncio.to_thread(reader.finish)
        assert reader.exhausted
        assert called[-1] == "validated"

    run(main())


def test_reader_propagates_checksum_failure_at_eof():
    async def main():
        async def chunks():
            yield b"value"
            raise ValueError("checksum mismatch")

        reader = AsyncReader(chunks=chunks(), loop=asyncio.get_running_loop())
        assert await asyncio.to_thread(reader.read, 100) == b"value"
        with pytest.raises(ValueError, match="checksum"):
            await asyncio.to_thread(reader.finish)

    run(main())


def test_reader_cancellation_unblocks_worker():
    async def main():
        reading = asyncio.Event()
        closed = asyncio.Event()

        async def chunks():
            try:
                reading.set()
                await asyncio.Event().wait()
                yield b"never"
            finally:
                closed.set()

        reader = AsyncReader(chunks=chunks(), loop=asyncio.get_running_loop())
        task = asyncio.create_task(asyncio.to_thread(reader.read, 10))
        await reading.wait()
        reader.cancel()
        with pytest.raises(InterruptedError):
            await task
        await closed.wait()

    run(main())


def test_reader_rejects_unbounded_reads_and_unconsumed_input():
    async def main():
        async def chunks():
            yield b"value"

        reader = AsyncReader(chunks=chunks(), loop=asyncio.get_running_loop())
        with pytest.raises(ValueError, match="Unbounded"):
            reader.read()
        with pytest.raises(ValueError, match="fully consumed"):
            await asyncio.to_thread(reader.finish)

    run(main())
