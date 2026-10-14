"""Bounded JSON messages; table contents never cross the process boundary."""

import asyncio
import json

MAX_MESSAGE_SIZE = 1024 * 1024


def decode_message(line: bytes) -> dict:
    """Validate one complete protocol line without exposing its contents."""
    if len(line) > MAX_MESSAGE_SIZE or not line.endswith(b"\n"):
        raise ValueError("Invalid worker message size or framing")

    value = json.loads(line)
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise TypeError("Worker message must be an object with a type")
    return value


def encode_message(message: dict) -> bytes:
    """Encode a small control message with a trailing newline."""
    data = (json.dumps(obj=message, ensure_ascii=True, allow_nan=False) + "\n").encode()
    if len(data) > MAX_MESSAGE_SIZE:
        raise ValueError("Worker message exceeds size limit")
    return data


async def read_message(reader: asyncio.StreamReader) -> dict | None:
    """Read a message or return None when the peer closes its pipe."""
    line = await reader.readline()
    return decode_message(line) if line else None


async def write_message(writer: asyncio.StreamWriter, *, message: dict) -> None:
    """Send a message with pipe backpressure."""
    writer.write(encode_message(message))
    await writer.drain()
