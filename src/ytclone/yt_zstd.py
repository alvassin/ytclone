"""Expose standard Zstd frames from YT's blockcodec HTTP representation."""

import struct
from typing import BinaryIO

import zstandard

# YT normally emits 32 KiB transport blocks. Bound allocations independently of
# the much larger archive parts and reject unexpectedly large transport records.
MAX_ENCODED_BLOCK_SIZE = 4 * 1024 * 1024
MAX_DECODED_BLOCK_SIZE = 128 * 1024 * 1024
_HEADER = struct.Struct("<HQ")
_LENGTH = struct.Struct("<Q")
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


def yt_zstd_codec(level: int) -> str:
    """Return the YT HTTP codec name; Zstd level zero means its default, three."""
    if type(level) is not int or not -7 <= level <= 22:
        raise ValueError("Zstandard level must be between -7 and 22")
    if level < 0:
        return f"z-zstd_fast_{-level}"
    return f"z-zstd_{level or 3}"


def _murmur32(data: bytes) -> int:
    """Match blockcodec's little-endian MurmurHash2 with seed zero."""
    multiplier = 0x5BD1E995
    mask = 0xFFFFFFFF
    value = len(data)
    end = len(data) - len(data) % 4
    for offset in range(0, end, 4):
        word = int.from_bytes(data[offset : offset + 4], "little")
        word = word * multiplier & mask
        word ^= word >> 24
        word = word * multiplier & mask
        value = (value * multiplier & mask) ^ word
    if end < len(data):
        value ^= int.from_bytes(data[end:], "little")
        value = value * multiplier & mask
    value ^= value >> 13
    value = value * multiplier & mask
    return value ^ (value >> 15)


def _codec_id(codec: str) -> int:
    """Hash the canonical codec name, rather than its HTTP alias."""
    for level in range(-7, 23):
        if codec == yt_zstd_codec(level):
            name = f"zstd_fast_{-level}" if level < 0 else f"zstd08_{level or 3}"
            value = _murmur32(name.encode("ascii"))
            return (value >> 16) ^ (value & 0xFFFF)
    raise ValueError(f"Unsupported YT Zstandard codec: {codec}")


def _validate_frame(frame: bytes, *, decoded_size: int) -> None:
    """Check frame boundaries without decoding or validating compressed symbols."""
    if not frame.startswith(_ZSTD_MAGIC):
        raise ValueError("Invalid YT Zstandard frame magic")
    try:
        parameters = zstandard.get_frame_parameters(frame)
        position = zstandard.frame_header_size(frame)
    except zstandard.ZstdError as error:
        raise ValueError("Invalid YT Zstandard frame header") from error
    if parameters.content_size != decoded_size:
        raise ValueError("YT Zstandard frame content size differs from its prefix")
    if parameters.dict_id:
        raise ValueError("YT Zstandard frames must not require a dictionary")
    if parameters.window_size > MAX_DECODED_BLOCK_SIZE:
        raise ValueError("YT Zstandard frame window exceeds the limit")

    block_limit = min(parameters.window_size, 128 * 1024)
    known_size = 0
    compressed = False
    while True:
        if position + 3 > len(frame):
            raise ValueError("Truncated YT Zstandard frame block header")
        header = int.from_bytes(frame[position : position + 3], "little")
        position += 3
        last, kind, size = header & 1, (header >> 1) & 3, header >> 3
        if kind == 3 or size > block_limit:
            raise ValueError("Invalid YT Zstandard frame block")
        if kind == 2:
            if size < 2:
                raise ValueError("Invalid YT Zstandard compressed block size")
            compressed = True
        else:
            known_size += size
            if known_size > decoded_size:
                raise ValueError("YT Zstandard blocks exceed their content size")
        position += 1 if kind == 1 else size
        if position > len(frame):
            raise ValueError("Truncated YT Zstandard frame block")
        if last:
            break

    if not compressed and known_size != decoded_size:
        raise ValueError("YT Zstandard blocks differ from their content size")
    if parameters.has_checksum:
        position += 4
    if position != len(frame):
        raise ValueError("Invalid YT Zstandard frame length or checksum trailer")


class ZstdBlockReader:
    """Strip bounded blockcodec records while retaining their Zstd frames.

    Normal EOF requires the zero-content terminator and the underlying HTTP EOF,
    so late SDK errors still reach the caller. Structural validation detects
    truncation and invalid boundaries; compressed symbols and optional checksums
    are validated only when the archive is decompressed. Closing does not close
    or drain the source, whose lifetime belongs to the snapshot reader.
    """

    def __init__(self, source: BinaryIO, *, codec: str):
        self._source = source
        self._codec_id = _codec_id(codec)
        self._pending = memoryview(b"")
        self._seen_data = False
        self._done = False
        self._closed = False
        self._error: BaseException | None = None

    def _read_exact(self, size: int) -> bytes:
        """Honor short reads without asking the source for an unbounded buffer."""
        first = self._source.read(size)
        if len(first) == size:
            return first
        if not first or len(first) > size:
            raise ValueError("Truncated or oversized YT Zstandard transport read")
        data = bytearray(first)
        while len(data) < size:
            chunk = self._source.read(size - len(data))
            if not chunk or len(chunk) > size - len(data):
                raise ValueError("Truncated or oversized YT Zstandard transport read")
            data.extend(chunk)
        return bytes(data)

    def _next_frame(self) -> bytes:
        """Validate one complete transport record before exposing its payload."""
        codec_id, size = _HEADER.unpack(self._read_exact(_HEADER.size))
        if codec_id != self._codec_id:
            raise ValueError("Unexpected YT Zstandard block codec ID")
        if not _LENGTH.size + 9 <= size <= MAX_ENCODED_BLOCK_SIZE:
            raise ValueError("Invalid YT Zstandard transport block length")
        decoded_size = _LENGTH.unpack(self._read_exact(_LENGTH.size))[0]
        if decoded_size > MAX_DECODED_BLOCK_SIZE:
            raise ValueError("YT Zstandard decoded block size exceeds the limit")
        frame = self._read_exact(size - _LENGTH.size)
        _validate_frame(frame, decoded_size=decoded_size)
        if decoded_size:
            self._seen_data = True
            return frame

        # Reading past the terminator is deliberate: ResponseStream checks the
        # final YT response status at HTTP EOF, after the last payload bytes.
        if self._source.read(1):
            raise ValueError("Trailing data after the YT Zstandard terminator")
        self._done = True
        return b"" if self._seen_data else frame

    def read(self, size: int = -1) -> bytes:
        """Return at most size bytes, aggregating small frames for async callers."""
        if self._closed:
            raise ValueError("Cannot read a closed YT Zstandard reader")
        if type(size) is not int or size < 0:
            raise ValueError("Unbounded reads are not supported")
        if self._error is not None:
            raise ValueError(
                "Cannot reuse a failed YT Zstandard reader"
            ) from self._error
        if size == 0:
            return b""
        result = bytearray()
        try:
            while len(result) < size:
                if self._pending:
                    count = min(size - len(result), len(self._pending))
                    result.extend(self._pending[:count])
                    self._pending = self._pending[count:]
                    if not self._pending:
                        self._pending = memoryview(b"")
                elif self._done:
                    break
                else:
                    self._pending = memoryview(self._next_frame())
        except BaseException as error:
            self._error = error
            self._pending = memoryview(b"")
            raise
        return bytes(result)

    def close(self) -> None:
        """Release buffered bytes without taking ownership of the source."""
        self._closed = True
        self._pending = memoryview(b"")
        self._error = None
