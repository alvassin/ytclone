import hashlib
import io
import random
import struct
import tracemalloc

import pytest
import zstandard

from ytclone.yt_zstd import (
    MAX_DECODED_BLOCK_SIZE,
    MAX_ENCODED_BLOCK_SIZE,
    ZstdBlockReader,
    _codec_id,
    _murmur32,
    yt_zstd_codec,
)

CODEC_ID = 23308  # MurmurHash32("zstd08_3"), folded to 16 bits by blockcodec.
CODEC = "z-zstd_3"
MAGIC = b"\x28\xb5\x2f\xfd"


def record(frame, decoded_size, *, codec_id=CODEC_ID):
    return struct.pack("<HQQ", codec_id, len(frame) + 8, decoded_size) + frame


def encoded(data, *, checksum=False):
    frame = zstandard.ZstdCompressor(level=3, write_checksum=checksum).compress(data)
    return record(frame, len(data)), frame


def terminator():
    return encoded(b"")[0]


def raw_frame(data):
    assert len(data) < 256
    header = ((len(data) << 3) | 1).to_bytes(3, "little")
    return MAGIC + b"\x20" + bytes([len(data)]) + header + data


class Source:
    def __init__(self, data, *, fragment=MAX_ENCODED_BLOCK_SIZE, error=None):
        self.stream = io.BytesIO(data)
        self.fragment = fragment
        self.error = error
        self.requests = []
        self.closed = False
        self.eof_reads = 0

    def read(self, size):
        assert 0 < size <= MAX_ENCODED_BLOCK_SIZE
        self.requests.append(size)
        value = self.stream.read(min(size, self.fragment))
        if not value:
            self.eof_reads += 1
            if self.error:
                raise self.error
        return value

    def close(self):
        self.closed = True


@pytest.mark.parametrize(
    ("level", "codec"),
    [
        (-7, "z-zstd_fast_7"),
        (-1, "z-zstd_fast_1"),
        (0, CODEC),
        (3, CODEC),
        (22, "z-zstd_22"),
    ],
)
def test_http_codec_names(level, codec):
    assert yt_zstd_codec(level) == codec


@pytest.mark.parametrize("level", [-8, 23, True, "3", 3.0, None])
def test_invalid_levels(level):
    with pytest.raises(ValueError, match="level"):
        yt_zstd_codec(level)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (b"", 0x00000000),
        (b"a", 0x92685F5E),
        (b"ab", 0x1AA14063),
        (b"abc", 0x13577C9B),
        (b"abcd", 0x26873021),
        (b"hello", 0xE56129CB),
        (b"zstd08_3", 0x526F0963),
    ],
)
def test_murmur_matches_cpp_reference_vectors(value, expected):
    """Vectors were generated with the unsigned C++ MurmurHash2 implementation."""
    assert _murmur32(value) == expected


@pytest.mark.parametrize(
    ("codec", "expected"),
    [
        ("z-zstd_1", 55019),
        (CODEC, CODEC_ID),
        ("z-zstd_22", 34441),
        ("z-zstd_fast_1", 6537),
        ("z-zstd_fast_7", 43946),
    ],
)
def test_canonical_codec_ids(codec, expected):
    assert _codec_id(codec) == expected
    wire, _ = encoded(b"value")
    wire = struct.pack("<H", expected) + wire[2:]
    ending = struct.pack("<H", expected) + terminator()[2:]
    output = ZstdBlockReader(Source(wire + ending), codec=codec).read(1024)
    assert zstandard.ZstdDecompressor().decompress(output) == b"value"


@pytest.mark.parametrize(
    "codec", ["gzip", "zstd", "z-zstd_0", "z-zstd_03", "z-zstd_23", "z-zstd_fast_8"]
)
def test_unknown_codec_names(codec):
    with pytest.raises(ValueError, match="Unsupported"):
        ZstdBlockReader(Source(b""), codec=codec)


@pytest.mark.parametrize("fragment", [1, 7, MAX_ENCODED_BLOCK_SIZE])
@pytest.mark.parametrize("read_size", [1, 13, 32 * 1024 * 1024])
def test_fragmented_transport_and_partial_reads_restore_all_frames(fragment, read_size):
    values = [b"first" * 400, random.Random(3).randbytes(32768), b"last" * 300]
    records, frames = zip(*(encoded(value) for value in values))
    source = Source(b"".join(records) + terminator(), fragment=fragment)
    reader = ZstdBlockReader(source, codec=CODEC)
    assert reader.read(0) == b"" and source.requests == []
    output = b"".join(iter(lambda: reader.read(read_size), b""))
    assert output == b"".join(frames)
    assert source.eof_reads == 1
    assert reader.read(10) == b"" and source.eof_reads == 1
    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(output)) as restored:
        assert restored.read() == b"".join(values)
    reader.close()
    assert not source.closed


def test_empty_response_contains_a_valid_empty_frame():
    source = Source(terminator(), fragment=1)
    reader = ZstdBlockReader(source, codec=CODEC)
    output = reader.read(1024)
    assert output == zstandard.ZstdCompressor(level=3).compress(b"")
    assert zstandard.ZstdDecompressor().decompress(output) == b""
    assert reader.read(1024) == b"" and source.eof_reads == 1


def test_reading_does_not_compress_or_decompress_payload(monkeypatch):
    wire, frame = encoded(b"unchanged compressed bytes" * 1000, checksum=True)
    source = Source(wire + terminator())

    def forbidden(*args, **kwargs):
        pytest.fail("The passthrough reader must not instantiate a codec")

    monkeypatch.setattr(zstandard, "ZstdDecompressor", forbidden)
    monkeypatch.setattr(zstandard, "ZstdCompressor", forbidden)
    assert ZstdBlockReader(source, codec=CODEC).read(1024 * 1024) == frame


@pytest.mark.parametrize("kind", ["raw", "rle"])
def test_uncompressed_zstd_blocks_are_retained(kind):
    data = b"a" * 7
    if kind == "raw":
        frame = raw_frame(data)
    else:
        header = ((len(data) << 3) | 3).to_bytes(3, "little")
        frame = MAGIC + b"\x20" + bytes([len(data)]) + header + b"a"
    output = ZstdBlockReader(
        Source(record(frame, len(data)) + terminator()), codec=CODEC
    ).read(1024)
    assert output == frame
    assert zstandard.ZstdDecompressor().decompress(output) == data


@pytest.mark.parametrize("cut", range(1, 28))
def test_truncated_records_never_reach_successful_eof(cut):
    wire, _ = encoded(b"some rows")
    full = wire + terminator()
    reader = ZstdBlockReader(Source(full[:-cut]), codec=CODEC)
    with pytest.raises(ValueError, match="Truncated|length"):
        while reader.read(3):
            pass
    with pytest.raises(ValueError, match="failed"):
        reader.read(1024)


@pytest.mark.parametrize("wire", [b"", encoded(b"no terminator")[0]])
def test_terminator_is_required(wire):
    reader = ZstdBlockReader(Source(wire), codec=CODEC)
    with pytest.raises(ValueError, match="Truncated"):
        reader.read(1024)


def test_http_error_after_terminator_is_propagated():
    error = RuntimeError("late YT response error")
    wire, _ = encoded(b"body")
    reader = ZstdBlockReader(Source(wire + terminator(), error=error), codec=CODEC)
    with pytest.raises(RuntimeError, match="late YT") as caught:
        reader.read(1024)
    assert caught.value is error
    with pytest.raises(ValueError, match="failed"):
        reader.read(1024)


def test_trailing_data_is_rejected():
    reader = ZstdBlockReader(Source(terminator() + b"extra"), codec=CODEC)
    with pytest.raises(ValueError, match="Trailing"):
        reader.read(1024)


@pytest.mark.parametrize("wrong_end", [False, True])
def test_codec_is_checked_for_every_record(wrong_end):
    wire, frame = encoded(b"body")
    wrong = record(frame, 4, codec_id=CODEC_ID ^ 1)
    source = Source(wire + wrong if wrong_end else wrong + terminator())
    with pytest.raises(ValueError, match="codec ID"):
        ZstdBlockReader(source, codec=CODEC).read(1024)


@pytest.mark.parametrize("size", [0, 7, 16, MAX_ENCODED_BLOCK_SIZE + 1, 2**64 - 1])
def test_encoded_length_is_bounded_before_reading_payload(size):
    source = Source(struct.pack("<HQ", CODEC_ID, size))
    with pytest.raises(ValueError, match="block length"):
        ZstdBlockReader(source, codec=CODEC).read(1024)
    assert source.requests == [10]


def test_decoded_length_is_bounded_before_reading_frame():
    source = Source(struct.pack("<HQQ", CODEC_ID, 17, MAX_DECODED_BLOCK_SIZE + 1))
    with pytest.raises(ValueError, match="decoded block size"):
        ZstdBlockReader(source, codec=CODEC).read(1024)
    assert source.requests == [10, 8]


def invalid_frames():
    frame = raw_frame(b"example")
    checksum = zstandard.ZstdCompressor(write_checksum=True).compress(b"example")
    unknown_size = zstandard.ZstdCompressor(write_content_size=False).compress(
        b"example"
    )
    dictionary = MAGIC + b"\x21\x01\x07" + frame[6:]
    return [
        (b"BAD!" + frame[4:], 7, "magic"),
        (MAGIC + b"\xe0" + b"\x00" * 4, 7, "frame header"),
        (MAGIC + b"\x40\x90\x00\x00\x01\x00\x00", 256, "window"),
        (frame, 8, "content size"),
        (unknown_size, 7, "content size"),
        (dictionary, 7, "dictionary"),
        (frame[:6] + (7 | (7 << 3)).to_bytes(3, "little") + frame[9:], 7, "block"),
        (frame[:6] + (1 | (8 << 3)).to_bytes(3, "little") + frame[9:], 7, "block"),
        (
            frame[:6] + (1 | (6 << 3)).to_bytes(3, "little") + frame[9:-1],
            7,
            "content size",
        ),
        (frame[:6] + (7 << 3).to_bytes(3, "little") + frame[9:], 7, "block header"),
        (frame[:-1], 7, "Truncated"),
        (frame + b"extra", 7, "length"),
        (frame + frame, 7, "length"),
        (checksum[:-1], 7, "checksum trailer"),
    ]


@pytest.mark.parametrize(("frame", "decoded_size", "message"), invalid_frames())
def test_invalid_zstd_structure(frame, decoded_size, message):
    source = Source(record(frame, decoded_size) + terminator())
    with pytest.raises(ValueError, match=message):
        ZstdBlockReader(source, codec=CODEC).read(1024)


def test_empty_terminator_frame_is_validated():
    broken = bytearray(terminator())
    broken[-3] = 7  # Reserved Zstd block type, despite the zero decoded size.
    with pytest.raises(ValueError, match="frame block"):
        ZstdBlockReader(Source(bytes(broken)), codec=CODEC).read(1024)


def test_bounded_reads_do_not_retain_the_entire_response():
    wire, frame = encoded(random.Random(4).randbytes(128 * 1024))
    source = Source(wire * 80 + terminator())
    expected = hashlib.sha256(frame * 80).digest()
    reader = ZstdBlockReader(source, codec=CODEC)
    digest = hashlib.sha256()
    tracemalloc.start()
    try:
        while chunk := reader.read(64 * 1024):
            digest.update(chunk)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert digest.digest() == expected
    assert peak < 2 * 1024 * 1024
    assert max(source.requests) <= MAX_ENCODED_BLOCK_SIZE
    assert not reader._pending


def test_close_does_not_drain_or_close_source():
    wire, _ = encoded(b"body")
    source = Source(wire + terminator())
    reader = ZstdBlockReader(source, codec=CODEC)
    assert reader.read(1) == MAGIC[:1]
    position = source.stream.tell()
    reader.close()
    reader.close()
    assert not source.closed and source.stream.tell() == position
    with pytest.raises(ValueError, match="closed"):
        reader.read(1)


@pytest.mark.parametrize("size", [-1, -2, None, True])
def test_invalid_read_size(size):
    source = Source(terminator())
    with pytest.raises(ValueError, match="Unbounded"):
        ZstdBlockReader(source, codec=CODEC).read(size)
    assert source.requests == []


def test_source_must_respect_requested_size():
    class OversizedSource:
        def read(self, size):
            return b"x" * (size + 1)

    with pytest.raises(ValueError, match="oversized"):
        ZstdBlockReader(OversizedSource(), codec=CODEC).read(1024)
