"""File logging keeps console output quiet and preserves error details."""

import json
import logging

import pytest
from yt import logger as yt_logger

from ytclone.logging import file_logging


@pytest.mark.parametrize("log_format", ["stream", "json"])
def test_file_logs_append_and_drain_before_close(tmp_path, capsys, log_format):
    """Buffered records and tracebacks reach the file on both normal and failed runs."""
    path = tmp_path / "copy.log"
    root = logging.getLogger()
    original = root.handlers[:]
    sdk_original = yt_logger.LOGGER.handlers[:]
    sdk_propagate = yt_logger.LOGGER.propagate
    for run in range(2):
        with file_logging(
            filename=str(path),
            level="debug",
            log_format=log_format,
            date_format="%Y-%m-%d %H:%M:%S %z",
        ):
            log = logging.getLogger("ytclone.test")
            log.info("node_completed run=%d", run)
            yt_logger.warning("sdk_retry_detail run=%d", run)
            try:
                raise ValueError("test failure detail")
            except ValueError:
                log.exception("node_failed")
    contents = path.read_text()
    assert "node_completed run=0" in contents
    assert "node_completed run=1" in contents
    assert "ValueError: test failure detail" in contents
    assert "Traceback" in contents
    assert root.handlers == original
    assert yt_logger.LOGGER.handlers == sdk_original
    assert yt_logger.LOGGER.propagate == sdk_propagate
    assert "sdk_retry_detail run=0" in contents
    assert "sdk_retry_detail run=1" in contents
    assert capsys.readouterr() == ("", "")
    if log_format == "json":
        records = [json.loads(line) for line in contents.splitlines()]
        assert len(records) == 6
        assert all("msg" in record for record in records)
