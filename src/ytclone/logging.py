"""Detailed file logging through a background writer."""

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from logging.handlers import QueueHandler, QueueListener
from queue import SimpleQueue

from aiomisc.log import LogFormat
from aiomisc_log import basic_config, create_logging_handler
from yt import logger as yt_logger


@contextmanager
def file_logging(
    *, filename: str, level: str, log_format: str, date_format: str
) -> Iterator[None]:
    """Append detailed logs off the event loop and drain them on shutdown."""
    root = logging.getLogger()
    previous_handlers = root.handlers[:]
    previous_level = root.level
    previous_hook = sys.excepthook
    sdk_logger = yt_logger.LOGGER
    previous_sdk_handlers = sdk_logger.handlers[:]
    previous_sdk_propagate = sdk_logger.propagate
    # Files have no terminal colors; keep JSON when explicitly selected.
    file_format = LogFormat.json if log_format == "json" else LogFormat.stream
    template = create_logging_handler(log_format=file_format, date_format=date_format)
    target = logging.FileHandler(filename=filename, mode="a", encoding="utf-8")
    target.setFormatter(template.formatter)
    template.close()

    queue = SimpleQueue()
    handler = QueueHandler(queue=queue)
    handler.setLevel(level.upper())
    listener = QueueListener(queue, target)
    basic_config(level=level, log_format=LogFormat.disabled, handlers=[handler])
    listener.start()
    # The SDK otherwise writes to its own stderr handler, bypassing root.
    sdk_logger.handlers = []
    sdk_logger.propagate = True
    try:
        yield
    finally:
        sdk_logger.handlers = previous_sdk_handlers
        sdk_logger.propagate = previous_sdk_propagate
        listener.stop()
        target.close()
        handler.close()
        root.handlers = previous_handlers
        root.setLevel(previous_level)
        sys.excepthook = previous_hook
