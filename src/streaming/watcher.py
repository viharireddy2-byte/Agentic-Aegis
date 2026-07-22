"""
StreamWatcher — near-real-time ingestion by watching a landing directory
============================================================================

Aegis's core pipeline is batch (extract -> agents -> medallion layers),
which is the right default for a self-healing ETL tool. StreamWatcher
adds a thin streaming-ingestion layer on top without changing that
pipeline at all: it watches a directory for new files and calls a
callback (typically ``run_foundry_pipeline``) once per file, so each
arriving file becomes its own agentic run.

Uses ``watchdog`` for event-driven notification if installed
(``requirements-extra.txt``); otherwise falls back to simple polling so
this module has zero hard dependencies beyond the standard library.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Callable

logger = logging.getLogger("aegis.streaming")

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    _HAS_WATCHDOG = True
except ImportError:  # pragma: no cover - optional dependency
    _HAS_WATCHDOG = False


class StreamWatcher:
    """Watches ``watch_dir`` for new files matching ``pattern`` and calls
    ``on_new_file(path)`` for each one, at most once.

        def handle(path: str) -> None:
            run_foundry_pipeline(raw_path=path)

        watcher = StreamWatcher("data/incoming", on_new_file=handle)
        watcher.start()
        ...
        watcher.stop()
    """

    def __init__(self, watch_dir: str, on_new_file: Callable[[str], None],
                 pattern: str = "*.csv", poll_interval_seconds: float = 2.0) -> None:
        self.watch_dir = Path(watch_dir)
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.on_new_file = on_new_file
        self.pattern = pattern
        self.poll_interval_seconds = poll_interval_seconds
        self._seen: set[str] = set()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._observer = None  # only used in watchdog mode

    def start(self) -> None:
        """Begins watching in a background thread and returns immediately."""
        # Anything already sitting in the directory is treated as already
        # ingested — StreamWatcher only reacts to files that arrive *after*
        # start(), matching typical streaming semantics.
        self._seen = {str(p) for p in self.watch_dir.glob(self.pattern)}

        if _HAS_WATCHDOG:
            self._start_watchdog()
        else:
            logger.info("watchdog not installed, falling back to polling "
                        "every %.1fs. Install with: pip install -r requirements-extra.txt",
                        self.poll_interval_seconds)
            self._start_polling()

    def _start_watchdog(self) -> None:
        handler = _NewFileHandler(self)
        self._observer = Observer()
        self._observer.schedule(handler, str(self.watch_dir), recursive=False)
        self._observer.start()

    def _start_polling(self) -> None:
        def loop() -> None:
            while not self._stop_event.is_set():
                for path in sorted(self.watch_dir.glob(self.pattern)):
                    self._handle_if_new(str(path))
                self._stop_event.wait(self.poll_interval_seconds)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def _handle_if_new(self, path: str) -> None:
        if path in self._seen:
            return
        self._seen.add(path)
        logger.info("New file detected: %s", path)
        try:
            self.on_new_file(path)
        except Exception:  # noqa: BLE001 - one bad file must not kill the watcher
            logger.exception("on_new_file callback failed for %s", path)

    def stop(self) -> None:
        self._stop_event.set()
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=5)
        if self._thread is not None:
            self._thread.join(timeout=self.poll_interval_seconds + 1)


if _HAS_WATCHDOG:
    class _NewFileHandler(FileSystemEventHandler):
        def __init__(self, watcher: StreamWatcher) -> None:
            self.watcher = watcher

        def on_created(self, event) -> None:  # noqa: ANN001 - watchdog API
            if event.is_directory:
                return
            if Path(event.src_path).match(self.watcher.pattern):
                time.sleep(0.2)  # let the writer finish flushing
                self.watcher._handle_if_new(event.src_path)
