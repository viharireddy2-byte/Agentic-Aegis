"""Near-real-time ingestion for Agentic Aegis: watch a directory, run the
pipeline per arriving file."""

from .watcher import StreamWatcher

__all__ = ["StreamWatcher"]
