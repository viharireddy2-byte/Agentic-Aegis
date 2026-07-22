import time

from src.streaming import StreamWatcher


def test_stream_watcher_ignores_preexisting_files(tmp_path):
    (tmp_path / "preexisting.csv").write_text("a,b\n1,2\n")
    seen = []
    watcher = StreamWatcher(str(tmp_path), on_new_file=seen.append,
                             pattern="*.csv", poll_interval_seconds=0.15)
    watcher.start()
    time.sleep(0.4)
    watcher.stop()
    assert seen == []


def test_stream_watcher_detects_new_file(tmp_path):
    seen = []
    watcher = StreamWatcher(str(tmp_path), on_new_file=seen.append,
                             pattern="*.csv", poll_interval_seconds=0.15)
    watcher.start()
    (tmp_path / "new_file.csv").write_text("a,b\n3,4\n")
    time.sleep(0.5)
    watcher.stop()
    assert len(seen) == 1
    assert seen[0].endswith("new_file.csv")


def test_stream_watcher_survives_a_failing_callback(tmp_path):
    calls = []

    def flaky(path):
        calls.append(path)
        raise RuntimeError("boom")

    watcher = StreamWatcher(str(tmp_path), on_new_file=flaky,
                             pattern="*.csv", poll_interval_seconds=0.15)
    watcher.start()
    (tmp_path / "a.csv").write_text("x\n1\n")
    (tmp_path / "b.csv").write_text("x\n2\n")
    time.sleep(0.5)
    watcher.stop()
    # both files should still have been handled even though the callback raised
    assert len(calls) == 2
