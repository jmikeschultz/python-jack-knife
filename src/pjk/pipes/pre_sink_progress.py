import sys
import time
import threading
from dataclasses import dataclass
from typing import Iterable, Iterator
from pjk.base import Pipe

UPDATE_SECS = 3.0

@dataclass
class ProgressState:
    count: int
    start_time: float
    lock: threading.Lock
    stream: any
    header_printed: bool = False
    # runtime fields will be attached at runtime:
    #   display_started: bool
    #   stop_event: threading.Event
    #   display_thread: threading.Thread

class _ProgressDisplay:
    """Periodic renderer; reads shared state, shows root.num_threads."""
    def __init__(self, root_pipe: "PreSinkProgressPipe", state: ProgressState):
        self.root = root_pipe
        self.state = state

    def _maybe_print_header_locked(self):
        if (not self.state.header_printed):
            header = f"{'total records':>12} {'recs/sec':>12} {'num threads':>12} {'total time':>12}"
            print(header, file=self.state.stream, flush=True)
            print("", file=self.state.stream, flush=True)  # placeholder line for values
            self.state.header_printed = True

    def _print_values_locked(self):
        elapsed = time.time() - self.state.start_time
        rps = (self.state.count / elapsed) if elapsed > 0 else 0.0
        line = f"{self.state.count:12d} {rps:12.2f} {self.root.num_threads:12d} {elapsed:12.2f}"
        print("\033[F" + line, file=self.state.stream, flush=True)

    def run(self):
        # periodic ticks
        while not self.state.stop_event.is_set():
            if self.state.stop_event.wait(UPDATE_SECS):
                break

            with self.state.lock:
                self._maybe_print_header_locked()
                if self.state.header_printed:
                    self._print_values_locked()

        # final refresh on shutdown (even if < UPDATE_SECS)
        with self.state.lock:
            if not self.state.header_printed:
                self._maybe_print_header_locked()
            self._print_values_locked()
            try:
                self.state.stream.flush()
            except Exception:
                pass

class PreSinkProgressPipe(Pipe):
    def __init__(self, batch_size: int = 100, # batches reduce cross thread locking
                 stream=sys.stderr, state: ProgressState = None):
        super().__init__(None, None)
        """
        Counts records across threads; display handled by a separate thread.
        Root (first instance) starts/stops display when num_threads == 1.
        """
        if state is None:
            state = ProgressState(
                count=0,
                start_time=time.time(),
                lock=threading.Lock(),
                stream=stream,
            )
        self.state = state
        self.batch_size = max(1, batch_size)
        self.local_count = 0
        self.num_threads = 1  # stays on ROOT; clones don't touch their own
        self._upstream: Iterable = ()
        self._owns_display = False  # true only on root

        # root-only startup: if first instance (num_threads==1) and not started yet
        if not hasattr(self.state, "display_started"):
            self.state.display_started = False
        if (self.num_threads == 1) and (not self.state.display_started):
            self.state.display_started = True
            self.state.stop_event = threading.Event()
            disp = _ProgressDisplay(self, self.state)
            t = threading.Thread(target=disp.run, name="pjk-progress", daemon=True)
            self.state.display_thread = t
            self._owns_display = True
            t.start()

    def deep_copy(self):
        # clone upstream first
        source_clone = self.left.deep_copy()
        if not source_clone:
            return None

        # new pipe shares state; not root (doesn't own display)
        clone = PreSinkProgressPipe(
            batch_size=self.batch_size,
            stream=self.state.stream,
            state=self.state
        )

        # increment root's thread count
        self.num_threads += 1
        clone.add_source(source_clone)
        return clone

    def __iter__(self) -> Iterator:
        # only counting here
        for record in self.left:
            self.local_count += 1
            if self.local_count >= self.batch_size:
                self._flush_batch()
            yield record
        # no finalize/print; display thread handles rendering

    # ---- internals ----

    def _flush_batch(self):
        lc = self.local_count
        if lc == 0:
            return
        self.local_count = 0
        # counting needs to be atomic across threads
        with self.state.lock:
            self.state.count += lc

    def _update_progress_locked(self):
        # retained for compatibility; display uses equivalent logic
        elapsed = time.time() - self.state.start_time
        rps = (self.state.count / elapsed) if elapsed > 0 else 0.0
        line = f"{self.state.count:12d} {rps:12.2f} {self.num_threads:12d} {elapsed:12.2f}"
        print("\033[F" + line, file=self.state.stream, flush=True)

    def close(self):
        # push leftover local batch
        if self.local_count:
            self._flush_batch()

        # root stops the display thread
        if self._owns_display and getattr(self.state, "display_started", False):
            dt = self.state.display_thread
            self.state.stop_event.set()
            if dt is not None:
                try:
                    dt.join()  # ensure final print happens
                except Exception:
                    pass
        try:
            self.state.stream.flush()
        except Exception:
            pass
