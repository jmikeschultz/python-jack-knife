import sys
import time
import threading
from typing import Dict, Any

CSI = "\x1b["  # ANSI Control Sequence Introducer

class ProgressDisplay:
    """Periodic renderer that prints all ProgressAPI entries in-place."""

    def __init__(self, interval: float = 3.0, stream=sys.stderr):
        self.api = papi
        self.interval = interval
        self.stream = stream
        self._stop_event = threading.Event()
        self._thread = None
        self._last_lines = 0
        self._use_ansi = hasattr(stream, "isatty") and stream.isatty()  # <-- only use cursor moves on a TTY

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="ProgressDisplay", daemon=True)
        self._thread.start()

    def stop(self, timeout: float | None = 0.1):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            try:
                self._thread.join(timeout=timeout)  # short join so Ctrl-C isn’t delayed
            except Exception:
                pass

    def _run(self):
        while not self._stop_event.is_set():
            snap = self.api.snapshot()
            lines = self._render_lines(snap)

            # Move up to overwrite previous block
            if self._last_lines:
                self.stream.write(f"{CSI}{self._last_lines}F")  # move cursor up N lines, to column 1

            # Write fresh lines
            for line in lines:
                self.stream.write(line + "\n")

            # Erase extra old lines if the block got shorter
            if self._last_lines > len(lines):
                diff = self._last_lines - len(lines)
                for _ in range(diff):
                    self.stream.write(" " * 120 + "\n")
                # move cursor up to top of block again
                self.stream.write(f"{CSI}{self._last_lines}F")

            try:
                self.stream.flush()
            except Exception:
                pass

            self._last_lines = len(lines)

            if self._stop_event.wait(self.interval):
                break

        # --- FINAL REFRESH ON SHUTDOWN ---
        snap = self.api.snapshot()
        lines = self._render_lines(snap)

        if self._last_lines:
            self.stream.write(f"{CSI}{self._last_lines}F")

        for line in lines:
            self.stream.write(line + "\n")

        if self._last_lines > len(lines):
            diff = self._last_lines - len(lines)
            for _ in range(diff):
                self.stream.write(" " * 120 + "\n")
            self.stream.write(f"{CSI}{self._last_lines}F")

        try:
            self.stream.flush()
        except Exception:
            pass

        self._last_lines = len(lines)

    # --- formatting helpers ---

    def _render_lines(self, snap):
        lines = []
        for k, v in snap.items():
            lines.append(self._format_line(k, v))
        return lines

    @staticmethod
    def _format_line(key, rec):
        KEY_W = 15     # left column (component key)
        COL_W = 15     # width per name=value token

        parts = [f"{key:<{KEY_W}}"]
        for name, val in rec.items():
            # if val has .read(), use it (no getattr)
            try:
                v = val.read()
            except AttributeError:
                v = val

            v_str = f"{v:.3f}" if isinstance(v, float) else str(v)
            token = f"{name}={v_str}"          # keep name=value contiguous
            parts.append(f"{token:<{COL_W}}")  # pad AFTER the token to align columns
        return " ".join(parts)


class SafeCounter:
    """
    Dict: tid -> int
    - increment(n): lock only if this thread's key doesn't exist yet.
    - read(): sum without locks; retry if dict size changes during first-time inserts.
    """
    __slots__ = ("_counts", "_lock")

    def __init__(self):
        self._counts: dict[int, int] = {}
        self._lock = threading.Lock()

    def increment(self, n: int = 1) -> None:
        tid = threading.get_ident()
        d = self._counts
        if tid in self._counts:                 # fast path, no lock
            d[tid] += n
            return
        # first time this thread: create under lock (happens once per thread)
        with self._lock:
            d[tid] = d.get(tid, 0) + n

    def read(self) -> int:
        # no lock; retry if a first-time insert resizes during iteration
        while True:
            try:
                return sum(self._counts.values())
            except RuntimeError:
                # "dictionary changed size during iteration" → try again
                continue

    def __str__(self) -> str:
        return str(self.read())

class ElapsedTime:
    def __init__(self):
        self.start = time.time()

    def __str__(self) -> str:
        elapsed = time.time() - self.start
        t = int(elapsed)
        h, r = divmod(t, 3600)
        m, s = divmod(r, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

class ProgressAPI:
    def __init__(self):
        self._store: Dict[str, Dict[str, Any]] = {}

    def get_counter(self, component_label: str, var_label: str) -> SafeCounter:
        report = self._store.setdefault(component_label, {})
        return report.setdefault(var_label, SafeCounter())
    
    def add_elapsed_time(self, component_label, var_label: str) -> ElapsedTime:
        report = self._store.setdefault(component_label, {})
        report.setdefault(var_label, ElapsedTime())

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        return self._store

# singleton
papi = ProgressAPI()

