import sys
import time
import threading
from typing import Dict, Any
from pjk.base import Source, Sink

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
        for (comp_label, id), v in snap.items():
            lines.append(self._format_line(comp_label, v))
        return lines

    @staticmethod
    def _format_line(key, rec):
        KEY_W = 15     # left column width
        COL_W = 20     # width per name=value token (adjust if needed)

        parts = [f"{key:<{KEY_W}.{KEY_W}}"]           # left col, truncated if too long
        for name, val in rec.items():
            token = f"{name}={val}"                   # __str__ handles formatting
            parts.append(f"{token:<{COL_W}}") # left-justify, hard truncate at COL_W
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

class PercentageCounter(SafeCounter):
    def __init__(self, denominator: SafeCounter):
        super().__init__()
        self.denominator = denominator

    # read returns numerator

    def __str__(self):
        numer = self.read()
        denom = self.denominator.read()
        if not denom:
            return f"{numer} (—%)"          # or "0.00%" or whatever you prefer
        pcnt = 100.0 * float(numer) / float(denom)
        return f"{numer} ({pcnt:.1f}%)"

class ProgressAPI:
    def __init__(self):
        self._store: Dict[str, Dict[str, Any]] = {}

    def get_counter(self, component: Source | Sink, var_label: str) -> SafeCounter:
        return self._update_storage(component, var_label=var_label, value=SafeCounter())
    
    # returns the numerator counter
    def get_percentage_counter(self, component: Source | Sink, var_label: str, denom_counter: SafeCounter):
        return self._update_storage(component, var_label=var_label, value=PercentageCounter(denom_counter))
    
    def add_elapsed_time(self, component: Source | Sink, var_label: str) -> ElapsedTime:
        return self._update_storage(component, var_label=var_label, value=ElapsedTime())

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        return self._store
    
    def _update_storage(self, component: Source | Sink, var_label, value: Any):
        # we can have multiple instances of a component type in an expression so we need to
        # differentiate by id when we put them in the _store.
        component_label = self._get_component_label(component)

        if not var_label:
            return value # when var_label None, then we don't want the stat displayed
        
        store_key = (component_label, id(component))

        report = self._store.setdefault(store_key, {})
        return report.setdefault(var_label, value)
    
    def _get_component_label(self, component: Source | Sink):
        if hasattr(type(component), 'extension'):
            format = type(component).extension
            return f'{format}-sink' if isinstance(component, Sink) else f'{format}-source'
        elif hasattr(component, 'usage'):
            return type(component).usage().name
        return type(component).__name__

# singleton
papi = ProgressAPI()

