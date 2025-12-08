# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

"""
Class‑based, modular plotting with **drop‑in compatibility** for your existing
`graph_bar_line(obj, type)` call from `GraphSink`.

What you get:
- **No interface change**: `graph_bar_line(obj, type)` still works the same.
- **Classes + reorg** under the hood for readability & testability.
- **Multiple Y fields** via comma‑separated `obj.y_field` (e.g., "rpm,speed,temp").
- **Legacy** single‑Y with per‑row `set_name` still supported.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence
from datetime import date, datetime
from collections import defaultdict

# ----------------------------- Public Params -----------------------------
@dataclass
class GraphParams:
    records: Iterable[Dict[str, Any]]
    x_field: str
    y_fields: Sequence[str]
    x_is_time: Optional[bool] = None
    args_dict: Dict[str, Any] = field(default_factory=dict)
    title: Optional[str] = None


# ----------------------------- Time Detection ----------------------------
class TimeDetector:
    @staticmethod
    def looks_like_datetime_str(s: str) -> bool:
        return bool(
            re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", s) or  # YYYY-MM-DD or YYYY/MM/DD
            re.search(r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}", s) or  # MM-DD-YYYY, etc.
            re.search(r"\d{1,2}:\d{2}", s) or                  # HH:MM present
            ("T" in s) or ("Z" in s)                           # ISO 8601 hints
        )

    @staticmethod
    def is_time(xs: pd.Series) -> bool:
        import numpy as np # lazy
        import pandas as pd # lazy
        # Already datetime dtype?
        if pd.api.types.is_datetime64_any_dtype(xs):
            return True
        # Native datetime-like objects
        if (xs.map(lambda v: isinstance(v, (datetime, date, pd.Timestamp, np.datetime64))).mean() >= 0.9):
            return True
        # String-looking dates
        str_mask = xs.map(lambda v: isinstance(v, str))
        if str_mask.mean() >= 0.9:
            looks_mask = xs[str_mask].map(TimeDetector.looks_like_datetime_str)
            if looks_mask.mean() >= 0.9:
                parsed = pd.to_datetime(xs[str_mask][looks_mask], errors="coerce", utc=False)
                if parsed.notna().mean() >= 0.9:
                    return True
        # Numeric epoch seconds/millis
        num = pd.to_numeric(xs, errors="coerce")
        if num.notna().mean() >= 0.9:
            q05, q95 = num.quantile(0.05), num.quantile(0.95)
            seconds_like = 1e9 <= q05 <= 2.2e9 and 1e9 <= q95 <= 2.2e9
            millis_like  = 1e12 <= q05 <= 2.2e12 and 1e12 <= q95 <= 2.2e12
            if seconds_like or millis_like:
                return True
        return False

    @staticmethod
    def parse_times(series: pd.Series) -> pd.Series:
        import pandas as pd # lazy
        numeric = pd.to_numeric(series, errors="coerce")
        parsed = None
        if numeric.notna().mean() >= 0.9:
            q05, q95 = numeric.quantile(0.05), numeric.quantile(0.95)
            if 1e12 <= q05 <= 2.2e12 and 1e12 <= q95 <= 2.2e12:
                parsed = pd.to_datetime(numeric, unit="ms", errors="coerce", utc=False)
            elif 1e9 <= q05 <= 2.2e9 and 1e9 <= q95 <= 2.2e9:
                parsed = pd.to_datetime(numeric, unit="s", errors="coerce", utc=False)
        if parsed is None:
            parsed = pd.to_datetime(series, errors="coerce", utc=False)
        return parsed


# ----------------------------- Data Adapters -----------------------------
class MultiYAdapter:
    """Builds wide dataframe: columns = ['x'] + y_fields; sums duplicates of x."""
    @staticmethod
    def to_df(records: Iterable[Dict[str, Any]], x_field: str, y_fields: Sequence[str]) -> pd.DataFrame:
        import pandas as pd # lazy
        import numpy as np # lazy
        rows: List[Dict[str, Any]] = []
        for r in records:
            if x_field not in r:
                continue
            row = {"x": r[x_field]}
            has_any = False
            for yf in y_fields:
                v = r.get(yf, None)
                if v is None:
                    row[yf] = np.nan
                else:
                    try:
                        row[yf] = float(v)
                        has_any = True
                    except Exception:
                        row[yf] = np.nan
            if has_any:
                rows.append(row)
        if not rows:
            raise ValueError(f"No usable rows with x='{x_field}' and any of y={list(y_fields)}")
        df = pd.DataFrame(rows)
        num_cols = [c for c in df.columns if c != "x"]
        df = df.groupby("x", as_index=False)[num_cols].sum(min_count=1)
        return df

class SingleYWithSetsAdapter:
    """Legacy: single y_field + optional per-row set_name to create series."""
    @staticmethod
    def to_df(records: Iterable[Dict[str, Any]], x_field: str, y_field: str):
        import pandas as pd # lazy
        triplets = []  # (x, y, set_name)
        for r in records:
            if x_field in r and y_field in r:
                x = r[x_field]
                y = r[y_field]
                s = r.get("set_name", "__default__")
                try:
                    triplets.append((x, float(y), s))
                except Exception:
                    pass
        return pd.DataFrame(triplets, columns=["x", "y", "set"]) if triplets else pd.DataFrame(columns=["x","y","set"])


# ----------------------------- Plotter -----------------------------
class GraphPlotter:
    def __init__(self, params: GraphParams):
        self.pms = params
        self.y_fields = list(dict.fromkeys(self.pms.y_fields))  # dedupe, preserve order

    def plot(self, chart_type: str = "line"):
        import matplotlib.pyplot as plt # lazy
        import matplotlib.dates as mdates # lazy
        import pandas as pd # lazy
        import numpy as np # lazy

        fig = plt.figure()
        ax = plt.gca()

        # Multi-Y path (preferred)
        if len(self.y_fields) > 1:
            df = MultiYAdapter.to_df(self.pms.records, self.pms.x_field, self.y_fields)
            is_time = self.pms.x_is_time if isinstance(self.pms.x_is_time, bool) else TimeDetector.is_time(df["x"])
            if is_time:
                df["ts"] = TimeDetector.parse_times(df["x"])
                df = df.dropna(subset=["ts"]).sort_values("ts")
                if chart_type == "bar":
                    self._bars_time(ax, df, self.y_fields)
                else:
                    self._lines_time(ax, df, self.y_fields)
                self._format_time_axis(ax, df)
            else:
                if chart_type == "bar":
                    self._bars_categorical(ax, df, self.y_fields)
                else:
                    self._lines_categorical(ax, df, self.y_fields)
                self._format_categorical_axis(ax, df)
            title = self.pms.title or ("Line over time" if is_time and chart_type=="line" else
                                     "Bar over time" if is_time else
                                     "Line by category" if chart_type=="line" else
                                     "Bar by category")
            ax.set_title(title)
            ax.set_xlabel(self.pms.x_field)
            ax.set_ylabel(", ".join(self.y_fields))
            ax.legend(title="Series")
            self._apply_args_dict()
            ax.grid(True, linestyle='--', alpha=0.6)
            fig.tight_layout()
            return fig, ax

        # Single-Y legacy path (maybe with set_name)
        y = self.y_fields[0]
        sdf = SingleYWithSetsAdapter.to_df(self.pms.records, self.pms.x_field, y)
        if sdf.empty:
            print(f"No valid '{self.pms.x_field}' and '{y}' records found.")
            return fig, ax

        # time vs categorical
        is_time = self.pms.x_is_time if isinstance(self.pms.x_is_time, bool) else TimeDetector.is_time(sdf["x"])
        if is_time:
            sdf["ts"] = TimeDetector.parse_times(sdf["x"])
            sdf = sdf.dropna(subset=["ts"])  # might be empty
            if sdf.empty:
                print("No parsable datetime values in x_field.")
                return fig, ax
            # aggregate duplicates at same ts per set
            sdf = sdf.groupby(["ts","set"], as_index=False)["y"].sum()
            for sname in sorted(sdf["set"].unique()):
                s = sdf[sdf["set"] == sname].set_index("ts")["y"].sort_index()
                label = None if sname == "__default__" else sname
                if chart_type == 'bar':
                    # Discrete bar bins for time
                    x_vals = s.index.to_numpy(); idx = np.arange(len(x_vals))
                    ax.bar(idx, s.values, width=0.6, label=label, edgecolor='black')
                    ax.set_xticks(idx, [pd.to_datetime(t).strftime("%Y-%m-%d %H:%M") for t in x_vals], rotation=45)
                else:
                    ax.plot(s.index, s.values, label=label)
            self._format_time_axis(ax, sdf.rename(columns={"ts":"ts"}))
            ax.set_title(self.pms.title or f"{y} over time")
            ax.set_xlabel(self.pms.x_field)
            ax.set_ylabel(y)
            if any(s != "__default__" for s in sdf["set"].unique()):
                ax.legend(title="data set")
        else:
            # categorical with legacy sets
            data = defaultdict(float)
            all_x = []
            all_sets = set()
            for _, row in sdf.iterrows():
                data[(row["x"], row["set"])] += row["y"]
                all_sets.add(row["set"])
                all_x.append(row["x"])
            seen = set()
            x_vals = [x for x in all_x if not (x in seen or seen.add(x))]
            set_names = sorted(all_sets)
            idx = np.arange(len(x_vals))
            width = 0.8 / len(set_names) if len(set_names) > 1 else 0.6
            for i, sname in enumerate(set_names):
                heights = [data.get((x, sname), 0) for x in x_vals]
                label = None if sname == "__default__" else sname
                offset = (i - (len(set_names) - 1) / 2) * width
                if chart_type == 'bar':
                    ax.bar(idx + offset, heights, width=width, label=label, edgecolor='black')
                else:
                    ax.plot(idx, heights, marker='o', label=label)
            # tick thinning
            max_ticks = 12
            if len(x_vals) > max_ticks:
                step = int(np.ceil(len(x_vals) / max_ticks))
                tick_idx = idx[::step]
                tick_lbl = [x_vals[i] for i in tick_idx]
            else:
                tick_idx = idx
                tick_lbl = x_vals
            ax.set_xticks(tick_idx, tick_lbl, rotation=45)
            ax.set_title(self.pms.title or f"{y} by {self.pms.x_field}")
            ax.set_xlabel(self.pms.x_field)
            ax.set_ylabel(y)
            if len(set_names) > 1 or "__default__" not in set_names:
                ax.legend(title="data set")

        # common finish
        self._apply_args_dict()
        ax.grid(True, linestyle='--', alpha=0.6)
        fig.tight_layout()
        return fig, ax

    # ---------- Formatting helpers ----------
    @staticmethod
    def _format_time_axis(ax, df: pd.DataFrame) -> None:
        import matplotlib.dates as mdates # lazy
        fig = ax.get_figure()
        ts = df["ts"]
        if ts.empty:
            return
        ts_min, ts_max = ts.min(), ts.max()
        span_hours = max((ts_max - ts_min).total_seconds() / 3600.0, 1)
        if span_hours <= 72:
            major = mdates.HourLocator(interval=6);   fmt = mdates.DateFormatter("%m-%d %H:%M")
        elif span_hours <= 14 * 24:
            major = mdates.HourLocator(interval=12);  fmt = mdates.DateFormatter("%m-%d %H:%M")
        elif span_hours <= 90 * 24:
            major = mdates.DayLocator(interval=1);    fmt = mdates.DateFormatter("%Y-%m-%d")
        else:
            major = mdates.WeekdayLocator(byweekday=mdates.MO, interval=1); fmt = mdates.DateFormatter("%Y-%m-%d")
        ax.xaxis.set_major_locator(major)
        ax.xaxis.set_major_formatter(fmt)
        fig.autofmt_xdate()

    @staticmethod
    def _format_categorical_axis(ax, df: pd.DataFrame) -> None:
        # keeps current xticks set by the bar/line functions; nothing extra here
        pass

    # ---------- Plot variants ----------
    def _lines_time(self, ax, df: pd.DataFrame, y_cols: Sequence[str]) -> None:
        for y in y_cols:
            ax.plot(df["ts"], df[y], label=y)

    def _bars_time(self, ax, df: pd.DataFrame, y_cols: Sequence[str]) -> None:
        # Grouped bars at each timestamp using index positions
        import numpy as np # lazy
        x_vals = df["ts"].to_numpy(); idx = np.arange(len(x_vals))
        n = len(y_cols); width = 0.8 / max(n, 1)
        for i, y in enumerate(y_cols):
            offset = (i - (n - 1) / 2) * width
            heights = df[y].to_numpy()
            ax.bar(idx + offset, heights, width=width, label=y, edgecolor='black')
        ax.set_xticks(idx, [pd.to_datetime(t).strftime("%Y-%m-%d %H:%M") for t in x_vals], rotation=45)

    def _bars_categorical(self, ax, df: pd.DataFrame, y_cols: Sequence[str]) -> None:
        import numpy as np # lazy
        seen = set(); ordered_x: List[Any] = []
        for x in df["x"].tolist():
            if x not in seen:
                seen.add(x); ordered_x.append(x)
        idx = np.arange(len(ordered_x)); n = len(y_cols); width = 0.8 / n
        for i, y in enumerate(y_cols):
            heights = [float(df.loc[df["x"] == x, y].sum()) for x in ordered_x]
            offset = (i - (n - 1) / 2) * width
            ax.bar(idx + offset, heights, width=width, label=y, edgecolor='black')
        ax.set_xticks(idx, ordered_x, rotation=45)

    def _lines_categorical(self, ax, df: pd.DataFrame, y_cols: Sequence[str]) -> None:
        import numpy as np # lazy
        seen = set(); ordered_x: List[Any] = []
        for x in df["x"].tolist():
            if x not in seen:
                seen.add(x); ordered_x.append(x)
        idx = np.arange(len(ordered_x))
        for y in y_cols:
            series = [float(df.loc[df["x"] == x, y].sum()) for x in ordered_x]
            ax.plot(idx, series, marker='o', label=y)
        ax.set_xticks(idx, ordered_x, rotation=45)

    # ---------- Misc ----------
    def _apply_args_dict(self) -> None:
        import matplotlib.pyplot as plt # lazy
        for name, val in getattr(self.pms, "args_dict", {}).items():
            fn = getattr(plt, name, None)
            if callable(fn):
                try:
                    fn(val)
                except Exception:
                    pass


# ----------------------------- Drop-in Wrapper ---------------------------
def graph_bar_line(obj, type):
    """
    Backward-compatible wrapper expected by GraphSink.

    - Reads obj.x_field, obj.y_field, obj.records, optional obj.x_is_time/args_dict/title
    - Supports **comma-separated y fields** in obj.y_field
    - If a **single y** and rows include `set_name`, we use the legacy sets path
    - Otherwise, we use the multi-Y class-based path

    Returns (fig, ax) for optional downstream tweaks (safe to ignore).
    """
    # Lazy import (ensures MPL backend)
    import matplotlib.pyplot as plt  # noqa: F401 # lazy

    # Normalize y_fields from string or list
    raw_y = obj.y_field if isinstance(obj.y_field, str) else str(obj.y_field)
    y_fields = [s.strip() for s in raw_y.split(',') if s.strip()]
    if not y_fields:
        y_fields = [raw_y.strip()] if raw_y.strip() else []
    if not y_fields:
        print("Both x_field and y_field must be specified.")
        return None, None

    # Detect whether legacy set_name is present when using single-Y
    has_sets = False
    if len(y_fields) == 1:
        for _r in getattr(obj, 'records', []):
            if 'set_name' in _r:
                has_sets = True
                break

    params = GraphParams(
        records=getattr(obj, 'records', []),
        x_field=getattr(obj, 'x_field', 'x'),
        y_fields=y_fields,
        x_is_time=getattr(obj, 'x_is_time', None),
        args_dict=getattr(obj, 'args_dict', {}),
        title=getattr(obj, 'title', None),
    )

    plotter = GraphPlotter(params)

    if len(y_fields) == 1 and has_sets:
        # Single-Y with legacy sets path
        return plotter.plot(chart_type=type)

    # Multi-Y or single-Y without sets → normal class path
    return plotter.plot(chart_type=type)
