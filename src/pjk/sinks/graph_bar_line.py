# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import re
from datetime import date, datetime
from collections import defaultdict

def graph_bar_line(obj, type):
    import matplotlib.pyplot as plt # lazy imports
    import matplotlib.dates as mdates
    import numpy as np
    import pandas as pd
    """
    Plot grouped data as a bar or line chart. Automatically detects time-series X
    unless explicitly overridden via `obj.x_is_time = True/False`.

    Required fields on `obj`:
      - obj.records: iterable of dicts
      - obj.x_field: str
      - obj.y_field: str
      - obj.args_dict: optional dict of {matplotlib.pyplot function name -> value}
      - obj.x_is_time: optional bool to force/disable time-series mode

    `type` should be 'bar' or 'line'.
    """
    if not obj.x_field or not obj.y_field:
        print("Both x_field and y_field must be specified.")
        return

    # ---------- collect raw rows ----------
    rows = []  # (x, y, set_name)
    count = 0
    for r in obj.records:
        if obj.x_field in r and obj.y_field in r:
            x = r[obj.x_field]
            y = r[obj.y_field]
            set_name = r.get("set_name", "__default__")
            try:
                rows.append((x, float(y), set_name))
                count += 1
            except Exception:
                pass

    if not rows:
        print(f"No valid '{obj.x_field}' and '{obj.y_field}' records found.")
        return

    plt.figure()

    # ---------- robust time-series detection (without false positives) ----------
    df = pd.DataFrame(rows, columns=["x", "y", "set"])

    # Optional explicit override from caller
    x_is_time_override = getattr(obj, "x_is_time", None)

    def looks_like_datetime_str(s: str) -> bool:
        # quick heuristics for common date/time strings (ISO, with separators, or HH:MM)
        return bool(
            re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", s) or  # YYYY-MM-DD or YYYY/MM/DD
            re.search(r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}", s) or  # MM-DD-YYYY, etc.
            re.search(r"\d{1,2}:\d{2}", s) or                  # HH:MM present
            ("T" in s) or ("Z" in s)                           # ISO 8601 hints
        )

    def detect_time_series(xs: pd.Series) -> bool:
        # Already datetime dtype?
        if pd.api.types.is_datetime64_any_dtype(xs):
            return True

        # Native datetime-like objects
        if (xs.map(lambda v: isinstance(v, (datetime, date, pd.Timestamp, np.datetime64))).mean() >= 0.9):
            return True

        # String-looking dates
        str_mask = xs.map(lambda v: isinstance(v, str))
        if str_mask.mean() >= 0.9:
            looks_mask = xs[str_mask].map(looks_like_datetime_str)
            if looks_mask.mean() >= 0.9:
                parsed = pd.to_datetime(xs[str_mask][looks_mask], errors="coerce", utc=False)
                if parsed.notna().mean() >= 0.9:
                    return True

        # Numeric epoch seconds/millis (avoid generic integer → ns trap)
        num = pd.to_numeric(xs, errors="coerce")
        if num.notna().mean() >= 0.9:
            # use quantiles to ignore a few bad points
            q05, q95 = num.quantile(0.05), num.quantile(0.95)
            # seconds since epoch ~ [1e9, 2e9] for 2001–2033
            seconds_like = 1e9 <= q05 <= 2.2e9 and 1e9 <= q95 <= 2.2e9
            # millis since epoch ~ [1e12, 2e12]
            millis_like = 1e12 <= q05 <= 2.2e12 and 1e12 <= q95 <= 2.2e12
            if seconds_like or millis_like:
                return True

        return False

    if isinstance(x_is_time_override, bool):
        is_time_series = x_is_time_override
    else:
        is_time_series = detect_time_series(df["x"])

    if is_time_series:
        # ===== time-series path (real datetime index) =====
        # Parse with care: handle epoch seconds/millis and strings
        x_series = df["x"]

        # Try numeric epochs first
        numeric = pd.to_numeric(x_series, errors="coerce")
        parsed = None
        if numeric.notna().mean() >= 0.9:
            q05, q95 = numeric.quantile(0.05), numeric.quantile(0.95)
            if 1e12 <= q05 <= 2.2e12 and 1e12 <= q95 <= 2.2e12:
                parsed = pd.to_datetime(numeric, unit="ms", errors="coerce", utc=False)
            elif 1e9 <= q05 <= 2.2e9 and 1e9 <= q95 <= 2.2e9:
                parsed = pd.to_datetime(numeric, unit="s", errors="coerce", utc=False)

        # Fallback to generic parser for strings/datetimes
        if parsed is None:
            parsed = pd.to_datetime(x_series, errors="coerce", utc=False)

        df["ts"] = parsed
        df = df.dropna(subset=["ts"])
        if df.empty:
            print("No parsable datetime values in x_field.")
            return

        # aggregate duplicates at same timestamp per set
        df = df.groupby(["ts", "set"], as_index=False)["y"].sum()
        sets = sorted(df["set"].unique())

        ax = plt.gca()
        for sname in sets:
            s = df[df["set"] == sname].set_index("ts")["y"].sort_index()
            label = None if sname == "__default__" else sname
            # line plot for time series (clean)
            ax.plot(s.index, s.values, label=label)

        # ---- sparse, readable ticks (dynamic) ----
        ts_min, ts_max = df["ts"].min(), df["ts"].max()
        span_hours = max((ts_max - ts_min).total_seconds() / 3600.0, 1)

        if span_hours <= 72:
            major = mdates.HourLocator(interval=6)        # every 6 hours
            fmt   = mdates.DateFormatter("%m-%d %H:%M")
        elif span_hours <= 14 * 24:
            major = mdates.HourLocator(interval=12)       # every 12 hours
            fmt   = mdates.DateFormatter("%m-%d %H:%M")
        elif span_hours <= 90 * 24:
            major = mdates.DayLocator(interval=1)         # daily
            fmt   = mdates.DateFormatter("%Y-%m-%d")
        else:
            major = mdates.WeekdayLocator(byweekday=mdates.MO, interval=1)  # weekly
            fmt   = mdates.DateFormatter("%Y-%m-%d")

        ax.xaxis.set_major_locator(major)
        ax.xaxis.set_major_formatter(fmt)
        # no minor tick labels; rotate to avoid overlap
        plt.gcf().autofmt_xdate()

        plt.xlabel(obj.x_field)
        plt.ylabel(obj.y_field)
        if any(s != "__default__" for s in sets):
            plt.legend(title="data set")
        plt.title(f"{obj.y_field} over time")
        plt.text(1.0, 0.95, f"{count} data points", transform=ax.transAxes,
                 ha='right', va='top', fontsize=10, color='gray')

    else:
        # ===== categorical / numeric path (original behavior, with tick thinning) =====
        data = defaultdict(float)
        all_x = []
        all_sets = set()

        for x, y, set_name in rows:
            data[(x, set_name)] += y
            all_sets.add(set_name)
            all_x.append(x)

        if not data:
            print(f"No valid '{obj.x_field}' and '{obj.y_field}' records found.")
            return

        # preserve order but unique
        seen = set()
        x_vals = [x for x in all_x if not (x in seen or seen.add(x))]
        set_names = sorted(all_sets)
        x_indices = np.arange(len(x_vals))
        width = 0.8 / len(set_names) if len(set_names) > 1 else 0.6

        for i, set_name in enumerate(set_names):
            heights = [data.get((x, set_name), 0) for x in x_vals]
            label = None if set_name == "__default__" else set_name
            offset = (i - (len(set_names) - 1) / 2) * width

            if type == 'bar':
                plt.bar(x_indices + offset, heights, width=width, label=label, edgecolor='black')
            else:
                plt.plot(x_indices, heights, marker='o', label=label)

        # ---- thin xticks to at most 12 labels ----
        max_ticks = 12
        if len(x_vals) > max_ticks:
            step = int(np.ceil(len(x_vals) / max_ticks))
            tick_idx = x_indices[::step]
            tick_lbl = [x_vals[i] for i in tick_idx]
        else:
            tick_idx = x_indices
            tick_lbl = x_vals

        plt.xticks(tick_idx, tick_lbl, rotation=45)
        plt.xlabel(obj.x_field)
        plt.ylabel(obj.y_field)
        if len(set_names) > 1 or "__default__" not in set_names:
            plt.legend(title="data set")
        plt.title(f"{obj.y_field} by {obj.x_field}")
        plt.text(1.0, 0.95, f"{count} data points", transform=plt.gca().transAxes,
                 ha='right', va='top', fontsize=10, color='gray')

    # ---------- optional plt args ----------
    for name, val in getattr(obj, "args_dict", {}).items():
        fn = getattr(plt, name, None)
        if callable(fn):
            try:
                fn(val)
            except Exception:
                pass

    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
