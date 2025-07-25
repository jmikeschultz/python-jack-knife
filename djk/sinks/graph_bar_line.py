# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np

def graph_bar_line(obj, type):
    if not obj.x_field or not obj.y_field:
        print("Both x_field and y_field must be specified.")
        return

    # Bucket data by (x, set_name)
    data = defaultdict(float)
    all_x = set()
    all_sets = set()
    count = 0

    for r in obj.records:
        if obj.x_field in r and obj.y_field in r:
            x = r[obj.x_field]
            y = r[obj.y_field]
            set_name = r.get("set_name", "__default__")
            try:
                data[(x, set_name)] += y
                all_x.add(x)
                all_sets.add(set_name)
                count += 1
            except Exception:
                pass

    if not data:
        print(f"No valid '{obj.x_field}' and '{obj.y_field}' records found.")
        return

    x_vals = sorted(all_x)
    set_names = sorted(all_sets)
    x_indices = np.arange(len(x_vals))
    width = 0.8 / len(set_names) if len(set_names) > 1 else 0.6

    plt.figure()

    for i, set_name in enumerate(set_names):
        heights = [data.get((x, set_name), 0) for x in x_vals]
        label = None if set_name == "__default__" else set_name
        offset = (i - (len(set_names) - 1) / 2) * width

        if type == 'bar':
            plt.bar(
                x_indices + offset,
                heights,
                width=width,
                label=label,
                edgecolor='black'
            )
        else:
            plt.plot(
                x_indices,
                heights,
                marker='o',
                label=label
            )

    plt.xticks(x_indices, x_vals, rotation=45)
    plt.xlabel(obj.x_field)
    plt.ylabel(obj.y_field)
    if len(set_names) > 1 or "__default__" not in set_names:
        plt.legend(title="data set")
    plt.title(f"{obj.y_field} by {obj.x_field}")
    plt.text(1.0, 0.95, f"{count} data points", transform=plt.gca().transAxes,
             ha='right', va='top', fontsize=10, color='gray')

    for name, val in obj.args_dict.items():
        fn = getattr(plt, name, None)
        if fn and callable(fn):
            fn(val)

    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()
