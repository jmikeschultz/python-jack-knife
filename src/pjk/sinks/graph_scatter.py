# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

def graph_scatter(obj):
    import matplotlib.pyplot as plt # lazy imports
    import numpy as np

    valid_records = [r for r in obj.records if obj.x_field in r and obj.y_field in r]
    x_vals = [r[obj.x_field] for r in valid_records]
    y_vals = [r[obj.y_field] for r in valid_records]

    if not x_vals or not y_vals:
        print(f"No valid '{obj.x_field}' and '{obj.y_field}' data for scatter plot.")
        return

    correlation = np.corrcoef(x_vals, y_vals)[0, 1]
    slope, intercept = np.polyfit(x_vals, y_vals, 1)
    regression_line = [slope * x + intercept for x in x_vals]

    plt.figure()
    plt.scatter(x_vals, y_vals, label="Data")
    plt.plot(x_vals, regression_line, color="red", label=f"{obj.y_field} = {slope:.2f}*{obj.x_field} + {intercept:.2f}")
    plt.xlabel(obj.x_field)
    plt.ylabel(obj.y_field)
    plt.title(f"Scatter Plot\nCorrelation: {correlation:.3f}")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

