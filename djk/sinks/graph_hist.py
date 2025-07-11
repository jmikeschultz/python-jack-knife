import matplotlib.pyplot as plt
import numpy as np
import matplotlib.pyplot as plt

def graph_hist(obj):
    if not obj.x_field:
        print("No x field specified.")
        return

    if obj.y_field:
        # Sum mode: aggregate y_field values per x_field bucket
        from collections import defaultdict

        agg = defaultdict(float)
        count = 0
        for r in obj.records:
            if obj.x_field in r and obj.y_field in r:
                try:
                    agg[r[obj.x_field]] += r[obj.y_field]
                    count += 1
                except Exception:
                    pass  # skip malformed record

        if not agg:
            print(f"No valid '{obj.x_field}' and '{obj.y_field}' data for bar chart.")
            return

        x_vals = sorted(agg)
        y_vals = [agg[x] for x in x_vals]

        plt.figure()
        plt.bar(x_vals, y_vals, edgecolor='black')

        ylabel = f"sum({obj.y_field})"
    else:
        # Count mode: standard histogram
        x_vals = [r[obj.x_field] for r in obj.records if obj.x_field in r]
        count = len(x_vals)

        if not x_vals:
            print(f"No valid '{obj.x_field}' data for histogram.")
            return

        bin_width = 1
        min_val = min(x_vals)
        max_val = max(x_vals)
        bins = np.arange(min_val, max_val + bin_width, bin_width)

        plt.figure()
        plt.hist(
            x_vals,
            bins=bins,
            edgecolor='black',
            rwidth=0.8,
            align='mid'
        )

        ylabel = "count"

    for name, val in obj.args_dict.items():
        fn = getattr(plt, name, None)
        if fn and callable(fn):
            fn(val)

    plt.xlabel(obj.x_field)
    plt.ylabel(ylabel)
    plt.text(1.0, 1.0, f"Histogram of {obj.x_field}", transform=plt.gca().transAxes,
            ha='right', va='top', fontsize=10, color='gray')
    plt.text(1.0, 0.95, f"{count} data points", transform=plt.gca().transAxes,
            ha='right', va='top', fontsize=10, color='gray')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()

