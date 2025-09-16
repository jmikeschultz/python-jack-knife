# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

def graph_cumulative(obj):
    import matplotlib.pyplot as plt # lazy import
    
    # Filter and sort records by x
    records = [
        r for r in obj.records
        if obj.x_field in r and obj.y_field in r
    ]
    try:
        sorted_records = sorted(records, key=lambda r: r[obj.x_field])
    except TypeError:
        print(f"Unable to sort records by '{obj.x_field}' — incompatible types.")
        return

    x_vals = []
    y_vals = []
    total = 0
    count = 0
    for r in sorted_records:
        try:
            x = r[obj.x_field]
            y = r[obj.y_field]
            total += y
            x_vals.append(x)
            y_vals.append(total)
            count += 1
        except Exception:
            pass  # silently skip bad records

    if not x_vals:
        print(f"No valid '{obj.x_field}' and '{obj.y_field}' data for cumulative plot.")
        return

    plt.figure()
    plt.plot(x_vals, y_vals, marker='o', linestyle='-', label='Cumulative')

    # Apply user-specified styling functions (e.g. title, xlabel, etc.)
    #for name, val in obj.args_dict.items():
    #    fn = getattr(plt, name, None)
    #    if fn and callable(fn):
    #        fn(val)

    plt.xlabel(obj.x_field)
    plt.ylabel(f"cumulative({obj.y_field})")
    plt.text(1.0, 1.0, f"Cumulative {obj.y_field} over {obj.x_field}", transform=plt.gca().transAxes,
            ha='right', va='top', fontsize=10, color='gray')
    plt.text(1.0, 0.95, f"{count} data points", transform=plt.gca().transAxes,
            ha='right', va='top', fontsize=10, color='gray')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()


