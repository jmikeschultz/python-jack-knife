from djk.base import Sink, Source, PipeSyntaxError
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.pyplot as plt

class GraphSink(Sink):
    def __init__(self, input_source: Source, arg_str: str):
        super().__init__(input_source)
        self.records = []
        self.kind, pairs = arg_str.split(':')
        self.args_dict = dict(item.split('=') for item in pairs.split(','))

        self.x_field = self.args_dict.pop('x', None)
        self.y_field = self.args_dict.pop('y', None)

    def process(self):
        while True:
            record = self.input.next()
            if record is None:
                break
            self.records.append(record)

        if self.kind == "scatter":
            self._scatter()
        elif self.kind == "hist":
            self._hist()
        else:
            raise PipeSyntaxError(f"Unsupported graph type: {self.kind}")

    def _scatter(self):
        x_field = self.args_dict.get('x')
        y_field = self.args_dict.get('y')

        valid_records = [r for r in self.records if self.x_field in r and self.y_field in r]
        x_vals = [r[self.x_field] for r in valid_records]
        y_vals = [r[self.y_field] for r in valid_records]

        if not x_vals or not y_vals:
            print(f"⚠️ No valid '{self.x_field}' and '{self.y_field}' data for scatter plot.")
            return

        correlation = np.corrcoef(x_vals, y_vals)[0, 1]
        slope, intercept = np.polyfit(x_vals, y_vals, 1)
        regression_line = [slope * x + intercept for x in x_vals]

        plt.figure()
        plt.scatter(x_vals, y_vals, label="Data")
        plt.plot(x_vals, regression_line, color="red", label=f"{self.y_field} = {slope:.2f}*{self.x_field} + {intercept:.2f}")
        plt.xlabel(self.x_field)
        plt.ylabel(self.y_field)
        plt.title(f"Scatter Plot\nCorrelation: {correlation:.3f}")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    def _hist(self):
        if not self.x_field:
            print("No x field specified.")
            return

        if self.y_field:
            # Sum mode: aggregate y_field values per x_field bucket
            from collections import defaultdict

            agg = defaultdict(float)
            count = 0
            for r in self.records:
                if self.x_field in r and self.y_field in r:
                    try:
                        agg[r[self.x_field]] += r[self.y_field]
                        count += 1
                    except Exception:
                        pass  # skip malformed record

            if not agg:
                print(f"No valid '{self.x_field}' and '{self.y_field}' data for bar chart.")
                return

            x_vals = sorted(agg)
            y_vals = [agg[x] for x in x_vals]

            plt.figure()
            plt.bar(x_vals, y_vals, edgecolor='black')

            ylabel = f"sum({self.y_field})"
        else:
            # Count mode: standard histogram
            x_vals = [r[self.x_field] for r in self.records if self.x_field in r]
            count = len(x_vals)

            if not x_vals:
                print(f"No valid '{self.x_field}' data for histogram.")
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

        for name, val in self.args_dict.items():
            fn = getattr(plt, name, None)
            if fn and callable(fn):
                fn(val)

        plt.xlabel(self.x_field)
        plt.ylabel(ylabel)
        plt.text(1.0, 1.0, f"Histogram of {self.x_field}", transform=plt.gca().transAxes,
                ha='right', va='top', fontsize=10, color='gray')
        plt.text(1.0, 0.95, f"{count} data points", transform=plt.gca().transAxes,
                ha='right', va='top', fontsize=10, color='gray')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        plt.show()
