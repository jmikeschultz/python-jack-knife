from types import SimpleNamespace

import matplotlib
import pytest

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pjk.sinks.graph_hist import graph_hist


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Similarity distribution", "Similarity distribution"),
        (None, "Histogram of score"),
    ],
)
def test_histogram_uses_configured_title_or_default(title, expected):
    graph = SimpleNamespace(
        records=[{"score": 1}, {"score": 2}],
        x_field="score",
        y_field="missing",
        xlog=False,
        ylog=False,
        title=title,
    )

    try:
        graph_hist(graph)
        assert plt.gca().get_title() == expected
    finally:
        plt.close("all")
