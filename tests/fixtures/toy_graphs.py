"""Small deterministic graphs for unit tests."""

from __future__ import annotations

# Triangle + tail: known structure for modularity tests
TRIANGLE_EDGES = [
    (0, 1, 1.0),
    (1, 2, 1.0),
    (0, 2, 1.0),
    (2, 3, 1.0),
]

# Two cliques connected by bridge
TWO_CLiques_EDGES = [
    (0, 1, 1.0),
    (1, 2, 1.0),
    (0, 2, 1.0),
    (3, 4, 1.0),
    (4, 5, 1.0),
    (3, 5, 1.0),
    (2, 3, 1.0),
]

DIRECTED_SAMPLE_LINES = [
    "# comment\n",
    "1 2\n",
    "2 1\n",
    "3 3\n",
    "3 4\n",
]
