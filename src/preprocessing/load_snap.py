"""Load SNAP directed edge lists (soc-Pokec)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import numpy as np

from graph.graph import Graph

LARGE_RAW_BYTES = 50 * 1024 * 1024
_READ_BUFFER = 8 * 1024 * 1024


def is_large_raw(path: Path) -> bool:
    return path.is_file() and path.stat().st_size >= LARGE_RAW_BYTES


def iter_directed_edges(path: Path) -> Iterator[tuple[int, int]]:
    """Yield directed edges (u, v) as stored in the SNAP file (one arc per line)."""
    with path.open("rb", buffering=_READ_BUFFER) as f:
        for line in f:
            if not line or line[0:1] == b"#":
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                u = int(parts[0])
                v = int(parts[1])
            except ValueError:
                continue
            if u != v:
                yield u, v


def _read_edges_filtered(
    path: Path,
    nodes: set[int] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    u_list: list[int] = []
    v_list: list[int] = []
    for a, b in iter_directed_edges(path):
        if nodes is None or (a in nodes and b in nodes):
            u_list.append(a)
            v_list.append(b)
    if not u_list:
        empty = np.empty(0, dtype=np.int32)
        return empty, empty
    return np.asarray(u_list, dtype=np.int32), np.asarray(v_list, dtype=np.int32)


def read_edges_coo(path: Path) -> tuple[np.ndarray, np.ndarray]:
    """Read SNAP edge list into directed COO (single pass)."""
    return _read_edges_filtered(path)


def read_edges_coo_subset(
    path: Path,
    nodes: set[int],
) -> tuple[np.ndarray, np.ndarray]:
    """Stream directed edges whose endpoints lie in ``nodes``."""
    if not nodes:
        empty = np.empty(0, dtype=np.int32)
        return empty, empty
    return _read_edges_filtered(path, nodes)


def graph_from_snap_file(path: Path) -> Graph:
    """TXT → numpy COO → out-CSR."""
    src, dst = read_edges_coo(path)
    return Graph.from_coo(src, dst)


def collect_node_set(path: Path) -> set[int]:
    nodes: set[int] = set()
    for u, v in iter_directed_edges(path):
        nodes.add(u)
        nodes.add(v)
    return nodes
