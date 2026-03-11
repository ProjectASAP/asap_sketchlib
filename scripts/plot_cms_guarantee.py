#!/usr/bin/env python3
"""Plot CMS within-count guarantee charts from metrics.csv.

Input CSV columns (required):
- path
- distribution
- n
- within_count
- required_within_lower_bound
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


GroupKey = tuple[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot CMS within-count guarantee charts from metrics.csv"
    )
    parser.add_argument("--metrics", required=True, help="Path to metrics.csv")
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory for PNG files (default: metrics.csv parent)",
    )
    return parser.parse_args()


def format_n_label(n: int) -> str:
    if n < 1000:
        return str(n)
    exponent = len(str(n)) - 1
    base = 10**exponent
    if n == base:
        return f"1e{exponent}"
    return f"{n:g}"


def load_metrics(path: Path) -> dict[GroupKey, list[dict[str, float]]]:
    grouped: dict[GroupKey, list[dict[str, float]]] = defaultdict(list)
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "path",
            "distribution",
            "n",
            "within_count",
            "required_within_lower_bound",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"metrics.csv missing columns: {sorted(missing)}")

        for row in reader:
            path_kind = row["path"].strip().lower()
            dist = row["distribution"].strip().lower()

            n = int(row["n"])
            within_count = float(row["within_count"])
            required_within_lower_bound = float(row["required_within_lower_bound"])

            if within_count <= 0.0:
                raise ValueError(
                    f"within_count must be > 0 for log scale; got {within_count} "
                    f"for path={path_kind}, distribution={dist}, n={n}"
                )
            if required_within_lower_bound <= 0.0:
                raise ValueError(
                    "required_within_lower_bound must be > 0 for log scale; "
                    f"got {required_within_lower_bound} for path={path_kind}, "
                    f"distribution={dist}, n={n}"
                )

            grouped[(path_kind, dist)].append(
                {
                    "n": float(n),
                    "within_count": within_count,
                    "required_within_lower_bound": required_within_lower_bound,
                }
            )

    for key in grouped:
        grouped[key].sort(key=lambda r: int(r["n"]))
    return grouped


def plot_group(path_kind: str, dist: str, rows: list[dict[str, float]], out_path: Path) -> None:
    n_values = [int(r["n"]) for r in rows]
    x = list(range(len(rows)))

    fig, ax = plt.subplots(1, 1, figsize=(12, 6), constrained_layout=True)

    ax.bar(
        x,
        [r["within_count"] for r in rows],
        color="#2a9d8f",
        width=0.6,
        label="within_count",
    )
    ax.plot(
        x,
        [r["required_within_lower_bound"] for r in rows],
        color="#e76f51",
        linewidth=2,
        marker="o",
        label="required_within_lower_bound",
    )

    ax.set_yscale("log")
    ax.set_title(f"CMS guarantee ({path_kind}, {dist})")
    ax.set_ylabel("count")
    ax.set_xlabel("stream size N")
    ax.legend()
    ax.grid(alpha=0.2)

    tick_labels = [format_n_label(n) for n in n_values]
    ax.set_xticks(x, tick_labels)

    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    metrics_path = Path(args.metrics).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else metrics_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    grouped = load_metrics(metrics_path)
    if not grouped:
        raise ValueError("metrics.csv is empty")

    plotted = 0
    for path_kind in ("regular", "fast"):
        for dist in ("uniform", "zipf"):
            rows = grouped.get((path_kind, dist))
            if not rows:
                continue
            out_path = out_dir / f"{path_kind}_{dist}_within_count.png"
            plot_group(path_kind, dist, rows, out_path)
            plotted += 1

    if plotted == 0:
        raise ValueError("no supported (path, distribution) groups found in metrics.csv")

    print(f"wrote {plotted} plots to {out_dir}")


if __name__ == "__main__":
    main()
