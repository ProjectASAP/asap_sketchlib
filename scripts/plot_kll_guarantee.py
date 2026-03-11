#!/usr/bin/env python3
"""Plot KLL guarantee charts from metrics.csv and quantiles.csv.

Required metrics.csv columns:
- distribution
- n
- within_count
- required_count

Required quantiles.csv columns:
- distribution
- n
- label
- truth_lower
- truth_upper
- estimate
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot KLL guarantee charts")
    parser.add_argument("--metrics", required=True, help="Path to metrics.csv")
    parser.add_argument(
        "--quantiles",
        default=None,
        help="Path to quantiles.csv (default: sibling of metrics.csv)",
    )
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


def load_metrics(path: Path) -> dict[str, list[dict[str, float]]]:
    grouped: dict[str, list[dict[str, float]]] = defaultdict(list)
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        required = {"distribution", "n", "within_count", "required_count"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"metrics.csv missing columns: {sorted(missing)}")

        for row in reader:
            dist = row["distribution"].strip().lower()
            grouped[dist].append(
                {
                    "n": int(row["n"]),
                    "within_count": float(row["within_count"]),
                    "required_count": float(row["required_count"]),
                }
            )

    for dist in grouped:
        grouped[dist].sort(key=lambda r: r["n"])
    return grouped


def load_quantiles(path: Path) -> dict[tuple[str, int], list[dict[str, float | str]]]:
    grouped: dict[tuple[str, int], list[dict[str, float | str]]] = defaultdict(list)
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "distribution",
            "n",
            "label",
            "truth_lower",
            "truth_upper",
            "estimate",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"quantiles.csv missing columns: {sorted(missing)}")

        for row in reader:
            dist = row["distribution"].strip().lower()
            n = int(row["n"])
            grouped[(dist, n)].append(
                {
                    "label": row["label"].strip(),
                    "truth_lower": float(row["truth_lower"]),
                    "truth_upper": float(row["truth_upper"]),
                    "estimate": float(row["estimate"]),
                }
            )

    return grouped


def plot_summary(dist: str, rows: list[dict[str, float]], out_path: Path) -> None:
    n_values = [int(r["n"]) for r in rows]
    x = list(range(len(rows)))

    fig, ax = plt.subplots(1, 1, figsize=(10, 5), constrained_layout=True)

    ax.bar(
        x,
        [r["within_count"] for r in rows],
        color="#2a9d8f",
        width=0.6,
        label="within_count",
    )
    ax.plot(
        x,
        [r["required_count"] for r in rows],
        color="#e76f51",
        linewidth=2,
        marker="o",
        label="required_count",
    )

    ax.set_title(f"KLL guarantee summary ({dist})")
    ax.set_ylabel("quantile count")
    ax.set_xlabel("stream size N")
    ax.legend()
    ax.grid(alpha=0.2)

    tick_labels = [format_n_label(n) for n in n_values]
    ax.set_xticks(x, tick_labels)

    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_detail(
    dist: str,
    n: int,
    rows: list[dict[str, float | str]],
    out_path: Path,
) -> None:
    x = list(range(len(rows)))
    labels = [str(r["label"]) for r in rows]
    lower = [float(r["truth_lower"]) for r in rows]
    upper = [float(r["truth_upper"]) for r in rows]
    estimate = [float(r["estimate"]) for r in rows]

    fig, ax = plt.subplots(1, 1, figsize=(10, 5), constrained_layout=True)

    ax.fill_between(x, lower, upper, color="#d9d9d9", alpha=0.45, label="truth bound band")
    ax.plot(x, lower, color="#6c757d", linewidth=1.5, label="truth_lower")
    ax.plot(x, upper, color="#495057", linewidth=1.5, label="truth_upper")
    ax.plot(x, estimate, color="#1d3557", marker="o", linewidth=2, label="estimate")

    ax.set_title(f"KLL quantile bounds ({dist}, N={n})")
    ax.set_ylabel("value")
    ax.set_xlabel("quantile")
    ax.legend()
    ax.grid(alpha=0.2)
    ax.set_xticks(x, labels)

    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    metrics_path = Path(args.metrics).resolve()
    quantiles_path = (
        Path(args.quantiles).resolve() if args.quantiles else metrics_path.with_name("quantiles.csv")
    )
    out_dir = Path(args.out_dir).resolve() if args.out_dir else metrics_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics_grouped = load_metrics(metrics_path)
    if not metrics_grouped:
        raise ValueError("metrics.csv is empty")

    quantiles_grouped = load_quantiles(quantiles_path)
    if not quantiles_grouped:
        raise ValueError("quantiles.csv is empty")

    summary_plotted = 0
    for dist in ("uniform", "zipf"):
        rows = metrics_grouped.get(dist)
        if not rows:
            continue
        plot_summary(dist, rows, out_dir / f"{dist}_within_count.png")
        summary_plotted += 1

    detail_plotted = 0
    for dist in ("uniform", "zipf"):
        n_values = sorted({n for (d, n) in quantiles_grouped.keys() if d == dist})
        for n in n_values:
            rows = quantiles_grouped[(dist, n)]
            plot_detail(dist, n, rows, out_dir / f"{dist}_n{n}_quantile_bounds.png")
            detail_plotted += 1

    if summary_plotted == 0 and detail_plotted == 0:
        raise ValueError("no plots generated from provided CSV files")

    print(f"wrote {summary_plotted} summary plots and {detail_plotted} detail plots to {out_dir}")


if __name__ == "__main__":
    main()
