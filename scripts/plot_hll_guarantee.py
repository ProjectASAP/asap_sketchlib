#!/usr/bin/env python3
"""Plot HLL relative-error guarantee charts from metrics.csv.

Required columns:
- variant
- distribution
- n
- relative_error
- theoretical_rse
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


GroupKey = tuple[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot HLL guarantee charts")
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
            "variant",
            "distribution",
            "n",
            "relative_error",
            "theoretical_rse",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"metrics.csv missing columns: {sorted(missing)}")

        for row in reader:
            variant = row["variant"].strip().lower()
            distribution = row["distribution"].strip().lower()
            n = int(row["n"])
            relative_error = float(row["relative_error"])
            theoretical_rse = float(row["theoretical_rse"])

            if relative_error < 0.0:
                raise ValueError(
                    f"relative_error must be >= 0, got {relative_error} "
                    f"for variant={variant}, distribution={distribution}, n={n}"
                )
            if theoretical_rse < 0.0:
                raise ValueError(
                    f"theoretical_rse must be >= 0, got {theoretical_rse} "
                    f"for variant={variant}, distribution={distribution}, n={n}"
                )

            grouped[(variant, distribution)].append(
                {
                    "n": float(n),
                    "relative_error": relative_error,
                    "theoretical_rse": theoretical_rse,
                }
            )

    for key in grouped:
        grouped[key].sort(key=lambda r: int(r["n"]))

    return grouped


def plot_group(
    variant: str,
    distribution: str,
    rows: list[dict[str, float]],
    out_path: Path,
) -> None:
    n_values = [int(r["n"]) for r in rows]
    x = list(range(len(rows)))

    fig, ax = plt.subplots(1, 1, figsize=(12, 6), constrained_layout=True)

    ax.bar(
        x,
        [r["relative_error"] for r in rows],
        color="#2a9d8f",
        width=0.6,
        label="observed relative_error",
    )
    ax.plot(
        x,
        [r["theoretical_rse"] for r in rows],
        color="#e76f51",
        linewidth=2,
        marker="o",
        label="theoretical_rse",
    )

    ax.set_title(f"HLL relative error ({variant}, {distribution})")
    ax.set_ylabel("relative error")
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
    for variant in ("regular", "datafusion", "hip"):
        for dist in ("uniform", "zipf"):
            rows = grouped.get((variant, dist))
            if not rows:
                continue
            out_path = out_dir / f"{variant}_{dist}_hll_error.png"
            plot_group(variant, dist, rows, out_path)
            plotted += 1

    if plotted == 0:
        raise ValueError("no supported (variant, distribution) groups found in metrics.csv")

    print(f"wrote {plotted} plots to {out_dir}")


if __name__ == "__main__":
    main()
