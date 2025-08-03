#!/usr/bin/env python3
"""
plot_rarefaction.py  –  Make a Nature‑style rarefaction figure from CSV data.

Requirements
------------
pip install pandas matplotlib

Usage
-----
python plot_rarefaction.py \
       --csv rarefaction_curve.csv \
       --out rarefaction_curve
"""

from __future__ import annotations
import argparse
import math
import pathlib
import pandas as pd
import matplotlib.pyplot as plt

# --------------------------------------------------------------------------- #
# Constants describing the sketch
HASH_BITS = 64
SCALE = 1_000  # FracMinHash `--scale` used
HASH_SPACE = math.ceil(2 ** HASH_BITS / SCALE)  # total distinct hashes possible


# --------------------------------------------------------------------------- #
def make_plot(df: pd.DataFrame, outfile_prefix: pathlib.Path) -> None:
    """Draw the figure and save to PDF/PNG."""
    # Matplotlib 'Nature‐ish' typography & layout
    plt.rcParams.update({
        "font.family": "serif",
        "font.size": 7,
        "axes.linewidth": 0.6,
        "xtick.direction": "in",
        "ytick.direction": "in",
        "xtick.major.size": 3,
        "ytick.major.size": 3,
        "xtick.minor.size": 1.5,
        "ytick.minor.size": 1.5,
        "grid.linewidth": 0.3,
    })

    # Increased figure height to accommodate legend outside plot area
    fig, ax_left = plt.subplots(figsize=(3.14, 3.0))  # ~80 mm wide, taller

    # Left axis: cumulative hashes (log10 scale)
    ax_left.plot(
        df["year"], df["cumulative_hashes"],
        marker="o", linewidth=1, markersize=3, label="Distinct hashes"
    )
    ax_left.set_yscale("log")
    ax_left.set_xlabel("Year")
    ax_left.set_ylabel("Cumulative distinct hashes")
    ax_left.margins(x=0.03)  # small breathing room
    ax_left.grid(axis="y", which="major", linestyle=":", alpha=0.6)

    # Right axis: fraction of hash space (%)
    ax_right = ax_left.twinx()
    frac = df["cumulative_hashes"] / HASH_SPACE * 100  # percent
    ax_right.plot(
        df["year"], frac,
        marker="s", linewidth=1, markersize=3, linestyle="--",
        label="Hash space sampled"
    )
    ax_right.set_ylabel("Hash space covered (%)")
    ax_right.set_ylim(0, frac.max() * 1.15)  # headroom

    # Unified legend (handles from both axes)
    handles, labels = [], []
    for ax in (ax_left, ax_right):
        h, l = ax.get_legend_handles_labels()
        handles.extend(h)
        labels.extend(l)

    # Position legend outside the plot area at the top
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.95),
               fontsize=6, frameon=False, ncol=2)

    # Adjust layout to make room for the legend
    fig.tight_layout(rect=[0, 0, 1, 0.9])

    for ext in (".pdf", ".png"):
        fig.savefig(outfile_prefix.with_suffix(ext), dpi=600 if ext == ".png" else None)
    plt.close(fig)


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Plot rarefaction curve.")
    ap.add_argument("--csv", required=True, type=pathlib.Path,
                    help="CSV produced by export_rarefaction.py")
    ap.add_argument("--out", required=True, type=pathlib.Path,
                    help="Output file prefix (no extension)")
    args = ap.parse_args()

    if not args.csv.is_file():
        raise SystemExit(f"Input CSV not found: {args.csv}")

    df = pd.read_csv(args.csv)
    if not {"year", "cumulative_hashes"}.issubset(df.columns):
        raise SystemExit("CSV must contain 'year' and 'cumulative_hashes' columns.")

    make_plot(df, args.out)
    print(f"✅  Figure written to {args.out}.pdf and {args.out}.png")


if __name__ == "__main__":
    main()