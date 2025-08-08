#!/usr/bin/env python3
"""
plot_diversity_metrics.py — revised layout with top‑centre legends.

This version supersedes the one delivered earlier on 2025‑08‑??.

Changes
-------
* Taller figure (3.2×3.4 in) for breathing room.
* Legends now at top‑centre (`loc="upper center"`, `bbox_to_anchor=(0.5, 0.99)`,
  `ncol=2`) to avoid overlap with curves.
* Captions clarify that all 'hash' counts are counts of **unique** 31‑mer
  FracMinHash values.
* Added color coding: teal for primary lines/axes, vermillion for secondary lines/axes.
"""

from __future__ import annotations
import argparse, math, pathlib, sys, pandas as pd, matplotlib.pyplot as plt

HASH_BITS = 64
SCALE     = 1_000
HASH_SPACE = math.ceil(2 ** HASH_BITS / SCALE)

# Color scheme
TEAL = "#008080"
VERMILLION = "#DC143C"

# ──────────────────────────────────────────────────────────────────────────────
def configure_nature_style() -> None:
    plt.rcParams.update({
        "font.family":    "serif",
        "font.size":      7,
        "axes.linewidth": 0.6,
        "xtick.direction": "in",
        "ytick.direction": "in",
        "xtick.major.size": 3,
        "ytick.major.size": 3,
        "grid.linewidth": 0.3,
    })

# ──────────────────────────────────────────────────────────────────────────────
def fig_archive_growth(df: pd.DataFrame, out_prefix: pathlib.Path) -> None:
    caption = (
        "Figure 1 | Archive growth of **unique** 31‑mer FracMinHash values in "
        "the NCBI SRA metagenome collection (scale = 1000, 64‑bit hash). "
        "The solid curve shows the cumulative number of distinct hashes "
        "(log‑scale, left axis).  The dashed curve converts the same counts "
        f"to % of the theoretical hash space (≈ {HASH_SPACE:,.0f} possible "
        "values, right axis).  This view emphasises **data availability**—the "
        "total sequence diversity that is accessible to researchers over time."
    )

    configure_nature_style()
    fig, ax_left = plt.subplots(figsize=(3.2, 3.4))   # wider & taller

    # left axis – cumulative distinct hashes (teal)
    ax_left.plot(df["year"], df["cumulative_hashes"],
                 marker="o", linewidth=1, markersize=3,
                 color=TEAL, label="Distinct hashes")
    ax_left.set_yscale("log")
    ax_left.set_xlabel("Year")
    ax_left.set_ylabel("Cumulative distinct hashes", color=TEAL)
    ax_left.tick_params(axis='y', labelcolor=TEAL)
    ax_left.grid(axis="y", which="major", linestyle=":", alpha=0.6)

    # right axis – hash‑space fraction (vermillion)
    ax_r = ax_left.twinx()
    frac = df["cumulative_hashes"] / HASH_SPACE * 100
    ax_r.plot(df["year"], frac,
              marker="s", markersize=3, linewidth=1, linestyle="--",
              color=VERMILLION, label="Hash space covered")
    ax_r.set_ylabel("Hash space covered (%)", color=VERMILLION)
    ax_r.tick_params(axis='y', labelcolor=VERMILLION)
    ax_r.set_ylim(0, frac.max() * 1.15)

    # unified legend (top‑centre, two columns)
    h1, l1 = ax_left.get_legend_handles_labels()
    h2, l2 = ax_r.get_legend_handles_labels()
    fig.legend(h1 + h2, l1 + l2,
               loc="upper center", bbox_to_anchor=(0.5, 0.99),
               ncol=2, fontsize=6, frameon=False)

    fig.tight_layout(rect=[0, 0, 1, 0.93])   # leave room for legend
    for ext in (".pdf", ".png"):
        fig.savefig(out_prefix.with_suffix(ext), dpi=600 if ext == ".png" else None,
                    metadata={"Title": "Archive growth", "Description": caption})
    plt.close(fig)
    print("\n" + caption + "\n")

# ──────────────────────────────────────────────────────────────────────────────
def fig_discovery_rate(df: pd.DataFrame, out_prefix: pathlib.Path) -> None:
    caption = (
        "Figure 2 | Per‑sample discovery of **unique** 31‑mer hashes. "
        "Solid line: new distinct hashes per metagenome accession each year "
        "(log‑scale).  Dashed line: cumulative distinct hashes divided by "
        "cumulative samples.  A downward trend indicates **detection "
        "saturation**, whereas a stable or rising trend signifies continuing "
        "biological novelty per unit sampling effort—useful for comparative "
        "ecology across time."
    )

    configure_nature_style()
    fig, ax = plt.subplots(figsize=(3.2, 3.4))

    # primary axis – new hashes per sample (teal)
    ax.plot(df["year"], df["hashes_per_sample"],
            marker="o", markersize=3, linewidth=1,
            color=TEAL, label="New hashes / sample")
    ax.set_yscale("log")
    ax.set_xlabel("Year")
    ax.set_ylabel("New hashes per sample", color=TEAL)
    ax.tick_params(axis='y', labelcolor=TEAL)
    ax.grid(axis="y", which="major", linestyle=":", alpha=0.6)

    # secondary axis – cumulative novelty per sample (vermillion)
    ax2 = ax.twinx()
    ax2.plot(df["year"], df["cumulative_hashes_per_sample"],
             marker="s", markersize=3, linewidth=1, linestyle="--",
             color=VERMILLION, label="Cumulative hashes / sample")
    ax2.set_ylabel("Cumulative hashes per sample", color=VERMILLION)
    ax2.tick_params(axis='y', labelcolor=VERMILLION)
    ax2.set_yscale("log")

    # legend at top
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    fig.legend(h1 + h2, l1 + l2,
               loc="upper center", bbox_to_anchor=(0.5, 0.99),
               ncol=2, fontsize=6, frameon=False)

    fig.tight_layout(rect=[0, 0, 1, 0.93])
    for ext in (".pdf", ".png"):
        fig.savefig(out_prefix.with_suffix(ext), dpi=600 if ext == ".png" else None,
                    metadata={"Title": "Discovery rate", "Description": caption})
    plt.close(fig)
    print("\n" + caption + "\n")

# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Plot metagenome diversity metrics.")
    ap.add_argument("--csv", required=True, type=pathlib.Path,
                    help="CSV produced by export_diversity_metrics.py")
    ap.add_argument("--outdir", required=True, type=pathlib.Path,
                    help="Directory for figures")
    args = ap.parse_args()

    if not args.csv.is_file():
        sys.exit(f"CSV not found → {args.csv}")
    args.outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.csv)
    required = {"year", "cumulative_hashes",
                "hashes_per_sample", "cumulative_hashes_per_sample"}
    if not required.issubset(df.columns):
        sys.exit(f"CSV missing columns: {required - set(df.columns)}")

    fig_archive_growth(df, args.outdir / "F1_archive_growth")
    fig_discovery_rate(df, args.outdir / "F2_discovery_rate")
    print("✅  Updated figures written to", args.outdir)

if __name__ == "__main__":
    main()
