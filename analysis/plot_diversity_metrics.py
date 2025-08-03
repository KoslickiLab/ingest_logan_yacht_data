#!/usr/bin/env python3
"""
plot_diversity_metrics.py  –  Visualise archive growth and discovery saturation.

Creates two figures:

F1_archive_growth.pdf / .png
    • cumulative distinct hashes (log‑scale, left y‑axis)
    • fraction of hash space covered (right y‑axis)

F2_discovery_rate.pdf / .png
    • new hashes per sample (log‑scale)
    • cumulative hashes per cumulative sample (secondary axis)

Captions are printed to stdout and embedded as PDF metadata for easy
copy‑and‑paste into manuscripts.
"""
from __future__ import annotations
import argparse, math, pathlib, sys, pandas as pd, matplotlib.pyplot as plt

HASH_BITS = 64
SCALE     = 1_000
HASH_SPACE = math.ceil(2 ** HASH_BITS / SCALE)

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
        "Figure 1 | Archive growth of metagenomic sequence diversity in the "
        "NCBI SRA. The solid curve shows the cumulative number of distinct "
        "31‑mer FracMinHash values (scale = 1000, 64‑bit hash) discovered up "
        "to each calendar year (log scale). The dashed curve maps the same "
        "counts to the percentage of the theoretical hash space "
        f"(≈ {HASH_SPACE:,.0f} possible values). "
        "This panel answers the data‑centric question: "
        "“How much *accessible* sequence diversity has the archive amassed?”"
    )

    configure_nature_style()
    fig, ax_left = plt.subplots(figsize=(3.2, 2.6))

    # left axis – cumulative hashes
    ax_left.plot(df["year"], df["cumulative_hashes"],
                 marker="o", linewidth=1, markersize=3,
                 label="Distinct hashes")
    ax_left.set_yscale("log")
    ax_left.set_xlabel("Year")
    ax_left.set_ylabel("Cumulative distinct hashes")
    ax_left.grid(axis="y", which="major", linestyle=":", alpha=0.6)

    # right axis – hash‑space fraction
    ax_r = ax_left.twinx()
    frac = df["cumulative_hashes"] / HASH_SPACE * 100
    ax_r.plot(df["year"], frac,
              marker="s", markersize=3, linewidth=1, linestyle="--",
              label="Hash space covered")
    ax_r.set_ylabel("Hash space covered (%)")
    ax_r.set_ylim(0, frac.max() * 1.15)

    # legend
    h1, l1 = ax_left.get_legend_handles_labels()
    h2, l2 = ax_r.get_legend_handles_labels()
    fig.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=6, frameon=False)

    fig.tight_layout(pad=0.5)
    for ext in (".pdf", ".png"):
        fname = out_prefix.with_suffix(ext)
        fig.savefig(fname, dpi=600 if ext == ".png" else None,
                    metadata={"Title": "Archive growth", "Description": caption})
    plt.close(fig)
    print("\n" + caption + "\n")

# ──────────────────────────────────────────────────────────────────────────────
def fig_discovery_rate(df: pd.DataFrame, out_prefix: pathlib.Path) -> None:
    caption = (
        "Figure 2 | Per‑sample discovery of novel 31‑mer hashes. "
        "The solid line shows the *new* hashes discovered in each year divided "
        "by the number of metagenome accessions submitted that year "
        "(log scale). A declining trend indicates diminishing returns of "
        "additional sequencing—evidence of detection saturation. "
        "The dashed curve shows the cumulative hashes per cumulative sample, "
        "capturing long‑term biological novelty normalised by sampling effort; "
        "this metric is useful for comparative ecology."
    )

    configure_nature_style()
    fig, ax = plt.subplots(figsize=(3.2, 2.6))

    ax.plot(df["year"], df["hashes_per_sample"],
            marker="o", markersize=3, linewidth=1,
            label="New hashes per sample")
    ax.set_yscale("log")
    ax.set_xlabel("Year")
    ax.set_ylabel("Novel hashes per sample")
    ax.grid(axis="y", which="major", linestyle=":", alpha=0.6)

    # secondary axis – cumulative novelty per sample
    ax2 = ax.twinx()
    ax2.plot(df["year"], df["cumulative_hashes_per_sample"],
             marker="s", markersize=3, linewidth=1, linestyle="--",
             label="Cumulative hashes per sample")
    ax2.set_ylabel("Cumulative hashes per sample")
    ax2.set_yscale("log")

    # legend
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    fig.legend(h1 + h2, l1 + l2, loc="upper right", fontsize=6, frameon=False)

    fig.tight_layout(pad=0.5)
    for ext in (".pdf", ".png"):
        fname = out_prefix.with_suffix(ext)
        fig.savefig(fname, dpi=600 if ext == ".png" else None,
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
    expected = {"year", "cumulative_hashes", "hashes_per_sample",
                "cumulative_hashes_per_sample"}
    if not expected.issubset(df.columns):
        sys.exit(f"CSV missing columns: {expected - set(df.columns)}")

    fig_archive_growth(df, args.outdir / "F1_archive_growth")
    fig_discovery_rate(df, args.outdir / "F2_discovery_rate")

    print("✅  Figures written to", args.outdir)

if __name__ == "__main__":
    main()
