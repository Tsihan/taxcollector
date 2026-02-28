#!/usr/bin/env python3
# Compare current component choices vs best_combination CSV.
#
# Usage:
#   python compare_component_similarity.py \
#       --best-csv best_combination_job_no_geqo.csv \
#       --current-txt query_component_map_job_v2.txt
#
# Notes:
# - Bit order is CE, CM, JN (000=NONE/baseline, 111=ALL).
# - The script normalizes "_round1" suffixes to align query names.

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from typing import Dict, Iterable, Tuple


BIT_MAP = {
    "none": (0, 0, 0),
    "baseline": (0, 0, 0),
    "ce": (1, 0, 0),
    "cm": (0, 1, 0),
    "jn": (0, 0, 1),
    "ce_cm": (1, 1, 0),
    "ce_jn": (1, 0, 1),
    "cm_jn": (0, 1, 1),
    "all_three": (1, 1, 1),
    "all": (1, 1, 1),
    "ce+cm": (1, 1, 0),
    "ce+jn": (1, 0, 1),
    "cm+jn": (0, 1, 1),
    "ce+cm+jn": (1, 1, 1),
}

BEST_NORM = {
    "baseline": "baseline",
    "ce": "ce",
    "cm": "cm",
    "jn": "jn",
    "ce_cm": "ce_cm",
    "ce_jn": "ce_jn",
    "cm_jn": "cm_jn",
    "all_three": "all_three",
    "all": "all_three",
    "ce+cm": "ce_cm",
    "ce+jn": "ce_jn",
    "cm+jn": "cm_jn",
    "ce+cm+jn": "all_three",
}

COMP_NORM = {
    "NONE": "none",
    "N": "none",
    "CM": "cm",
    "CE": "ce",
    "JN": "jn",
    "CE+CM": "ce_cm",
    "CE+JN": "ce_jn",
    "CM+JN": "cm_jn",
    "ALL": "all_three",
}


def normalize_query(name: str) -> str:
    name = name.strip()
    if name.endswith("_round1"):
        name = name[:-7]
    return name


def load_best_csv(path: str) -> Dict[str, str]:
    best: Dict[str, str] = {}
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            query = normalize_query(row["query"])
            combo = (row.get("best_combo") or "").strip().lower()
            combo = BEST_NORM.get(combo, combo)
            best[query] = combo
    return best


def parse_map_line(line: str) -> Tuple[str, str] | Tuple[None, None]:
    parts = line.strip().split("\t")
    if len(parts) < 3:
        return None, None
    query = normalize_query(parts[1])
    comp = parts[2].strip()
    comp = re.sub(r"\(.*?\)", "", comp).strip()
    comp = comp.replace("Auto: ", "").replace("Auto", "").strip()
    comp = comp.replace(" ", "").upper()
    return query, comp


def load_current_txt(path: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if not line.strip() or line.startswith("#"):
                continue
            query, comp = parse_map_line(line)
            if not query:
                continue
            norm = COMP_NORM.get(comp, comp.lower())
            mapping[query] = norm
    return mapping


def hamming(a: Iterable[int], b: Iterable[int]) -> int:
    return sum(x != y for x, y in zip(a, b))


def evaluate(best: Dict[str, str], pred: Dict[str, str]) -> Dict[str, object]:
    rows = []
    for query, best_combo in best.items():
        pred_combo = pred.get(query)
        if pred_combo is None:
            continue
        if best_combo not in BIT_MAP or pred_combo not in BIT_MAP:
            continue
        b = BIT_MAP[best_combo]
        p = BIT_MAP[pred_combo]
        rows.append((query, b, p))

    if not rows:
        return {"count": 0}

    dists = [hamming(b, p) for _, b, p in rows]
    exact = sum(1 for _, b, p in rows if b == p)
    dist_counts = Counter(dists)
    avg = sum(dists) / len(dists)
    return {
        "count": len(rows),
        "exact": exact,
        "exact_pct": exact / len(rows),
        "avg_hamming": avg,
        "max_hamming": max(dists),
        "dist_counts": {i: dist_counts.get(i, 0) for i in range(4)},
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare current component choices vs best_combination CSV."
    )
    parser.add_argument("--best-csv", required=True, help="Path to best_combination CSV.")
    parser.add_argument("--current-txt", required=True, help="Path to query_component_map*.txt.")
    args = parser.parse_args()

    best = load_best_csv(args.best_csv)
    pred = load_current_txt(args.current_txt)
    result = evaluate(best, pred)

    if result.get("count", 0) == 0:
        print("No overlapping queries found.")
        return 1

    print(f"Queries compared: {result['count']}")
    print(f"Exact matches: {result['exact']} ({result['exact_pct']*100:.1f}%)")
    print(f"Avg Hamming distance: {result['avg_hamming']:.3f}")
    print(f"Max Hamming distance: {result['max_hamming']}")
    print(f"Distance counts: {result['dist_counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
