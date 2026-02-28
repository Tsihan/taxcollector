#!/usr/bin/env python3
# Examples:
#   python generate_sql_speedup_hash_cache.py \
#       --src /path/to/best_combination_job_no_geqo.csv \
#       --dst /path/to/sql_speedup_hash_cache.csv \
#       --query-dir /path/to/job_queries
#   python generate_sql_speedup_hash_cache.py --src ./input.csv
#   python generate_sql_speedup_hash_cache.py --query-dir /home/ubuntu/job_queries
import argparse
import csv
import os
import sys


def rot(x, k):
    return ((x << k) | (x >> (32 - k))) & 0xFFFFFFFF


def mix(a, b, c):
    a = (a - c) & 0xFFFFFFFF
    a ^= rot(c, 4)
    c = (c + b) & 0xFFFFFFFF
    b = (b - a) & 0xFFFFFFFF
    b ^= rot(a, 6)
    a = (a + c) & 0xFFFFFFFF
    c = (c - b) & 0xFFFFFFFF
    c ^= rot(b, 8)
    b = (b + a) & 0xFFFFFFFF
    a = (a - c) & 0xFFFFFFFF
    a ^= rot(c, 16)
    c = (c + b) & 0xFFFFFFFF
    b = (b - a) & 0xFFFFFFFF
    b ^= rot(a, 19)
    a = (a + c) & 0xFFFFFFFF
    c = (c - b) & 0xFFFFFFFF
    c ^= rot(b, 4)
    b = (b + a) & 0xFFFFFFFF
    return a, b, c


def final(a, b, c):
    c ^= b
    c = (c - rot(b, 14)) & 0xFFFFFFFF
    a ^= c
    a = (a - rot(c, 11)) & 0xFFFFFFFF
    b ^= a
    b = (b - rot(a, 25)) & 0xFFFFFFFF
    c ^= b
    c = (c - rot(b, 16)) & 0xFFFFFFFF
    a ^= c
    a = (a - rot(c, 4)) & 0xFFFFFFFF
    b ^= a
    b = (b - rot(a, 14)) & 0xFFFFFFFF
    c ^= b
    c = (c - rot(b, 24)) & 0xFFFFFFFF
    return a, b, c


def hash_bytes(data):
    length = len(data)
    a = b = c = (0x9E3779B9 + length + 3923095) & 0xFFFFFFFF

    i = 0
    while length - i >= 12:
        a = (a + (data[i + 0]
                  | (data[i + 1] << 8)
                  | (data[i + 2] << 16)
                  | (data[i + 3] << 24))) & 0xFFFFFFFF
        b = (b + (data[i + 4]
                  | (data[i + 5] << 8)
                  | (data[i + 6] << 16)
                  | (data[i + 7] << 24))) & 0xFFFFFFFF
        c = (c + (data[i + 8]
                  | (data[i + 9] << 8)
                  | (data[i + 10] << 16)
                  | (data[i + 11] << 24))) & 0xFFFFFFFF
        a, b, c = mix(a, b, c)
        i += 12

    tail = data[i:]
    tlen = len(tail)
    if tlen:
        if tlen >= 1:
            a = (a + tail[0]) & 0xFFFFFFFF
        if tlen >= 2:
            a = (a + (tail[1] << 8)) & 0xFFFFFFFF
        if tlen >= 3:
            a = (a + (tail[2] << 16)) & 0xFFFFFFFF
        if tlen >= 4:
            a = (a + (tail[3] << 24)) & 0xFFFFFFFF
        if tlen >= 5:
            b = (b + tail[4]) & 0xFFFFFFFF
        if tlen >= 6:
            b = (b + (tail[5] << 8)) & 0xFFFFFFFF
        if tlen >= 7:
            b = (b + (tail[6] << 16)) & 0xFFFFFFFF
        if tlen >= 8:
            b = (b + (tail[7] << 24)) & 0xFFFFFFFF
        if tlen >= 9:
            c = (c + (tail[8] << 8)) & 0xFFFFFFFF
        if tlen >= 10:
            c = (c + (tail[9] << 16)) & 0xFFFFFFFF
        if tlen >= 11:
            c = (c + (tail[10] << 24)) & 0xFFFFFFFF

    a, b, c = final(a, b, c)
    return c & 0xFFFFFFFF


def strip_explain_prefix(sql):
    p = 0
    n = len(sql)
    while p < n and sql[p].isspace():
        p += 1
    if sql[p:p + 7].lower() != "explain":
        return sql
    p += 7
    while p < n and sql[p].isspace():
        p += 1
    if p < n and sql[p] == "(":
        depth = 1
        p += 1
        while p < n and depth > 0:
            if sql[p] == "(":
                depth += 1
            elif sql[p] == ")":
                depth -= 1
            p += 1
        while p < n and sql[p].isspace():
            p += 1
    else:
        options = {
            "analyze", "verbose", "costs", "buffers",
            "timing", "summary", "settings", "wal"
        }
        while p < n:
            matched = False
            for opt in options:
                if sql[p:p + len(opt)].lower() == opt:
                    end = p + len(opt)
                    if end == n or sql[end].isspace():
                        matched = True
                        p = end
                        while p < n and not sql[p].isspace():
                            p += 1
                        while p < n and sql[p].isspace():
                            p += 1
                        break
            if not matched:
                break
    keywords = ("select", "with", "insert", "update", "delete")
    while p < n:
        for kw in keywords:
            if sql[p:p + len(kw)].lower() == kw:
                return sql[p:]
        p += 1
    return sql


def normalize_sql(sql):
    start = strip_explain_prefix(sql)
    return "".join(ch.lower() for ch in start if not ch.isspace()).encode("utf-8")


def map_scenario(best_combo):
    if not best_combo:
        return "NONE"
    key = best_combo.strip().lower()
    mapping = {
        "baseline": "NONE",
        "ce_cm": "CE+CM",
        "ce": "CE",
        "cm": "CM",
        "jn": "JN",
        "ce_jn": "CE+JN",
        "cm_jn": "CM+JN",
        "all_three": "ALL",
        "all": "ALL",
        "ce+cm": "CE+CM",
        "ce+jn": "CE+JN",
        "cm+jn": "CM+JN",
        "ce+cm+jn": "ALL",
    }
    return mapping.get(key, best_combo.strip().upper())


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate sql_speedup_hash_cache.csv from a best-combination CSV and SQL files."
    )
    parser.add_argument(
        "--src",
        default=os.path.join(os.path.dirname(__file__), "best_combination_job_no_geqo.csv"),
        help="Path to source CSV (default: best_combination_job_no_geqo.csv next to this script)",
    )
    parser.add_argument(
        "--dst",
        default=os.path.join(os.path.dirname(__file__), "sql_speedup_hash_cache.csv"),
        help="Path to output CSV (default: sql_speedup_hash_cache.csv next to this script)",
    )
    parser.add_argument(
        "--query-dir",
        default="/home/ubuntu/job_queries",
        help="Directory containing SQL files (default: /home/ubuntu/job_queries)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    src = args.src
    dst = args.dst
    query_dir = args.query_dir

    if not os.path.exists(src):
        print(f"Source CSV not found: {src}", file=sys.stderr)
        return 1
    if not os.path.isdir(query_dir):
        print(f"Query dir not found: {query_dir}", file=sys.stderr)
        return 1

    rows = []
    with open(src, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            query = (row.get("query") or "").strip()
            best_combo = row.get("best_combo") or ""
            if not query:
                continue
            if query.endswith("_round1"):
                query_file = query[:-7]
            else:
                query_file = query

            sql_path = os.path.join(query_dir, query_file)
            if not os.path.exists(sql_path):
                raise FileNotFoundError(
                    f"Missing SQL file for {query}: {sql_path}"
                )
            with open(sql_path, "r", encoding="utf-8") as qf:
                sql = qf.read()

            norm = normalize_sql(sql)
            h = hash_bytes(norm)
            if h & 0x80000000:
                h_signed = h - 0x100000000
            else:
                h_signed = h
            scenario = map_scenario(best_combo)
            rows.append((h_signed, scenario))

    with open(dst, "w", newline="") as f:
        f.write("hash,scenario\n")
        for h, scenario in rows:
            f.write(f"{h},{scenario}\n")

    print(f"Wrote {len(rows)} entries to {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
