from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Tuple

# Regex pulls the component choice from each TEE Adaptive log line
COMPONENT_RE = re.compile(r"TEE Adaptive: Auto: ([A-Z+ ]+(?:\(strong\))?)")


def extract_components(log_path: Path) -> List[str]:
    text = log_path.read_text(encoding="utf-8", errors="ignore")
    return COMPONENT_RE.findall(text)


def load_query_names(json_path: Path) -> List[Tuple[str, str]]:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    results = data.get("query_results", [])

    # Keep entries that contain both name and timestamp, sorted chronologically
    filtered = [
        (item["timestamp"], item["query_name"])
        for item in results
        if item.get("query_name") and item.get("timestamp")
    ]
    filtered.sort(key=lambda pair: pair[0])
    return [name for _, name in filtered]


def write_mapping(output_path: Path, query_names: List[str], components: List[str]) -> None:
    pairs = list(zip(query_names, components))
    with output_path.open("w", encoding="utf-8") as fh:
        for idx, (qname, comp) in enumerate(pairs, start=1):
            fh.write(f"{idx}\t{qname}\t{comp}\n")

    mismatched = len(query_names) != len(components)
    if mismatched:
        remaining_queries = query_names[len(pairs) :]
        remaining_components = components[len(pairs) :]
        with output_path.open("a", encoding="utf-8") as fh:
            if remaining_queries:
                fh.write("\n# Unmatched queries (no component)\n")
                for name in remaining_queries:
                    fh.write(f"# {name}\n")
            if remaining_components:
                fh.write("\n# Unmatched components (no query)\n")
                for comp in remaining_components:
                    fh.write(f"# {comp}\n")


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    json_path = base_dir / "ceb_v18_query_results_cvm_snp_20260118_141925.json"
    log_path = base_dir / "logfile"
    output_path = base_dir / "query_component_map_ceb_v18.txt"

    query_names = load_query_names(json_path)
    components = extract_components(log_path)

    write_mapping(output_path, query_names, components)
    print(f"Wrote {output_path} ({len(query_names)} queries, {len(components)} components)")


if __name__ == "__main__":
    main()
