# TEE Adaptive Selector

This extension adds an adaptive meta-optimizer that can select TEE components (CE/CM/JN) per query and optionally learn from feedback.

## How it works

1. **Planner hook**: For each plannable statement (non-utility), the planner hook extracts query features and decides component settings.
2. **Rule-based mode**: If cache is disabled, selection follows the weighted rule-based logic.
3. **Cache mode**:
   - **Lookup**: The cache is a hash table keyed by a hash `H` of the normalized SQL.
   - **Bucket**: Each `H` has a fixed-capacity array of length 8 storing `(v, t, sh, cb)`:
     - `v`: version (0–7)
     - `t`: measured execution time (ms)
     - `sh`: similarity hash of sanitized SQL
     - `cb`: 3-bit component combination (CE/CM/JN)
   - **Ordering**: Slots are kept in ascending `t`; the first slot is the current best combination.

## Update mode (cache population)

Enabled when **both** GUCs are true:

- `tee_adaptive_selector.use_cache = on`
- `tee_adaptive_selector.cache_populating = on`

Behavior:

- **If `H` not in cache**: propose an initial `cb` by taking the 3 nearest neighbors under `sh` (global), vote, and break ties randomly.
- **If `H` exists and bucket is full (8)**: use the best cached `cb` directly.
- **If `H` exists and bucket not full**: propose a new `cb` using a best‑biased vote:
  - When bucket size > 4: best `cb` + 1 nearest neighbor.
  - When bucket size ≤ 4: best `cb` + 3 nearest neighbors.
  - Avoid duplicate `cb`; if all duplicates, pick a random unused `cb`.

After execution, the executor hook records elapsed time and inserts `(v, t, sh, cb)` into the bucket.

## Cache persistence

- **Load**: On a new backend connection, the cache is read from `tee_adaptive_selector.cache_csv` on first use.
- **Flush**: When the backend connection exits, the cache is written back to CSV and the in‑memory cache is cleared.

## CSV format

The cache file uses this header and columns:

```
hash,version,time,sh,cb
```

Where:
- `hash` = `H`
- `version` = slot version (0–7)
- `time` = execution time in ms
- `sh` = similarity hash
- `cb` = 0–7 component bitmask

Legacy two‑column files (`hash,scenario`) are still accepted on load.

## Key GUCs

- `tee_adaptive_selector.enable` (bool): enable/disable selector.
- `tee_adaptive_selector.use_cache` (bool): enable cache lookup.
- `tee_adaptive_selector.cache_populating` (bool): enable update mode + reload CSV on new connection.
- `tee_adaptive_selector.cache_csv` (string): cache CSV path.
- `tee_adaptive_selector.source_csv` (string): source CSV for initial generation.
- `tee_adaptive_selector.query_dir` (string): SQL directory used by cache generation.
- `tee_adaptive_selector.workload` (string): job/ceb/stack/tpcds.
- `tee_adaptive_selector.log_decisions` (bool): log per‑query decisions.
