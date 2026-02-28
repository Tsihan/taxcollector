# TEE Cardinality/Size Heuristics — “Magic Numbers” and Default Rationale

This document explains every “magic number” (hard-coded constant and default GUC value) in the provided `tee_cardinality_estimation` PostgreSQL extension code. The goal is to make the parameters **auditable**, **defensible**, and **easy to tune** for AMD SEV-SNP / TEE environments.

> **Design intent (high level):**  
> We avoid patching PostgreSQL’s cost model directly. Instead, we bias the planner by inflating **`rel->pages`** (IO work proxy) and **`reltarget->width`** (memory/bandwidth proxy) when the relation or join’s **working set exceeds an effective cache budget**. This approach influences many cost components (SeqScan, sorts, hashes, materialize) with minimal invasiveness, while keeping row-count cardinalities stable to reduce plan churn.

---

## 1) Mental model: what the extension is modeling

### Why cache spill is the trigger
TEEs often incur **higher miss penalties** (e.g., additional checks/metadata overheads, encryption effects, I/O bounce buffering, etc.). When data fits comfortably in cache, overhead is usually less dramatic. When the working set exceeds cache and misses increase, overhead rises sharply.  
Therefore, this extension uses “spill beyond cache” as a clean, planner-visible proxy for “the query is entering the expensive regime.”

### Why inflate pages and width (instead of rewriting cost code)
PostgreSQL’s planner cost functions already incorporate **pages** and **tuple width** in many places:

- **`rel->pages`** affects I/O terms (SeqScan and more).
- **`reltarget->width`** affects memory-bound operators and size estimates that feed into hash/sort/materialize decisions.

So: **inflate the inputs the cost model already trusts** rather than forking the cost model.

### How “spill excess” is computed
For a base relation:
- `rel_pages = rel->pages`
- `cache_pages = effective_cache_size * sev_cache_size_scale`
- a grace zone is applied (`sev_spill_grace_ratio`) to avoid instability near the boundary.

If `rel_pages <= cache_pages * (1 + grace)`, then “no spill penalty.”  
Otherwise:

```
spill_excess = (rel_pages / cache_pages) - (1 + grace)
```
and it is capped to avoid runaway explosion.

Interpretation:
- **spill_excess = 0**: “fits or barely exceeds cache” (within grace)
- **spill_excess ≈ 1**: “meaningfully beyond cache”
- **spill_excess large**: “deep spill regime” (but we cap it)

---

## 2) Parameter catalog: every magic number and its rationale

### 2.1 Feature switches and baseline behavior

#### `enable_sev_snp_ce` (GUC)
- **Default:** `true`
- **Meaning:** Master enable switch.
- **Rationale:** Enables the extension by default so experiments don’t silently run “vanilla.” Still user-settable (PGC_USERSET) for easy A/B testing.

---

## 2.2 Cache spill detection (the “phase change” gate)

#### `sev_cache_size_scale`
- **Default:** `0.5` (range 0.05–2.0)
- **Meaning:** Treat the configured `effective_cache_size` as **only half as effective** under SNP when testing for spill.
- **Why 0.5 is defensible:**  
  This does *not* claim physical cache is halved; it models the empirical observation that **misses cost more**. A simple way to reflect “miss penalty ×2” is to treat the cache budget as “half-effective” for spill-trigger purposes.  
  - Conservative: scaling affects **triggering** of penalties, not raw cardinality math.
  - Intuitive tuning: if your TEE overhead is smaller, push toward 1.0; if larger, reduce below 0.5.

#### `sev_spill_grace_ratio`
- **Default:** `0.25`
- **Meaning:** Add a 25% “grace zone” above cache before penalties apply.
- **Why 0.25 is chosen:**  
  Planning is extremely sensitive near thresholds. Without a grace zone, minor estimate noise can flip “spill/no-spill,” causing plan instability.  
  25% is large enough to absorb estimation variance but small enough that truly over-cache cases still trigger promptly.

#### `sev_small_table_threshold_pages`
- **Default:** `2000` pages
- **Meaning:** Relations smaller than this are **not** inflated, even if the spill formula would trigger.
- **Rationale for 2000 pages:**  
  With 8KB pages (`BLCKSZ` default), 2000 pages ≈ **16 MB**. For relations of this size, it is common that:
  - they stay hot (frequent reuse),
  - they are unlikely to create the pathological “deep miss” regime,
  - aggressive penalties would distort planning disproportionately.  
  This is a stability/robustness guardrail: **protect small tables and dimension filters**.

#### `spill_excess cap = 9.0`
- **Location:** `calculate_cache_spill_excess()` and join spill computation
- **Meaning:** Prevent spill factor from growing beyond “very deep spill.”
- **Why 9.0:**  
  Once you are >~10× beyond cache, you are already in a regime where “it spills a lot.” Further increasing the penalty yields diminishing planner benefits but increases the risk of extreme (and possibly unrealistic) size inflation. The cap keeps estimates bounded and prevents numeric instability.

---

## 2.3 Base relation penalties (do not change cost code; change planner inputs)

### 2.3.1 IO work proxy: inflate `rel->pages`

#### `sev_io_inflation_alpha`
- **Default:** `3.0`
- **Meaning:** Pages inflation factor is:
  ```
  pages_factor = 1 + alpha * spill_excess
  ```
- **Why 3.0:**  
  This is intentionally strong because `rel->pages` is a **direct lever** on I/O-heavy paths (SeqScan and friends). Under TEE, I/O can be magnified by:
  - bounce buffering,
  - encryption/decryption overhead,
  - extra memory checks on I/O buffers,
  - generally worse tail behavior under misses.  
  Alpha=3.0 says: once you spill meaningfully beyond cache, treat that I/O work as several times heavier—enough to shift plans away from “scan everything” when appropriate.

#### `sev_max_pages_factor`
- **Default:** `10.0`
- **Meaning:** `rel->pages` inflation is capped to at most 10×.
- **Why 10×:**  
  Capping is crucial. Without a cap, a bad row/width estimate could multiply into huge pages, which could:
  - break planner assumptions,
  - cause extremely pessimistic plans even for moderate overshoot,
  - risk overflow in downstream calculations.  
  10× is large enough to express “TEE I/O is painful here” without making costs absurd.

**Example intuition (with defaults):**
- Suppose `rel_pages = 2 × cache_pages`, grace=0.25:
  - `spill_excess = 2 - 1.25 = 0.75`
  - `pages_factor = 1 + 3 × 0.75 = 3.25`
  So the planner sees the scan as ~3.25× more I/O work once it clearly spills.

### 2.3.2 Memory/RMP proxy: inflate `reltarget->width`

#### `sev_rmp_width_beta`
- **Default:** `0.0` (disabled by default)
- **Meaning:** Width inflation factor is:
  ```
  eff_width_factor = 1 + beta * spill_excess * width_factor
  ```
- **Why default is 0.0:**  
  Width inflation affects **many** memory-sensitive decisions (hash build size, sort memory pressure, etc.). Turning it on by default can be too aggressive for users not running under SNP.  
  Keeping it off ensures the default behavior is “safe” and users can enable it explicitly after validating on their workload.

#### `SEV_ROW_WIDTH_UNIT = 16.0`
- **Meaning:** Defines the normalization unit for the width penalty term `(width/16)^exp`.
- **Why 16 bytes:**  
  16 bytes is a practical “small tuple” unit and keeps the base of the exponent near 1.0 for narrow tuples. This stabilizes the scaling across typical row widths (tens to a few hundreds of bytes).

#### `sev_width_exponent = 1.2`
- **Meaning:** Controls how strongly wide tuples amplify penalties:
  ```
  width_factor = (width / 16) ^ 1.2
  ```
- **Why 1.2:**  
  Slightly super-linear growth captures that:
  - wider tuples strain memory bandwidth and cache lines more,
  - hash tables and sorts scale worse with per-tuple bytes than with tuple count alone.  
  1.2 is a measured compromise: it differentiates narrow vs. wide tuples without exploding for moderately wide rows.

#### `sev_max_width_factor = 4.0`
- **Meaning:** Width inflation is capped to at most 4×.
- **Why 4×:**  
  Hash/sort costs are sensitive to tuple width; a high cap risks “planner panic.” 4× is strong enough to steer away from memory-heavy plans in the spill regime, but bounded enough to avoid pathological overreaction.

#### `SEV_MIN_WIDTH = 1.0`
- **Meaning:** Prevent divide-by-zero or nonsense widths.
- **Why 1.0:**  
  Width=0 is not meaningful; 1 byte is the smallest safe floor.

### 2.3.3 Optional row inflation (deliberately conservative)

#### `sev_rows_inflation_gamma`
- **Default:** `0.0` (disabled by default)
- **Meaning:** If enabled, row estimate is inflated:
  ```
  row_factor = 1 + gamma * spill_excess * width_factor
  ```
  with an internal cap `row_factor <= 10.0`.
- **Why default is 0.0:**  
  Changing row counts changes join ordering and can cause plan churn. This extension’s philosophy is:
  - bias cost-sensitive operators via pages/width first,
  - only adjust rows if you absolutely must.
- **Why cap at 10×:**  
  Row inflation can cascade into exponential join cardinality effects. The cap prevents catastrophic blowups when enabled.

---

## 2.4 Join-level penalties (prevent bad join reorders and fanout amplification)

Join heuristics in this code aim to reduce failures where:
- an early one-to-many join creates a huge intermediate,
- then nested loops amplify the cost catastrophically,
- especially under TEE where misses and memory bandwidth are “more expensive.”

### Join spill penalty on join output size

#### `sev_join_spill_beta`
- **Default:** `0.32`
- **Meaning:** If join output working set spills cache:
  ```
  factor *= 1 + join_spill_beta * spill_excess
  ```
- **Why 0.32:**  
  This is intentionally moderate: join spill is real, but we do not want to rewrite join cardinalities aggressively. The code later shifts **most** of this factor into width inflation (not rows), limiting plan churn while still making large intermediates “look expensive.”

### Fanout penalty

#### `sev_join_fanout_threshold`
- **Default:** `2.7`
- **Meaning:** Fanout is computed as the maximum of:
  - `nrows / outer_rows`
  - `nrows / inner_rows`
  If fanout exceeds 2.7, we penalize.
- **Why 2.7 is defensible:**  
  Fanout below ~2× is common and often benign (e.g., mild one-to-many). The biggest disasters come from *strong* fanout (many-to-many or unfiltered one-to-many). 2.7 sets the threshold above “ordinary expansion” but below “obvious explosion.” It is intentionally not an integer to reduce alignment with typical selectivity rounding artifacts (which can cluster around 2.0, 3.0, etc.).

#### `sev_join_fanout_beta`
- **Default:** `0.6`
- **Meaning:** Penalty grows smoothly:
  ```
  fanout_pen = log1p(fanout - threshold)
  factor *= 1 + join_fanout_beta * fanout_pen
  ```
- **Why log1p + 0.6:**  
  - `log1p()` provides a “soft knee”: small exceedances don’t overreact; large exceedances still matter.
  - 0.6 yields a noticeable but bounded steering force when fanout is clearly harmful.
  This avoids turning a heuristic into a brittle hard rule.

### Stabilizers and caps

#### `sev_max_join_rows_factor`
- **Default:** `3.5`
- **Meaning:** The combined join penalty factor is capped at 3.5×.
- **Why 3.5×:**  
  Join penalties are heuristics; they should **bias**, not dominate. A 3.5× cap is strong enough to avoid catastrophic join orders, but not so strong that it forces one join strategy universally.

#### `sev_join_rows_cap`
- **Default:** `1.1`
- **Meaning:** Even if the join penalty factor is larger, the **exposed** row inflation is capped at 1.1×.
- **Why 1.1×:**  
  This is a key stability design decision: we want join penalties to influence **memory/I/O-sensitive operator choices** without corrupting cardinalities used for join enumeration.  
  The majority of the penalty is shifted to **width inflation**, which influences hash/sort/materialize cost, while keeping `rows` nearly stable.

#### `sev_join_skip_rows`
- **Default:** `12000.0`
- **Meaning:** If join output and both inputs are “tiny” (≤ 12k rows), skip join penalties.
- **Why 12k:**  
  This avoids paying complex heuristics where they do not matter and protects small joins from being distorted by spill heuristics. 12k is large enough to include many “small but not trivial” intermediate sizes while still filtering out the majority of cases where join penalties are unnecessary.

#### Width dampening factor `0.5`
- **Location:** `width_factor = 1 + (row_factor - 1) * 0.5`
- **Meaning:** Only half of the computed join factor is applied to width inflation.
- **Why 0.5:**  
  Join penalties already stack with base relation penalties. Applying the full factor to width can over-penalize and push the planner into overly conservative strategies. 0.5 is a pragmatic damping coefficient: it retains directional pressure while minimizing overshoot.

#### Default width `32.0`
- **Locations:** `estimate_width_local()` fallback and join penalty width defaults
- **Meaning:** If width cannot be determined, assume 32 bytes.
- **Why 32 bytes:**  
  32 bytes is a common “reasonable small row” estimate in the absence of better information: it is not tiny (like 8) and not huge (like 256). It keeps penalties and size calculations from being extreme when the planner’s targetlist width is unknown.

---

## 2.5 “Safety” numeric bounds and clamps (planner robustness)

These constants are about **never breaking planning** due to NaNs, overflow, or absurd values.

#### `1e100` upper bound (rows / cardinalities)
- **Locations:** `clamp_card_est_safe()`, `clamp_row_est_safe()`
- **Meaning:** Treat extremely large estimates as “effectively infinite” but bounded.
- **Why 1e100:**  
  It is far above any realistic row count, yet safely below floating overflow risk. This keeps planner math stable even when selectivity statistics are pathological.

#### Minimum rows `1.0` for `clamp_row_est_safe()`
- **Meaning:** Ensure `rows >= 1`.
- **Why 1:**  
  PostgreSQL’s planner generally expects relations to have at least one row for cost purposes; a zero-row estimate can produce odd corner behavior unless it is truly proven empty.

#### `UINT_MAX` / `INT_MAX` caps for pages/width
- **Locations:** `clamp_blocknumber_safe()`, `clamp_width_safe()`
- **Meaning:** Convert doubles back into integer fields safely.
- **Why:**  
  These fields are stored as integer types; enforcing type limits prevents undefined behavior and keeps the extension safe across architectures.

---

## 3) Default value “provenance” (why these defaults are reasonable starting points)

These values are **not arbitrary**; they reflect three practical constraints in real Postgres planning for TEE systems:

1. **Phase-change behavior:** Overheads are often mild while hot-in-cache, and harsh in miss-heavy regimes. Hence **spill detection** and **cache scaling**.
2. **Planner stability matters:** Join enumeration is sensitive to row counts; thus default to **pages/width inflation** and keep row inflation disabled or capped.
3. **Bounded heuristics:** Caps and grace zones prevent runaway pessimism and reduce plan flips due to small estimation noise.

In practice, these defaults are meant to be a strong first approximation that:
- meaningfully changes plans when a query is clearly in the “spill” regime,
- does not perturb small tables or small joins,
- remains numerically safe.

---

## 4) Tuning guide (how to justify changes scientifically)

### If you observe too little plan change under SNP
1. Decrease `sev_cache_size_scale` (e.g., 0.5 → 0.4) to trigger penalties earlier.
2. Increase `sev_io_inflation_alpha` (e.g., 3.0 → 4.0) for stronger I/O steering.
3. Enable width modeling: set `sev_rmp_width_beta` (start at 0.2–0.6).

### If you observe plan instability (frequent flips)
1. Increase `sev_spill_grace_ratio` (0.25 → 0.35) to widen the “don’t care” zone.
2. Reduce `sev_io_inflation_alpha` or tighten caps (`sev_max_pages_factor`).
3. Keep `sev_rows_inflation_gamma = 0.0` unless you have a validated need.

### If join ordering is still problematic (fanout disasters)
1. Lower `sev_join_fanout_threshold` slightly (2.7 → 2.3).
2. Increase `sev_join_fanout_beta` modestly (0.6 → 0.8).
3. Keep `sev_join_rows_cap` near 1.1 to preserve cardinality stability.

### If you suspect over-penalization
- Raise `sev_small_table_threshold_pages` (protect more relations),
- or increase `sev_cache_size_scale` toward 1.0,
- or reduce `sev_join_spill_beta` / `sev_join_fanout_beta`.

---

## 5) Summary: what each number accomplishes (one-line cheat sheet)

- **0.5 cache scale:** model “cache is half-effective” under SNP miss penalties.  
- **0.25 grace:** avoid plan flapping near cache boundary.  
- **2000 pages (~16MB):** small-table protection for stability.  
- **cap spill_excess at 9:** bounded pessimism, prevents runaway.  
- **alpha=3, pages cap=10:** strong but bounded I/O tax when spilling.  
- **width unit=16, exp=1.2, width cap=4:** memory tax grows gently with row width, bounded.  
- **join spill beta=0.32:** moderate penalty when join output spills.  
- **fanout threshold=2.7, beta=0.6, log1p:** smooth discouragement of harmful expansions.  
- **max join factor=3.5:** biases join order without overriding everything.  
- **join rows cap=1.1:** keep cardinalities stable; steer via width instead.  
- **skip rows=12k:** don’t waste heuristics on tiny intermediates.  
- **1e100 / INT_MAX / UINT_MAX:** numeric safety, never break planning.

---

## Appendix: parameter-to-code mapping

| Parameter / Constant | Default | Code location / usage |
|---|---:|---|
| `enable_sev_snp_ce` | `true` | master gate |
| `sev_io_inflation_alpha` | `3.0` | pages_factor = 1 + alpha·spill_excess |
| `sev_rmp_width_beta` | `0.0` | width inflation under spill |
| `sev_rows_inflation_gamma` | `0.0` | optional row inflation (off) |
| `sev_cache_size_scale` | `0.5` | cache_pages = effective_cache_size·scale |
| `sev_spill_grace_ratio` | `0.25` | ignore slight >cache cases |
| `sev_join_spill_beta` | `0.32` | join output spill penalty |
| `sev_join_fanout_beta` | `0.6` | join fanout penalty multiplier |
| `sev_join_fanout_threshold` | `2.7` | fanout knee point |
| `sev_max_join_rows_factor` | `3.5` | cap combined join factor |
| `sev_join_rows_cap` | `1.1` | cap visible row inflation |
| `sev_join_skip_rows` | `12000` | skip penalties for tiny joins |
| `sev_ndistinct_multiplier` | `1.0` | disabled (kept for compatibility) |
| `sev_small_table_threshold_pages` | `2000` | protect small tables |
| `sev_max_pages_factor` | `10.0` | cap base pages inflation |
| `sev_max_width_factor` | `4.0` | cap width inflation |
| `sev_width_exponent` | `1.2` | (width/16)^exp term |
| `SEV_ROW_WIDTH_UNIT` | `16.0` | width normalization |
| `SEV_MIN_WIDTH` | `1.0` | minimum width floor |
| `spill_excess cap` | `9.0` | cap spill sensitivity |
| `row_factor cap` | `10.0` | cap optional row inflation |
| `fallback width` | `32.0` | default width if unknown |
| `1e100` | — | numeric bound for rows |
| `UINT_MAX`, `INT_MAX` | — | safe conversion caps |

