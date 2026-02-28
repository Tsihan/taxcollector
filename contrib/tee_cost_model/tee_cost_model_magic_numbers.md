# TEE Cost Model (V10.1) — “Magic Numbers” and Default Rationale

This document explains every “magic number” (hard-coded constants and default GUC values) in the provided `tee_cost_model.c` (Version V10.1: **Corrected & Full Spectrum Coverage**). The goal is to make each number **auditable**, **defensible**, and **easy to tune** for AMD SEV-SNP / TEE environments while minimizing regressions on non-TEE or mixed workloads.

> **Design intent (high level):**  
> Rather than rewriting PostgreSQL’s entire cost model, this extension introduces targeted, operator-specific “taxes” that approximate the dominant TEE overhead modes:
> - **Bounce-buffer / page-touch overhead** → modeled as a page-based IO tax (`get_io_tax(pages)`)
> - **Encrypted/metadata-heavy CPU** → modeled as a CPU tax on the processing portion of operators (sort/agg)
> - **Parallel communication overhead** → modeled as a gentle, worker-aware Gather/GatherMerge tax
> - **Cache spill regime changes** (e.g., hash build beyond L3) → modeled by size-aware penalties
>
> A key theme in V10.1 is “**physics-based proxies**”: use pages for IO-like penalties, bytes for memory footprint, and small-workload exemptions to preserve good plans for cache-resident cases.

---

## 1) Mental model: what the extension is modeling

### 1.1 Why “pages touched” is the IO proxy
In TEE (and especially SNP-like settings), page movement and access can incur extra overhead:
- bounce buffering / SWIOTLB-style memcpy behavior,
- encryption/decryption latency on I/O paths,
- generally higher miss penalties and bandwidth pressure.

Therefore, V10.1 corrects earlier versions by ensuring the IO tax is based on **estimated pages touched** rather than a blunt constant multiplier.

### 1.2 Why there are “safe” thresholds
Many overheads are strongly *nonlinear* with working set size. If a scan/build fits in cache, the overhead may be minimal. Once it spills, overhead grows rapidly. This model uses:
- **`tee_safe_cache_kb`** as a conservative cache-resident cutoff,
- **`tee_l3_cache_kb`** as an L3 spill boundary for hash join build sizes,
- additional “tiny-result” adjustments (e.g., Gather) where fixed overhead dominates.

The aim is to avoid over-penalizing small and selective plans that remain optimal even under TEE.

---

## 2) Parameter catalog: every magic number and its rationale

### 2.1 Global feature switch

#### `tee_enable_cost_model` (GUC: `tee_cost_model.enable`)
- **Default:** `true`
- **Meaning:** Master switch for all TEE taxes.
- **Rationale:** Enables the model by default for experiment reproducibility, but remains user-settable to support A/B comparisons quickly.

---

## 2.2 Baseline overhead percentages (“softened to reduce regressions”)

These are “base tax rates” used across operators. They are deliberately **smaller than worst-case measurements** to reduce unintended plan flips and regressions while still providing steering power.

#### `tee_io_overhead_pct`
- **Default:** `0.08` (8%) — GUC range `0.0 … 5.0`
- **Meaning:** IO tax rate applied by `get_io_tax(pages)`.
- **Why 8%:**  
  We observed overheads such as Bitmap Heap Scan around +11% (CEB) and +17% (TPC-DS), plus scan-heavy cases. Using **8%** is intentionally conservative (“softened”) to avoid over-penalizing scans where Postgres already models I/O cost reasonably and where TEE overhead may vary by platform and device path.  
  It provides a baseline directional pressure without “forcing” index-only behavior or extreme join strategy shifts.

#### `tee_cpu_overhead_pct`
- **Default:** `0.06` (6%) — GUC range `0.0 … 5.0`
- **Meaning:** CPU tax applied to the *processing-only* portion of Sort and Agg.
- **Why 6%:**  
  We observed that sort and window-like operators around +13–25% in some workloads. But those overheads can include memory effects and spill behavior not purely “CPU.” A 6% CPU-only uplift is a stable baseline that biases away from heavy compute in large-data regimes without overreacting when the operator is not the dominant bottleneck.

#### `tee_gather_overhead_pct`
- **Default:** `0.10` (10%) — GUC range `0.0 … 10.0`
- **Meaning:** Base communication/coordination overhead for Gather / GatherMerge.
- **Why 10%:**  
  Observed Gather overhead can be very high (e.g., +151% in Stack), but that typically reflects **specific contention and cross-core coordination regimes**, not a universal constant. 10% serves as a “presence tax” and is later scaled gently by worker count and row count to avoid disabling parallelism entirely.

#### `tee_memoize_overhead_pct`
- **Default:** `0.12` (12%) — GUC range `0.0 … 5.0`
- **Meaning:** Memoize rescan overhead factor applied to startup/total costs with additional entry-count logic.
- **Why 12%:**  
  Memoize can be disproportionately expensive under TEE due to cache maintenance, metadata checks, and memory traffic. Your note indicates Memoize overhead can be large (+69% CEB). This implementation deliberately applies **moderated penalties** plus entry-based scaling to avoid blanket discouragement where Memoize is still the best choice (e.g., highly repetitive probes).

---

## 2.3 Cache size constants and thresholds

#### `tee_l3_cache_kb`
- **Default:** `32768` KB (32 MB) — GUC range `1024 … 1024*1024`
- **Meaning:** The L3 cache size threshold for hash table spill modeling.
- **Why 32 MB:**  
  32 MB is a common ballpark for per-socket L3 cache sizes on server-class CPUs and is a reasonable default when the actual L3 size is unknown or differs by SKU. The purpose is not perfect hardware modeling, but a stable “knee point” at which hash-table build costs begin to rise faster due to cache misses and memory bandwidth pressure.

#### `tee_safe_cache_kb`
- **Default:** `16384` KB (16 MB) — GUC range `0 … 1024*1024`
- **Meaning:** A conservative “cache-resident” threshold: if the working set is below this size, skip most taxes.
- **Why 16 MB:**  
  16 MB is chosen as a “safe” cutoff that is usually small enough to remain hot even under variation and shared-cache contention, yet large enough to avoid taxing common small dimension tables and selective pipelines. This improves plan stability and prevents discouraging good index/scan plans that touch only a small working set.

> **Important:** `tee_safe_cache_kb` is a *heuristic* cutoff for **disabling tax**, not a claim about physical cache size.

---

## 2.4 IO tax function constants

### `get_io_tax(pages)`
```
return pages * 1.0 * tee_io_overhead_pct;
```

#### The constant `1.0` (“base page cost”)
- **Meaning:** A normalized base per-page cost in the tax formula.
- **Why 1.0:**  
  This code intentionally treats the tax as an **add-on** proportional to pages, independent of Postgres’s already computed I/O cost. In other words: “add X% of a normalized page work unit per page.”  
  **Engineering note:** A more tightly coupled variant could multiply by `seq_page_cost` (or `random_page_cost`) to align with user-tuned storage settings. V10.1 keeps `1.0` to reduce interactions with unrelated storage knobs and to keep the tax easy to interpret and calibrate.

---

## 2.5 Small-workload exemptions (anti-regression stabilizers)

### `is_cache_resident_pages(pages)` and `is_small_workload(rows, width)`

#### Conversion constant: `kb = (pages * BLCKSZ) / 1024.0`
- **Magic constants:** `1024.0` and `BLCKSZ`
- **Meaning:** Convert pages (8KB each by default) to KB.
- **Why:** Standard unit conversion; `BLCKSZ` (usually 8192 bytes) is the correct Postgres page size constant.

#### Exemption threshold: `kb < tee_safe_cache_kb`
- **Default threshold:** 16 MB (via `tee_safe_cache_kb=16384`)
- **Meaning:** Skip taxes if workload is likely cache-resident.
- **Rationale:** This is a major stability feature: small scans, small sorts, small memoize caches, and small hash builds should remain attractive because their fixed overheads dominate and TEE amplification may not be pronounced.

---

## 3) Operator-specific “magic numbers”

### 3.1 Seq Scan — page-based IO tax
- **Uses:** `get_io_tax(baserel->pages)`
- **Key numbers:** `tee_io_overhead_pct = 0.08`, safe-cache exemption via `tee_safe_cache_kb`

**Rationale:** SeqScan is fundamentally page-touch driven; page-based tax is the most defensible proxy.

---

### 3.2 Index Scan — pointer-chasing + random IO proxy

#### Fanout constant in tree height: `log(...)/log(300.0)`
- **Magic number:** `300.0`
- **Meaning:** Approximate average B-tree fanout (~hundreds of pointers per page). This converts page count into a rough tree height estimate.
- **Why 300:**  
  Typical B-tree internal pages can hold on the order of a few hundred pointers depending on key size and page format. 300 is a reasonable default to obtain a stable “height” estimate without requiring physical introspection.

#### RMP penalty per level constant: `0.005`
- **Magic number:** `0.005`
- **Meaning:** Added cost per tuple per index level (scaled by rows * height).
- **Why 0.005:**  
  We observed a “roughly 2× cpu_tuple_cost.” This is meant to model additional pointer-chasing + metadata-check overhead per descent step under TEE.  
  Importantly, it scales with **rows** and **height**, so it grows when many index probes are needed and the index is large.

#### Random IO pages estimate: `estimated_pages = indexselectivity * index_pages`
- **Magic aspect:** direct proportionality assumption.
- **Rationale:** This is a simple, defensible approximation: if you touch X% of index keys, you may touch ~X% of index pages (scattered). The goal is to add IO-like tax in proportion to likely random page touches.

---

### 3.3 Bitmap Heap Scan — restored IO tax
#### Page estimate formula: `estimated_heap_pages = baserel->pages * path->rows / baserel->tuples`
- **Magic aspect:** linear scaling by selectivity.
- **Rationale:** A common approximation: rows fraction ≈ pages fraction for heap access under bitmap heap scan (not perfect, but usable for a tax term). It matches the intention: bitmap heap scan cost increases with pages fetched from heap.

---

### 3.4 Sort — CPU overhead on processing-only portion
#### CPU tax applied to: `processing_cost = total_cost - input_cost`
- **Magic:** “tax only incremental sort work, not upstream.”
- **Rationale:** This avoids double-charging upstream operators and keeps the tax focused on the operator’s own CPU work.

#### Exemption threshold: `is_small_workload(tuples, width)` using `tee_safe_cache_kb`
- **Default:** skip if <16MB
- **Rationale:** Sort overhead in small working sets can be dominated by fixed costs; avoid discouraging sorts that are harmless and still optimal.

---

### 3.5 Materialize — page-based “memory write” tax (quarter-rate)
#### Size-to-pages conversion: `pages = (tuples * width) / BLCKSZ`
- **Magic:** using `BLCKSZ` to estimate page count.
- **Rationale:** Materialize writes and reads a buffer of size proportional to bytes. Expressing it in pages makes IO tax consistent with scan penalties and avoids mixing incompatible units.

#### Quarter-rate constant: `* 0.25`
- **Magic number:** `0.25`
- **Meaning:** Materialize tax is applied at 25% of IO tax.
- **Why 0.25:**  
  Materialize may be in-memory and not incur the full bounce-buffer/disk-path overhead implied by the IO tax. Quarter-rate is a pragmatic compromise:
  - keeps large spools from looking free under TEE,
  - but preserves materialize as an option when it helps (e.g., avoiding repeated expensive work).

---

### 3.6 Aggregate — CPU tax on incremental work
#### Tax percent: `tee_cpu_overhead_pct` (default 0.06)
- **Comment mismatch note:** The comment says “~20% CPU tax,” but the code uses `tee_cpu_overhead_pct` (default 6%).
- **Why this matters:** Documentation should match behavior. The current default is intentionally softer (6%) to reduce over-penalization; if you intended 20% for Agg specifically, it should be implemented as a separate GUC or constant.

---

### 3.7 WindowAgg — constant overhead multiplier `0.06`
#### `overhead = 0.06; path->total_cost *= (1.0 + overhead);`
- **Magic number:** `0.06`
- **Meaning:** A 6% multiplicative total-cost uplift for WindowAgg on non-small workloads.
- **Why 6%:**  
  We observed ~13% overhead (TPC-DS). The code chooses **half that** as a conservative default. This is persuasive because WindowAgg cost already includes multiple upstream effects; a softer multiplier reduces the risk of pushing plans away from WindowAgg when it’s structurally necessary.

---

### 3.8 Memoize — entry-count-aware cache maintenance penalties

#### Entry scaling denominator: `entries / 2000.0`
- **Magic number:** `2000.0`
- **Meaning:** “Large cache” reference point for Memoize entries.
- **Why 2000:**  
  This is a pragmatic threshold where cache management overhead (hashing, metadata, memory footprint) becomes non-trivial. It provides a stable scale for entry-based penalties without requiring a precise model of key widths or hit rates.

#### Ratio cap: `if (ratio > 2.0) ratio = 2.0;`
- **Magic number:** `2.0`
- **Meaning:** Cap the entry-based multiplier to avoid runaway penalties.
- **Rationale:** Memoize is often the right plan even with large caches if it dramatically reduces inner scans. A cap prevents oversteering away from Memoize.

#### Entry penalty slope: `entry_penalty = 0.08 * ratio;`
- **Magic number:** `0.08`
- **Meaning:** Adds up to ~0.16 (16%) extra cost factor at capped ratio.
- **Why 0.08:**  
  Intended to be noticeable but not dominant. It complements `tee_memoize_overhead_pct` rather than replacing it.

#### Small-cache exemption: `if (entries > 0 && entries < 500.0) return;`
- **Magic number:** `500.0`
- **Meaning:** Skip heavy penalties for small memoize caches.
- **Why 500:**  
  Below a few hundred entries, memoize maintenance overhead is typically modest; penalizing it would cause regressions for common “small reuse” patterns.

#### Startup vs total scaling constants: `0.40`, and differing application
```
*startup *= (1 + tee_memoize_overhead_pct*0.40 + entry_penalty);
*total   *= (1 + tee_memoize_overhead_pct       + entry_penalty);
```
- **Magic number:** `0.40`
- **Meaning:** Startup cost gets only 40% of the memoize overhead; total cost gets 100%.
- **Why 0.40:**  
  Memoize overhead is dominated by ongoing cache lookups/maintenance across rescans, not upfront initialization. Scaling startup more gently prevents overly discouraging Memoize in cases where startup matters but repeated rescans still benefit.

---

### 3.9 Gather / Gather Merge — worker-aware, result-size-aware taxes

#### Worker thresholds: `>4`, `>2`
- **Magic numbers:** `2`, `4`
- **Meaning:** Apply slightly higher communication tax as workers increase.
- **Why these thresholds:**  
  Small numbers of workers often have manageable overhead; as workers grow, synchronization and communication overhead rises. `2` and `4` are common “knee points” in practical parallel planning where coordination becomes more visible.

#### Scaling constants: `1.10`, `1.05`
- **Magic numbers:** `1.05`, `1.10`
- **Meaning:** Increase gather penalty by 5% or 10% depending on worker count tier.
- **Why so small:**  
  Parallelism can be extremely beneficial under TEE too; a strong penalty would disable good parallel plans. This model chooses a *gentle* scaling so Gather remains viable.

#### Tiny-result thresholds: `rows < 1000`, `rows < 10000`
- **Magic numbers:** `1000.0`, `10000.0`
- **Meaning:** Increase penalty for small result sets where fixed overhead dominates.
- **Why 1k / 10k:**  
  These are common order-of-magnitude cutoffs for “small” vs “moderate” outputs. The overhead of parallel orchestration can be disproportionately high when only a few thousand rows are produced.

#### Row adjustment factors: `1.20`, `1.08`
- **Magic numbers:** `1.20`, `1.08`
- **Meaning:** Increase penalty by 20% for very small outputs; 8% for moderately small outputs.
- **Rationale:** Makes Gather less attractive for tiny outputs without turning off parallelism globally.

#### Startup vs total weighting: `0.25`, `0.10`
- **Magic numbers:** `0.25`, `0.10`
- **Meaning:** Apply only 25% of the scaled penalty to startup and 10% to total.
- **Why:**  
  Gather overhead often manifests as coordination latency and fixed scheduling overhead rather than strictly proportional to tuples. By weighting lightly, the plugin avoids “killing parallelism” while still reflecting the presence of TEE-related coordination costs.

---

### 3.10 Merge Join — pipeline stall penalty

#### Base overhead: `0.10`
- **Magic number:** `0.10`
- **Meaning:** Multiply total cost by 1.10 for MergeJoin.
- **Why 10%:**  
  Merge joins can be sensitive to pipeline stalls and memory behavior. Your notes show +22% MergeJoin overhead in some workloads. Using 10% is conservative (“soft base”) and is later increased when an input is an IndexScan (more pointer chasing / irregular access).

#### IndexScan add-on: `+ 0.04`
- **Magic number:** `0.04`
- **Meaning:** Add 4% if either input is an IndexPath.
- **Rationale:** Index-driven inputs can increase irregular access and stall behavior; this add-on introduces a targeted steer without blanket penalization.

---

### 3.11 Hash Join — L3 cache spill modeling

#### Per-tuple overhead constant: `inner_width + 16`
- **Magic number:** `16`
- **Meaning:** Add 16 bytes per tuple for hash-table overhead (pointers, hash value, alignment).
- **Why 16 bytes:**  
  Hash tables typically store additional metadata beyond tuple payload. 16 bytes is a reasonable conservative estimate that captures the “hidden” overhead and helps detect L3 spill more accurately.

#### Safe-cache exemption: `if (hash_table_size_kb <= tee_safe_cache_kb) return;`
- **Default:** 16MB
- **Rationale:** Avoid penalizing small hash builds that remain cache-resident and are often optimal.

#### Spill ratio cap: `if (spill_ratio > 2.5) spill_ratio = 2.5;`
- **Magic number:** `2.5`
- **Meaning:** Cap how far beyond L3 we consider in penalty ramp.
- **Why 2.5×:**  
  Beyond ~2–3× L3 size, you are firmly memory-bound and additional penalty scaling yields diminishing planner benefit but risks overcorrecting away from hash join even when it is still the best option.

#### Penalty slope: `0.05`
- **Magic number:** `0.05`
- **Meaning:** Penalty factor:
  ```
  penalty_factor = 1 + 0.05 * (spill_ratio - 1)
  ```
- **Why 5% per “L3 multiple”:**  
  This is intentionally gentle. Hash join is often robust and beneficial; over-penalizing it can cause regressions (e.g., switching to nested loops). A 5% ramp provides bias without domination.

#### “Fits in cache” multiplier: `1.02`
- **Magic number:** `1.02`
- **Meaning:** Small 2% uplift even when hash table fits in (L3) cache.
- **Rationale:** Hash join still incurs some TEE overhead (hashing, metadata checks, memory traffic). 2% is a nudge, not a hammer.

---

### 3.12 Nested Loop — random access amplification

#### Base multiplier: `1.02`
- **Magic number:** `1.02`
- **Meaning:** 2% default uplift for nested loop joins.
- **Why 2%:**  
  The bottleneck notes show nested loop overhead ~+26–27% in some workloads. A uniform +26% would cause many regressions because nested loops are often correct for highly selective joins. 2% is a conservative baseline.

#### Index-driven inner multiplier: `1.06`
- **Magic number:** `1.06`
- **Meaning:** 6% uplift when inner is IndexScan or IndexOnlyScan.
- **Rationale:** Index-driven nested loops can amplify pointer chasing and irregular access patterns under TEE. 6% is enough to differentiate from non-index inners without forbidding the pattern.

#### Large-outer threshold and multiplier: `outer_rows > 1000 → 1.12`
- **Magic numbers:** `1000.0`, `1.12`
- **Meaning:** If outer is “large” and inner is index-driven, nested loop amplification becomes more severe; apply 12% uplift.
- **Why 1000:**  
  Past ~1k outer rows, repeated index probes often transition from “selective lookup” to “many repeated probes,” where overhead compounds. 12% is a moderate steering force that still allows nested loop when truly best.

---

## 4) GUC range bounds (“magic numbers” that protect tuning safety)

These ranges are important because they define safe tuning envelopes.

- `io_overhead_pct`, `cpu_overhead_pct`, `memoize_overhead_pct`: `0.0 … 5.0`  
  Rationale: allow aggressive experimentation (up to +500%) but prevent negative or nonsensical values.

- `gather_overhead_pct`: `0.0 … 10.0`  
  Gather overhead can be extreme in some systems; allow wider exploration.

- `l3_cache_kb`: `1024 … 1024*1024` (1MB to 1GB)  
  Supports diverse CPUs and environments while preventing tiny/invalid caches.

- `safe_cache_kb`: `0 … 1024*1024`  
  Allows disabling the safe-cache filter (0) or setting it large for very cache-rich systems.

---

## 5) Code consistency notes (important for credibility)

### 5.1 Duplicate assignment bug in `_PG_fini`
```
cost_agg_hook = prev_cost_agg_hook;
cost_agg_hook = prev_cost_agg_hook;
```
- This appears twice; one line is redundant (likely a copy/paste typo). It does not change behavior but is worth cleaning to avoid confusion in review.

### 5.2 Comment/value mismatch in Agg tax
- We observed a “~20% CPU tax,” but the code applies `tee_cpu_overhead_pct` (default 6%).  
  The implementation seems intentionally softened; update the comment or introduce an Agg-specific GUC if 20% was the intended default.

---

## 6) Tuning guide (how to adjust defaults scientifically)

### If your TEE platform shows larger I/O amplification
- Increase `tee_cost_model.io_overhead_pct` (e.g., 0.08 → 0.12–0.20)
- Consider tying `get_io_tax()` to `seq_page_cost`/`random_page_cost` if you want taxes to respect storage configuration (code change).

### If CPU-heavy operators dominate overhead
- Increase `tee_cost_model.cpu_overhead_pct` (e.g., 0.06 → 0.10–0.20)
- Consider adding a dedicated Agg tax if aggregation overhead is consistently larger than sorting overhead on your workload.

### If Hash Join is still chosen too aggressively and spills cache
- Reduce `tee_cost_model.l3_cache_kb` (spill detected earlier), or
- Increase the spill slope (0.05 → 0.08) carefully, or
- Increase `tee_cost_model.safe_cache_kb` only if small hash builds should remain exempt.

### If parallel plans are discouraged too much
- Decrease `tee_cost_model.gather_overhead_pct`, or
- Reduce row adjustments (1.20/1.08) if your system handles small-result parallelism well.

### If nested loops regress too much (become too rare)
- Reduce index-driven multipliers (1.06, 1.12) or increase the large-outer threshold (1000 → 5000).

---

## 7) One-line cheat sheet (what each number accomplishes)

- **0.08 IO tax:** conservative page-touch penalty to reflect bounce-buffer/encryption overhead without forcing plan flips.
- **0.06 CPU tax:** gentle processing overhead for sort/agg; prevents overreaction to mixed bottlenecks.
- **0.10 gather tax:** acknowledges parallel coordination overhead; scaled gently by workers and row count.
- **0.12 memoize tax:** moderated memoize maintenance cost with entry-aware scaling.
- **L3=32MB, safe=16MB:** stable knee points for cache spill detection and small-workload exemptions.
- **Index tree fanout 300, per-level 0.005:** models pointer-chasing + metadata overhead in index descent.
- **Materialize quarter-rate 0.25:** treats spooling as “not fully I/O-like,” but not free.
- **WindowAgg 0.06:** soft multiplier aligned with observed overhead but reduced for stability.
- **Memoize 2000/500 + 0.08 + cap 2:** penalize big caches, protect small caches, bound the effect.
- **Gather thresholds 2/4, row thresholds 1k/10k:** capture coordination overhead regimes.
- **MergeJoin 0.10 + 0.04 with Index:** model pipeline stalls, stronger under index-driven inputs.
- **HashJoin +16 bytes, spill cap 2.5, slope 0.05, fits 1.02:** size-aware, gentle cache spill modeling.
- **NestLoop 1.02 base, 1.06 index, 1.12 if outer>1k:** capture random-access amplification while keeping selective NL viable.

---

## Appendix: Parameter/Constant → Code mapping

| Parameter / Constant | Default | Where used / what it controls |
|---|---:|---|
| `tee_cost_model.enable` | `true` | master enable switch |
| `tee_cost_model.io_overhead_pct` | `0.08` | `get_io_tax(pages)` for SeqScan/Index/Bitmap/Materialize |
| `tee_cost_model.cpu_overhead_pct` | `0.06` | Sort/Agg incremental processing tax |
| `tee_cost_model.gather_overhead_pct` | `0.10` | Gather/GatherMerge coordination tax |
| `tee_cost_model.memoize_overhead_pct` | `0.12` | Memoize rescan startup/total scaling |
| `tee_cost_model.l3_cache_kb` | `32768` | HashJoin L3 spill boundary |
| `tee_cost_model.safe_cache_kb` | `16384` | skip taxes for cache-resident workloads |
| `get_io_tax base` | `1.0` | normalized per-page unit in tax formula |
| `index fanout base` | `300.0` | tree height estimate for B-tree |
| `index level penalty` | `0.005` | per-row per-level cost add for index descent |
| `materialize rate` | `0.25` | quarter-rate IO tax for materialize pages |
| `windowagg overhead` | `0.06` | multiplicative WindowAgg cost factor |
| `memoize denom` | `2000.0` | entry scaling reference point |
| `memoize cap` | `2.0` | bounds entry-based ratio |
| `memoize slope` | `0.08` | entry penalty slope |
| `memoize small-exempt` | `500.0` | skip heavy penalties for small caches |
| `memoize startup factor` | `0.40` | apply only 40% of memoize pct to startup |
| `gather worker knees` | `2`, `4` | scale comms penalty for larger worker counts |
| `gather worker scale` | `1.05`, `1.10` | gentle scaling factors |
| `gather row knees` | `1000`, `10000` | boost penalty for small outputs |
| `gather row adj` | `1.20`, `1.08` | row-based adjustments |
| `gather weights` | `0.25`, `0.10` | apply modestly to startup/total |
| `mergejoin base` | `0.10` | base stall penalty |
| `mergejoin index add` | `0.04` | extra stall penalty if input is index-driven |
| `hashjoin overhead bytes` | `16` | hash-table per-tuple metadata estimate |
| `hash spill cap` | `2.5` | bound spill ratio |
| `hash spill slope` | `0.05` | penalty ramp rate |
| `hash fits factor` | `1.02` | small tax even when cache-fitting |
| `nestloop base` | `1.02` | conservative baseline |
| `nestloop index` | `1.06` | inner index-driven amplification |
| `nestloop outer threshold` | `1000` | “many probes” knee point |
| `nestloop big multiplier` | `1.12` | stronger amplification for large outer * index inner |

