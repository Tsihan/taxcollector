# TEE Join Enumerator — “Magic Numbers” and Default Rationale

This document explains every “magic number” (hard-coded constant and default GUC value) in the provided `tee_join_enumerator.c` join enumeration plugin. It is written to be **auditable**, **defensible**, and **practical for tuning** under AMD SEV-SNP / TEE conditions.

> **Design intent (high level):**  
> Join enumeration determines the *shape* of the join tree. Under TEE, bad early join choices can be disproportionately expensive because large intermediates amplify (1) I/O work (page movement / decryption / bounce buffering proxies) and (2) memory footprint (RMP-like overhead proxies).  
> This plugin introduces a lightweight “TEE Tax” score to bias early join construction toward **lower I/O + lower memory footprint** pairs—*but only for the first few levels*, then it switches to a fast path to avoid planner slowdowns.

---

## 1) Mental model: what the plugin is modeling

### 1.1 Why “Tax Score” = IO proxy + Memory proxy
The score is intentionally simple and uses planner-visible fields:

- **IO proxy:** `left->pages + right->pages`  
  Pages correlate with scan / access work; in TEE settings, page movement and misses tend to have higher effective cost.

- **Memory proxy:** `(left->rows * left_width) + (right->rows * right_width)`  
  This approximates “bytes flowing through” and the size of data structures a join might touch or materialize. Under TEE, memory-heavy paths can see amplified overhead (metadata checks, bandwidth pressure, etc.).

This is a *ranking heuristic*, not a physical simulator: the goal is to **prefer “small + cheap” foundations** for the join tree.

### 1.2 Why only optimize the first N levels (“Hybrid Search Strategy”)
Join enumeration grows combinatorially as the join level increases. Computing scores and sorting all candidate pairs at high levels can add noticeable planning overhead. The plugin therefore uses:

1) **Deep Optimization** (levels ≤ `tee_jn_tax_level_limit`): score + sort + prune tail  
2) **Fast Path** (levels > limit): skip scoring/sorting, behave like standard enumeration

This structure aims to capture the biggest win: **prevent early catastrophic fanout / heavy intermediates**, without paying full scoring overhead for every level.

---

## 2) Parameter catalog: every magic number and its rationale

### 2.1 Feature switches

#### `tee_jn_enabled` (GUC: `tee_join_enumerator.jn_enabled`)
- **Default:** `true`
- **Meaning:** Master on/off switch for the plugin.
- **Rationale:** Enables the feature by default for reproducibility in TEE experiments. Still user-settable (PGC_USERSET) to allow quick A/B testing without rebuilds.

---

## 2.2 Hybrid strategy boundary

#### `tee_jn_tax_level_limit` (GUC: `tee_join_enumerator.jn_tax_level_limit`)
- **Default:** `3`
- **Range:** `1 … 100`
- **Meaning:** Apply Tax scoring + sorting only for the first N join levels.
- **Why 3 is defensible:**  
  The “foundation” of the join tree is set in the first few levels. Choosing low-tax joins early often prevents the “snowball effect” where one bad early join creates a large intermediate that dominates all later decisions.  
  Level 2–3 is a sweet spot:
  - Low overhead: fewer candidates than later levels.
  - High leverage: early intermediate sizes heavily influence the rest of the plan.
  - Stable: avoids overfitting enumeration at high levels where estimates are noisier and combinations explode.

- **Why allow up to 100:**  
  The cap supports experimentation on very wide queries, but the recommended operational range is typically small (e.g., 2–6). A large upper bound makes the GUC flexible without enforcing an arbitrary hard stop.

---

## 2.3 Scoring weights (IO vs memory pressure)

The Tax score is:
```
score = ( (left_pages + right_pages) * tee_jn_io_weight )
      + ( (left_rows*left_width + right_rows*right_width) * tee_jn_rmp_weight )
```

#### `tee_jn_io_weight` (GUC: `tee_join_enumerator.jn_io_weight`)
- **Default:** `2.0`
- **Range:** `0 … 1000`
- **Meaning:** Multiplier for the I/O proxy term (pages).
- **Why 2.0:**  
  A modest IO bias reflects that in many TEE settings, the “effective cost per page” is higher than in native execution, especially as working sets grow and miss rates rise. A factor of 2 is strong enough to reorder candidates when page footprints differ materially, without totally dominating memory effects.

#### `tee_jn_rmp_weight` (GUC: `tee_join_enumerator.jn_rmp_weight`)
- **Default:** `1.0`
- **Range:** `0 … 1000`
- **Meaning:** Multiplier for the memory-footprint proxy (bytes).
- **Why 1.0:**  
  Memory effects are important but harder to calibrate universally from planner stats alone. Setting this to 1.0 keeps it influential, but the IO term (with default 2.0) gets slightly more priority—consistent with the goal of avoiding “large-scan/large-IO” foundations early.

> **Important practical note (units):**  
> `pages` and `bytes` are different units. This heuristic intentionally does not try to normalize to a single physical unit because it is a **ranker**, not a cost estimator. Weights are the tuning knobs that reconcile these terms for your platform.

---

## 2.4 Candidate pruning (soft limits)

#### `tee_jn_generation_limit` (GUC: `tee_join_enumerator.jn_generation_limit`)
- **Default (in `_PG_init`):** `20`
- **Range:** `1 … 1000`
- **Meaning:** A soft cap on how many join candidate pairs are attempted per level *in heuristic mode* (levels ≤ limit). After sorting by Tax score, only the first K candidates are tried.
- **Why a soft limit exists:**  
  Even at low levels, candidate generation can be large for multi-relation queries. A soft cap provides a planning-time budget: try the best-looking candidates first, and cut off the expensive tail once at least one joinrel is found.

- **Why 20 is defensible:**  
  20 typically preserves a diverse set of promising candidates while bounding planning overhead. The goal is not to fully search all options, but to prevent exploring a long tail of obviously expensive pairs that are unlikely to produce the best plan under TEE.

##### **Code consistency note (important):**
In the global declarations:
```c
static int tee_jn_generation_limit = 10;
```
but the GUC default is registered as **20**:
```c
DefineCustomIntVariable(..., &tee_jn_generation_limit, 20, ...);
```
The GUC registration will overwrite the static initializer during `_PG_init`, so runtime default becomes **20**.  
However, this mismatch is a maintainability hazard (and confusing during code review). Best practice is to **make these match** (either set the static initializer to 20, or register 10).

---

## 2.5 Hard-coded constants in the scoring / enumeration logic

### 2.5.1 Width fallback in `rel_width_bytes()`

#### `return 8.0;` fallback width
- **Meaning:** If `reltarget->width` is unknown, assume 8 bytes/tuple.
- **Why 8 bytes is reasonable as a *fallback*:**  
  This is a conservative “small tuple” stand-in—roughly the size of a single 64-bit value. It prevents the memory term from becoming zero (which would under-penalize unknown-width relations).  
  Because this is only a fallback, typical relations with known target widths will not use it.

**Trade-off:** If your relations often have unknown widths and are actually wide, consider increasing this fallback (e.g., 16 or 32) or improving width availability in the planner inputs.

### 2.5.2 Cartesian join penalty multiplier

#### `* 100.0` penalty for clauseless (Cartesian) candidates
- **Location:** when `old_rel` has no joininfo/eclass restrictions, candidates are treated as clauseless; their score is multiplied:
```c
cand->score = calculate_join_tax_score(old_rel, other_rel) * 100.0;
```
- **Meaning:** Heavily de-prioritize Cartesian products during heuristic ordering.
- **Why 100×:**  
  Cartesian joins are almost always catastrophic in cost and cardinality unless forced by query structure. In TEE settings, “catastrophic” becomes “even more catastrophic,” because:
  - huge intermediates amplify memory pressure,
  - additional scanning/probing increases I/O and miss rates,
  - poor early join shapes can dominate total runtime.  
  A 100× factor is deliberately large to push these candidates to the end of the sorted list, *without forbidding them entirely* (the fallback logic still ensures joins can be generated if needed).

### 2.5.3 “Bytes per MB” conversion helper

#### `mb_to_bytes(mb) { return mb * 1024.0 * 1024.0; }`
- **Constants:** `1024.0 * 1024.0`
- **Meaning:** Standard binary MB conversion.
- **Why included:** It’s a utility for future extensions (not currently used in the shown code). The constants are conventional and not tuning knobs.

### 2.5.4 Soft limit cut-off condition details

The pruning loop stops when:
```c
generated_count >= tee_jn_generation_limit &&
list_length(joinrels[level]) > 0
```
- **Meaning:** Only cut off once at least one join relation at this level exists (avoid cutting so early that nothing is produced).
- **Rationale:** Prevents “premature pruning” that could leave a join level empty, which would break planning. This is a stability guard.

### 2.5.5 Candidate sort order
The comparator sorts by increasing score:
- Lower Tax → earlier attempt
- Higher Tax → pruned first

This is not a “magic number” but it is a “design constant”: it encodes the policy “TEE prefers low IO + low footprint foundations.”

---

## 2.6 Safety fallback behavior (non-numeric but critical)

### Forced Cartesian fallback when heuristic mode generates nothing
If heuristic mode produces no joinrels for a level, the code forces cartesian joins:
```c
if (use_heuristic && joinrels[level] == NIL) { ... force cartesian ... }
```
- **Meaning:** Even with pruning and penalties, the enumerator must never fail to produce candidates.
- **Rationale:** Planner correctness > heuristic purity. This makes the heuristic “safe to enable” because it cannot wedge the planner into an error state due to over-pruning.

---

## 3) Default value provenance: why these defaults are persuasive starting points

These defaults are motivated by three pragmatic constraints:

1) **High leverage early:** Levels 2–3 determine intermediate sizes that drive the rest of the plan. Hence `tee_jn_tax_level_limit = 3`.
2) **Planner time budget:** Scoring/sorting all candidates at high levels is expensive. Hence the hybrid strategy and `jn_generation_limit`.
3) **TEE asymmetry:** Under TEE, I/O-like effects often become more painful earlier than expected. Hence `jn_io_weight = 2.0` biasing away from page-heavy combinations.

Importantly, the heuristic is **ranking-only**: it reorders candidate exploration, it does not directly rewrite Postgres’s cost equations. That makes defaults easier to defend because they influence **search order and breadth**, not correctness or physical operator semantics.

---

## 4) Tuning guide (how to adjust defaults scientifically)

### If planning overhead is too high
- Decrease `tee_join_enumerator.jn_tax_level_limit` (e.g., 3 → 2)
- Decrease `tee_join_enumerator.jn_generation_limit` (e.g., 20 → 10)
- Consider disabling heuristic for bushy joins (code change) if that dominates cost

### If join order still produces large intermediates early
- Increase `tee_join_enumerator.jn_tax_level_limit` slightly (3 → 4 or 5)
- Increase `tee_join_enumerator.jn_generation_limit` (20 → 40) to explore more low-tax options
- Increase `tee_join_enumerator.jn_rmp_weight` if memory-driven failures dominate

### If the heuristic over-penalizes wide-but-necessary joins
- Reduce `tee_join_enumerator.jn_rmp_weight` (1.0 → 0.5)
- Or reduce the Cartesian penalty multiplier (100 → 30) if you have queries where clause-less joins are legitimately required

### Calibrating IO vs memory weights (practical method)
- Run `EXPLAIN (ANALYZE, BUFFERS)` on representative queries under non-TEE and TEE.
- If runtime inflates mostly with higher buffer read / I/O phases: increase `jn_io_weight`.
- If runtime inflates mostly in hash/sort/materialize phases: increase `jn_rmp_weight`.
- Keep weights modest; you want *ranking pressure*, not rigid rules.

---

## 5) One-line cheat sheet for each number

- **`jn_tax_level_limit = 3`**: Optimize the join tree foundation where it matters most; keep planning time low.
- **`jn_io_weight = 2.0`**: Slightly prioritize avoiding page-heavy joins under TEE.
- **`jn_rmp_weight = 1.0`**: Keep memory footprint influential but not dominating by default.
- **`jn_generation_limit = 20`**: Bound planning overhead; try the best candidates first.
- **`width fallback = 8.0` bytes**: Prevent zero-width memory term; conservative small-row default.
- **`Cartesian penalty = 100×`**: Strongly de-prioritize clause-less joins, but don’t forbid them.
- **`1024×1024`**: Conventional MB→bytes conversion (utility constant).

---

## Appendix A: Parameter-to-code mapping

| Parameter / Constant | Default | Code location / usage |
|---|---:|---|
| `tee_join_enumerator.jn_enabled` | `true` | master enable switch |
| `tee_join_enumerator.jn_tax_level_limit` | `3` | heuristic applied only for `level <= limit` |
| `tee_join_enumerator.jn_io_weight` | `2.0` | pages term multiplier |
| `tee_join_enumerator.jn_rmp_weight` | `1.0` | footprint term multiplier |
| `tee_join_enumerator.jn_generation_limit` | `20` | soft cap on candidates tried per level (heuristic mode) |
| `static initializer jn_generation_limit` | `10` | **inconsistent** with GUC default; should be aligned |
| `width fallback` | `8.0` bytes | used if `reltarget->width` unavailable |
| `Cartesian penalty multiplier` | `100.0` | `score *= 100` for clause-less candidates |
| `MB conversion` | `1024*1024` | `mb_to_bytes` helper |
| `GUC ranges` | `1..100`, `0..1000`, `1..1000` | tuning bounds for safety and flexibility |

---

## Appendix B: What this plugin does *not* claim

- It does **not** claim the Tax score equals true runtime cost.
- It does **not** replace PostgreSQL’s cost model.
- It intentionally targets **search strategy**: “explore cheaper-looking join pairs first” early, then fall back to standard enumeration to preserve planning scalability.

This scoped design is why the defaults are persuasive: they introduce strong directional bias where it matters most (early join tree shape) without risking planner blow-ups at high levels.

