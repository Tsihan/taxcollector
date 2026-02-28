#include "postgres.h"
#include <math.h>
#include <limits.h>

#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "fmgr.h"
#include "float.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

PG_MODULE_MAGIC;

Datum tee_cardinality_estimation_activate(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(tee_cardinality_estimation_activate);

/* --- GUCs --- */
static bool enable_sev_snp_ce = true;

/*
 * NOTE:
 * We keep the original GUC name (sev_io_inflation_alpha) but we now use it to
 * inflate rel->pages (IO work proxy), not rel->rows. This makes a much bigger
 * difference for SeqScan and many IO-sensitive costs without touching cost code.
 */
static double sev_io_inflation_alpha = 3.0;

/*
 * Model RMP/memory tax by inflating effective row width (bytes/tuple) when
 * the working set spills beyond effective_cache_size.
 * This pushes planner away from hash/sort/materialize-heavy choices under SNP.
 */
static double sev_rmp_width_beta = 0.0;

/* Optional: keep old row inflation OFF by default to avoid destroying cardinalities */
static double sev_rows_inflation_gamma = 0.0;

/*
 * In SNP, effective cache is often "smaller" due to RMP overhead and higher
 * miss penalties. We model that by scaling effective_cache_size used for spill checks.
 * 0.5 means we treat cache as half as effective.
 */
static double sev_cache_size_scale = 0.5;

/* Ignore tiny over-cache cases to avoid plan instability near the boundary. */
static double sev_spill_grace_ratio = 0.25;

/*
 * Join-level penalties: discourage creating large/high-fanout intermediates early.
 * This helps prevent catastrophic join reorders like pushing a one-to-many join
 * (e.g., title -> cast_info) above a selective dimension filter.
 */
static double sev_join_spill_beta = 0.32;
static double sev_join_fanout_beta = 0.6;
static double sev_join_fanout_threshold = 2.7;
static double sev_max_join_rows_factor = 3.5;
static double sev_join_rows_cap = 1.1;   /* cap exposed row inflation; width can still grow */
static double sev_join_skip_rows = 12000.0; /* skip join penalties when outputs are tiny */


/* Small table protection */
static int sev_small_table_threshold_pages = 2000; /* ~16MB at 8KB pages */

/* Safety caps */
static double sev_max_pages_factor = 10.0;     /* cap rel->pages inflation */   
static double sev_max_width_factor = 4.0;      /* cap width inflation */    
static double sev_width_exponent   = 1.2;      /* (width/16)^exp */     

/* Saved hooks */
static set_baserel_size_estimates_hook_type prev_baserel_hook = NULL;
static set_joinrel_size_estimates_hook_type prev_joinrel_hook = NULL;
static get_relation_stats_hook_type prev_relstats_hook = NULL;

#define SEV_ROW_WIDTH_UNIT 16.0
#define SEV_MIN_WIDTH 1.0

static double
estimate_width_local(PlannerInfo *root, RelOptInfo *rel);

/* Clamp helpers */
static double
clamp_card_est_safe(double x)
{
    if (isnan(x) || x < 0.0)
        return 0.0;
    if (x > 1e100)
        return 1e100;
    return x;
}

static double
clamp_row_est_safe(double nrows)
{
    if (nrows > 1e100 || isnan(nrows))
        nrows = 1e100;
    else if (nrows <= 1.0)
        nrows = 1.0;
    else
        nrows = rint(nrows);
    return nrows;
}

static BlockNumber
clamp_blocknumber_safe(double pages)
{
    if (isnan(pages) || pages < 0.0)
        pages = 0.0;
    if (pages > (double) UINT_MAX)
        pages = (double) UINT_MAX;
    return (BlockNumber) rint(pages);
}

static int
clamp_width_safe(double w)
{
    if (isnan(w) || w < 1.0)
        w = 1.0;
    if (w > (double) INT_MAX)
        w = (double) INT_MAX;
    return (int) rint(w);
}

/*
 * Helper: working-set spill (relative) beyond effective_cache_size.
 * Returns 0 if fits, else (rel_pages/cache_pages - 1), capped.
 */
static double
calculate_cache_spill_excess(RelOptInfo *rel)
{
    double rel_pages = (double) rel->pages;
    /* scale effective cache for SNP */
    double cache_pages = (double) effective_cache_size * sev_cache_size_scale;
    double grace = Max(sev_spill_grace_ratio, 0.0);
    double excess = 0.0;

    if (cache_pages <= 0.0)
        return 0.0;

    /* Small table protection */
    if (rel_pages < (double) sev_small_table_threshold_pages)
        return 0.0;

    /*
     * Grace zone: if only slightly above cache, don't inflate (avoids flapping).
     * excess is normalized so that:
     *   rel_pages = cache*(1+grace)  => excess=0
     *   rel_pages = cache*2.0        => excess= (2 - (1+grace))
     */
    if (rel_pages <= cache_pages * (1.0 + grace))
        return 0.0;

    excess = (rel_pages / cache_pages) - (1.0 + grace);

    /* cap to avoid crazy explosions downstream */
    if (excess > 9.0)
        excess = 9.0;

    return excess;
}

/*
 * Core: apply TEE penalties WITHOUT touching cost code.
 * - IO tax: inflate rel->pages (proxy for IO work under bounce-buffer/encryption)
 * - RMP tax: inflate reltarget->width (proxy for memory/bandwidth overhead)
 * - Optional: (disabled by default) inflate output rows a bit (gamma)
 */
static double
apply_sev_tee_inflation(PlannerInfo *root, RelOptInfo *rel, double nrows)
{
    double spill_excess;
    double width;
    double width_factor;
    double pages_factor = 1.0;
    double eff_width_factor = 1.0;

    if (!enable_sev_snp_ce)
        return nrows;

    spill_excess = calculate_cache_spill_excess(rel);
    if (spill_excess <= 0.0)
        return nrows;

    /* width */
    width = (rel->reltarget && rel->reltarget->width > 0) ? (double) rel->reltarget->width : 0.0;
    if (width <= 0.0)
        width = estimate_width_local(root, rel);
    width = Max(width, SEV_MIN_WIDTH);

    /* base width factor: (width/16)^exp */
    width_factor = width / SEV_ROW_WIDTH_UNIT;
    width_factor = pow(width_factor, sev_width_exponent);
    if (width_factor > sev_max_width_factor)
        width_factor = sev_max_width_factor;

    /*
     * IO pages inflation:
     * Use alpha * spill_excess (so 2x cache => excess=1.0).
     * This directly impacts SeqScan + many IO-related costs.
     */
    if (sev_io_inflation_alpha > 0.0)
    {
        pages_factor = 1.0 + (sev_io_inflation_alpha * spill_excess);
        if (pages_factor > sev_max_pages_factor)
            pages_factor = sev_max_pages_factor;

        rel->pages = clamp_blocknumber_safe(ceil((double) rel->pages * pages_factor));
    }

    /*
     * RMP / memory-tax as "effective width" inflation:
     * We scale width more when spill happens, and also with base width_factor.
     * This pushes hash/sort/materialize to look bigger (more bytes).
     */
    if (sev_rmp_width_beta > 0.0 && rel->reltarget)
    {
        eff_width_factor = 1.0 + (sev_rmp_width_beta * spill_excess * width_factor);
        if (eff_width_factor > sev_max_width_factor)
            eff_width_factor = sev_max_width_factor;

        rel->reltarget->width = clamp_width_safe(ceil((double) rel->reltarget->width * eff_width_factor));
    }

    /*
     * Optional: very small row inflation (OFF by default).
     * Keep this conservative; pages/width already drive most plan changes.
     */
    if (sev_rows_inflation_gamma > 0.0)
    {
        double row_factor = 1.0 + (sev_rows_inflation_gamma * spill_excess * width_factor);
        if (row_factor > 10.0)
            row_factor = 10.0;
        nrows *= row_factor;
    }

    return nrows;
}

static double
compute_base_rows(PlannerInfo *root, RelOptInfo *rel)
{
    return rel->tuples * clauselist_selectivity(root, rel->baserestrictinfo, 0, JOIN_INNER, NULL);
}

static double
compute_join_rows(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                  double outer_rows, double inner_rows, SpecialJoinInfo *sjinfo, List *restrictlist)
{
    JoinType jointype = sjinfo->jointype;
    Selectivity jselec = clauselist_selectivity(root, restrictlist, 0, jointype, sjinfo);
    double nrows;

    switch (jointype)
    {
        case JOIN_INNER:
        case JOIN_LEFT:
        case JOIN_FULL:
            nrows = outer_rows * inner_rows * jselec;
            if (IS_OUTER_JOIN(jointype) && nrows < outer_rows) nrows = outer_rows;
            break;
        case JOIN_SEMI:
            nrows = outer_rows * jselec;
            break;
        case JOIN_ANTI:
            nrows = outer_rows * (1.0 - jselec);
            break;
        default:
            nrows = outer_rows * inner_rows * jselec;
            break;
    }
    return nrows;
}

/*
 * Optional join working-set width inflation:
 * If join working set (outer_rows*outer_width + inner_rows*inner_width) exceeds cache,
 * inflate joinrel->reltarget->width. This helps when each base rel fits cache but the
 * join build/probe structure doesn't.
 */
static double
apply_sev_join_penalty(PlannerInfo *root, RelOptInfo *joinrel,
                       RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                       double nrows)
{
    double cache_pages = (double) effective_cache_size * sev_cache_size_scale;
    double grace = Max(sev_spill_grace_ratio, 0.0);
    double width;
    double out_pages;
    double spill_excess = 0.0;
    double fanout = 1.0;
    double factor = 1.0;
    double row_factor;
    double width_base = 32.0;
    double width_factor;
    double outer_rows = outer_rel ? clamp_card_est_safe(outer_rel->rows) : 0.0;
    double inner_rows = inner_rel ? clamp_card_est_safe(inner_rel->rows) : 0.0;

    if (!enable_sev_snp_ce)
        return nrows;
    if (cache_pages <= 0.0)
        return nrows;
    if (sev_join_spill_beta <= 0.0 && sev_join_fanout_beta <= 0.0)
        return nrows;
    if (clamp_card_est_safe(nrows) <= sev_join_skip_rows &&
        outer_rows <= sev_join_skip_rows &&
        inner_rows <= sev_join_skip_rows)
        return nrows;

    width = (joinrel && joinrel->reltarget && joinrel->reltarget->width > 0) ?
            (double) joinrel->reltarget->width : 32.0;

    /* (A) Output working-set spill penalty (bytes of tuples flowing upward). */
    out_pages = (clamp_card_est_safe(nrows) * width) / (double) BLCKSZ;
    if (out_pages > cache_pages * (1.0 + grace))
    {
        spill_excess = (out_pages / cache_pages) - (1.0 + grace);
        if (spill_excess > 9.0)
            spill_excess = 9.0;

        if (sev_join_spill_beta > 0.0)
            factor *= 1.0 + (sev_join_spill_beta * spill_excess);
    }

    /*
     * (B) Fanout penalty: if the join expands rows a lot relative to either input,
     * it tends to amplify later nested loops (common root-cause of big regressions).
     * Use a smooth log growth to avoid overreacting.
     */
    if (outer_rel && outer_rel->rows > 1.0)
        fanout = Max(fanout, clamp_card_est_safe(nrows) / clamp_card_est_safe(outer_rel->rows));
    if (inner_rel && inner_rel->rows > 1.0)
        fanout = Max(fanout, clamp_card_est_safe(nrows) / clamp_card_est_safe(inner_rel->rows));

    if (sev_join_fanout_beta > 0.0 && fanout > sev_join_fanout_threshold)
    {
        double fanout_pen = log1p(fanout - sev_join_fanout_threshold);
        factor *= 1.0 + (sev_join_fanout_beta * fanout_pen);
    }

    if (factor > sev_max_join_rows_factor)
        factor = sev_max_join_rows_factor;
    row_factor = factor;

    /*
     * Shift most of the penalty into width inflation instead of row inflation.
     * This still makes hash/sort/materialize look more expensive but avoids
     * blowing up join cardinalities that can cause plan flips/regressions.
     */
    if (joinrel && joinrel->reltarget)
    {
        width_base = (joinrel->reltarget->width > 0) ?
                     (double) joinrel->reltarget->width :
                     estimate_width_local(root, joinrel);
        /* damp width inflation to avoid over-penalizing joins */
        width_factor = 1.0 + (row_factor - 1.0) * 0.5;
        if (width_factor > sev_max_width_factor)
            width_factor = sev_max_width_factor;
        joinrel->reltarget->width = clamp_width_safe(ceil(width_base * width_factor));
    }

    /*
     * Keep the exposed row inflation gentle; enough to bias away from
     * extreme fanout but capped to limit plan churn.
     */
    if (row_factor > sev_join_rows_cap)
        row_factor = sev_join_rows_cap;

    return nrows * row_factor;
}


/* --- Hooks --- */

static double
sev_baserel_hook(PlannerInfo *root, RelOptInfo *rel)
{
    double nrows;

    if (prev_baserel_hook)
        nrows = prev_baserel_hook(root, rel);
    else
        nrows = compute_base_rows(root, rel);

    /* Apply TEE penalties via pages/width (and optional mild rows) */
    nrows = apply_sev_tee_inflation(root, rel, nrows);

    return clamp_row_est_safe(nrows);
}

static double
sev_joinrel_hook(PlannerInfo *root, RelOptInfo *rel,
                 RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                 SpecialJoinInfo *sjinfo, List *restrictlist)
{
    double nrows;
    double outer_rows = outer_rel->rows;
    double inner_rows = inner_rel->rows;

    if (prev_joinrel_hook)
        nrows = prev_joinrel_hook(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);
    else
        nrows = compute_join_rows(root, rel, outer_rel, inner_rel, outer_rows, inner_rows, sjinfo, restrictlist);

    /*
     * Do NOT inflate join rows aggressively (keep cardinality stable).
     * Only inflate join output *width* when join working set spills cache,
     * which influences hash/sort/materialize decisions under SNP.
     */
    nrows = apply_sev_join_penalty(root, rel, outer_rel, inner_rel, nrows);

    return clamp_row_est_safe(nrows);
}

static bool
sev_relstats_hook(PlannerInfo *root, RangeTblEntry *rte, AttrNumber attnum,
                  VariableStatData *vardata)
{
    bool result = false;
    if (prev_relstats_hook)
        result = prev_relstats_hook(root, rte, attnum, vardata);

    /* Hook kept but distinct logic disabled for stability */
    return result;
}

static double
estimate_width_local(PlannerInfo *root, RelOptInfo *rel)
{
    double width = 0.0;
    ListCell *lc;

    if (!rel || !rel->reltarget) return 32.0;

    foreach(lc, rel->reltarget->exprs)
    {
        Node *node = (Node *) lfirst(lc);
        int32 item_width = get_typavgwidth(exprType(node), exprTypmod(node));
        if (item_width > 0) width += item_width;
    }

    if (width <= 0.0) width = 32.0;
    return width;
}

void
_PG_init(void)
{
    DefineCustomBoolVariable("tee_cardinality_estimation.enable_sev_snp_ce",
        "Enable SEV-SNP-aware cardinality/size heuristics.",
        NULL, &enable_sev_snp_ce, true, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    /* Reused: now inflates rel->pages when spilling cache */
    DefineCustomRealVariable("tee_cardinality_estimation.sev_io_inflation_alpha",
        "Scales rel->pages inflation based on cache spill (proxy for TEE IO tax).",
        NULL, &sev_io_inflation_alpha, 3.0, 0.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_rmp_width_beta",
        "Inflates effective tuple width under cache spill (proxy for RMP/memory tax).",
        NULL, &sev_rmp_width_beta, 0.0, 0.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_rows_inflation_gamma",
        "Optional mild row inflation under cache spill (0.0 = disabled).",
        NULL, &sev_rows_inflation_gamma, 0.0, 0.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    
    DefineCustomRealVariable("tee_cardinality_estimation.sev_cache_size_scale",
        "Scale factor for effective_cache_size used by SNP spill detection (0.5 = treat cache as half-effective).",
        NULL, &sev_cache_size_scale, 0.5, 0.05, 2.0, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_spill_grace_ratio",
        "Grace ratio above cache before spill penalties apply (reduces plan flapping near boundary).",
        NULL, &sev_spill_grace_ratio, 0.25, 0.0, 10.0, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_join_spill_beta",
        "Inflate joinrel rows when estimated join output spills cache (TEE IO/RMP proxy).",
        NULL, &sev_join_spill_beta, 0.32, 0.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_join_fanout_beta",
        "Inflate joinrel rows for high-fanout joins (prevents fanout amplification in nested loops).",
        NULL, &sev_join_fanout_beta, 0.6, 0.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_join_fanout_threshold",
        "Fanout threshold above which join fanout penalty kicks in (e.g., 2.0 means >2x expansion).",
        NULL, &sev_join_fanout_threshold, 2.7, 1.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_max_join_rows_factor",
        "Cap for join-level row inflation factor.",
        NULL, &sev_max_join_rows_factor, 3.5, 1.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_join_rows_cap",
        "Cap applied to join row inflation (width inflation may use a larger factor).",
        NULL, &sev_join_rows_cap, 1.1, 1.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_join_skip_rows",
        "Skip join penalties when join output and inputs are tiny (protects small joins).",
        NULL, &sev_join_skip_rows, 12000.0, 1.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomIntVariable("tee_cardinality_estimation.sev_small_table_threshold_pages",
        "Threshold (pages) below which tables are NOT inflated.",
        NULL, &sev_small_table_threshold_pages, 2000, 0, INT_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_max_pages_factor",
        "Cap for rel->pages inflation factor.",
        NULL, &sev_max_pages_factor, 10.0, 1.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_max_width_factor",
        "Cap for reltarget->width inflation factor.",
        NULL, &sev_max_width_factor, 4.0, 1.0, DBL_MAX, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    DefineCustomRealVariable("tee_cardinality_estimation.sev_width_exponent",
        "Exponent for width penalty term (width/16)^exp.",
        NULL, &sev_width_exponent, 1.2, 0.0, 10.0, PGC_USERSET, GUC_EXPLAIN, NULL, NULL, NULL);

    EmitWarningsOnPlaceholders("tee_cardinality_estimation");

    prev_baserel_hook = set_baserel_size_estimates_hook;
    set_baserel_size_estimates_hook = sev_baserel_hook;

    prev_joinrel_hook = set_joinrel_size_estimates_hook;
    set_joinrel_size_estimates_hook = sev_joinrel_hook;

    prev_relstats_hook = get_relation_stats_hook;
    get_relation_stats_hook = sev_relstats_hook;
}

void
_PG_fini(void)
{
    set_baserel_size_estimates_hook = prev_baserel_hook;
    set_joinrel_size_estimates_hook = prev_joinrel_hook;
    get_relation_stats_hook = prev_relstats_hook;
}

Datum
tee_cardinality_estimation_activate(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(true);
}
