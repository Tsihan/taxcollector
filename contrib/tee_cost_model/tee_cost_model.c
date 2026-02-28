/*-------------------------------------------------------------------------
 *
 * tee_cost_model.c
 * Version V10.1: "Corrected & Full Spectrum Coverage"
 *
 * Fixes:
 * - Utilized the previously unused get_io_tax() function for physics-based IO penalty.
 * - Enabled Bitmap Heap Scan hook (missing in V10).
 * - Refined Materialize cost to use page-based estimation.
 *
 * Based on comprehensive bottleneck analysis from:
 * - CEB (Memoize +69%, NL +27%)
 * - TPC-DS (WindowAgg +13%, Limit +25%)
 * - Stack (Gather +151%, MergeJoin +22%)
 * - JOB (NestedLoop +26%, Sort +25%)
 *
 *-------------------------------------------------------------------------
 */

 #include "postgres.h"
 #include "fmgr.h"
 #include <math.h>
 #include "optimizer/cost.h"
 #include "optimizer/pathnode.h"
 #include "optimizer/plancat.h"
 #include "utils/guc.h"
 #include "utils/spccache.h"
 #include "utils/selfuncs.h"
 #include "access/amapi.h"
 #include "catalog/pg_am.h"
 #include "utils/rel.h" /* For BLCKSZ */
 
 PG_MODULE_MAGIC;
 
 Datum tee_cost_model_activate(PG_FUNCTION_ARGS);
 PG_FUNCTION_INFO_V1(tee_cost_model_activate);
 
 /* --- GUC Variables --- */
 static bool tee_enable_cost_model = true;
 
/* Baseline Taxes (softened to reduce regressions) */
static double tee_io_overhead_pct = 0.08;
static double tee_cpu_overhead_pct = 0.06;
static double tee_gather_overhead_pct = 0.10;
static double tee_memoize_overhead_pct = 0.12;
 static int    tee_l3_cache_kb = 32768; 
static int    tee_safe_cache_kb = 16384; /* ~16MB: treat as cache-resident */
 
 /* Hooks Storage */
 static cost_seqscan_hook_type prev_cost_seqscan_hook = NULL;
 static cost_index_hook_type prev_cost_index_hook = NULL;
 static cost_bitmap_heap_scan_hook_type prev_cost_bitmap_heap_scan_hook = NULL;
 static cost_sort_hook_type prev_cost_sort_hook = NULL;
 static cost_agg_hook_type prev_cost_agg_hook = NULL;
 static cost_windowagg_hook_type prev_cost_windowagg_hook = NULL;
 static cost_material_hook_type prev_cost_material_hook = NULL;
 static cost_memoize_rescan_hook_type prev_cost_memoize_rescan_hook = NULL;
 static cost_gather_hook_type prev_cost_gather_hook = NULL;
 static cost_gather_merge_hook_type prev_cost_gather_merge_hook = NULL;
 static final_cost_nestloop_hook_type prev_final_cost_nestloop_hook = NULL;
 static final_cost_mergejoin_hook_type prev_final_cost_mergejoin_hook = NULL;
 static final_cost_hashjoin_hook_type prev_final_cost_hashjoin_hook = NULL;
 
 /* --- Logic Helpers --- */
 
 /*
  * get_io_tax
  * Calculates the "Bounce Buffer Tax" based on the number of pages touched.
  * In TEE, every page IO involves a memcpy via SWIOTLB and decryption latency.
  */
 static double
 get_io_tax(double pages)
 {
     if (pages <= 0) return 0.0;
     
     /* * Formula: Pages * Cost_Per_Page * Overhead_Pct
      * We assume the base sequential page cost is roughly 1.0 (seq_page_cost).
      * The overhead (e.g. 0.15) is applied on top of that base cost.
      */
     return pages * 1.0 * tee_io_overhead_pct;
 }

static bool
is_cache_resident_pages(double pages)
{
    double kb;

    if (pages <= 0)
        return false;

    kb = (pages * BLCKSZ) / 1024.0;
    return (kb < (double) tee_safe_cache_kb);
}

static bool
is_small_workload(double rows, int width)
{
    double kb;

    if (rows <= 0 || width <= 0)
        return false;

    kb = (rows * width) / 1024.0;
    return (kb < (double) tee_safe_cache_kb);
}
 
 /* --- Hook Implementations --- */
 
 /* 1. Seq Scan: Bounce Buffer Tax (Corrected usage) */
 static void
 tee_cost_seqscan(Path *path, PlannerInfo *root,
                  RelOptInfo *baserel, ParamPathInfo *param_info)
 {
     if (prev_cost_seqscan_hook)
         prev_cost_seqscan_hook(path, root, baserel, param_info);
     else
         standard_cost_seqscan(path, root, baserel, param_info);
 
     if (!tee_enable_cost_model) return;
 
    if (is_cache_resident_pages(baserel->pages)) return;

     /* * FIX: Use get_io_tax instead of simple multiplication.
      * Use baserel->pages as the most accurate count of physical pages to read.
      */
     path->total_cost += get_io_tax(baserel->pages);
 }
 
 /* 2. Index Scan: RMP Tax (Tree Descent) + Random IO Tax */
 static void
 tee_cost_index(IndexPath *path, PlannerInfo *root,
                double loop_count, bool partial_path)
 {
     double tree_height;
     double rmp_penalty;
     double estimated_pages;
 
     if (prev_cost_index_hook)
         prev_cost_index_hook(path, root, loop_count, partial_path);
     else
         standard_cost_index(path, root, loop_count, partial_path);
 
     if (!tee_enable_cost_model) return;
 
    if (is_cache_resident_pages(path->path.parent->pages)) return;

     if (path->indexinfo->pages > 0)
     {
         /* A. RMP Penalty for pointer chasing (Tree Height) */
         tree_height = (path->indexinfo->pages > 1) ? 
                       (log(path->indexinfo->pages) / log(300.0)) : 1.0;
         
         /* 0.005 is roughly 2x cpu_tuple_cost, applied per level */
         rmp_penalty = path->path.rows * tree_height * 0.005; 
         path->path.total_cost += rmp_penalty;
         
         /* * B. IO Penalty (Random Access via Bounce Buffer)
          * Estimate pages touched: Selectivity * Total Index Pages
          * (This assumes random scattered reads)
          */
         estimated_pages = path->indexselectivity * path->indexinfo->pages;
         path->path.total_cost += get_io_tax(estimated_pages);
     }
 }
 
 /* 3. Bitmap Heap Scan: IO Tax (Restored from NULL) */
 static void
 tee_cost_bitmap_heap_scan(Path *path, PlannerInfo *root,
                           RelOptInfo *baserel, ParamPathInfo *param_info,
                           Path *bitmapqual, double loop_count)
 {
     double estimated_heap_pages;
 
     if (prev_cost_bitmap_heap_scan_hook)
         prev_cost_bitmap_heap_scan_hook(path, root, baserel, param_info, bitmapqual, loop_count);
     else
         standard_cost_bitmap_heap_scan(path, root, baserel, param_info, bitmapqual, loop_count);
 
     if (!tee_enable_cost_model) return;
    if (is_cache_resident_pages(baserel->pages)) return;
 
     /* * Bitmap Scan reads heap pages. The number of pages is roughly selectivity * table_pages.
      * TPC-DS reports +17% overhead here, CEB +11%.
      * We apply the standard IO tax to the estimated pages.
      */
     estimated_heap_pages = baserel->pages * path->rows / baserel->tuples; 
     path->total_cost += get_io_tax(estimated_heap_pages);
 }
 
 /* 4. Sort: Encrypted Memory Shuffle Tax */
 static void
 tee_cost_sort(Path *path, PlannerInfo *root,
               List *pathkeys, Cost input_cost, double tuples, int width,
               Cost comparison_cost, int sort_mem,
               double limit_tuples)
 {
     double processing_cost;
 
     if (prev_cost_sort_hook)
         prev_cost_sort_hook(path, root, pathkeys, input_cost, tuples, width,
                             comparison_cost, sort_mem, limit_tuples);
     else
         standard_cost_sort(path, root, pathkeys, input_cost, tuples, width,
                            comparison_cost, sort_mem, limit_tuples);
 
     if (!tee_enable_cost_model) return;
 
    if (is_small_workload(tuples, width)) return;

     /* CPU overhead for sorting encrypted data */
     processing_cost = path->total_cost - input_cost;
     path->total_cost += (processing_cost * tee_cpu_overhead_pct);
 }
 
 /* 5. Materialize: Write/Read IO Tax (Refined) */
 static void
 tee_cost_material(Path *path, Cost input_startup_cost, Cost input_total_cost,
                   double tuples, int width)
 {
     double size_bytes;
     double pages;
 
     if (prev_cost_material_hook)
         prev_cost_material_hook(path, input_startup_cost, input_total_cost, tuples, width);
     else
         standard_cost_material(path, input_startup_cost, input_total_cost, tuples, width);
 
     if (!tee_enable_cost_model) return;
    
    /*
     * Materialize involves writing to memory (or disk if spilling).
     * Calculate approx pages: rows * width / 8KB.
     */
    size_bytes = tuples * width;
    if (is_small_workload(tuples, width))
        return;

    pages = size_bytes / BLCKSZ;

    /* Lighten the tax so small spools stay attractive; apply quarter-rate. */
    path->total_cost += get_io_tax(pages) * 0.25;
 }
 
 /* 5b. Agg: Encrypted Aggregation CPU Tax */
 static void
 tee_cost_agg(Path *path, PlannerInfo *root,
              AggStrategy aggstrategy, const AggClauseCosts *aggcosts,
              int numGroupCols, double numGroups,
              List *quals,
              Cost input_startup_cost, Cost input_total_cost,
              double input_tuples, double input_width)
 {
     double processing_cost;
 
     if (prev_cost_agg_hook)
         prev_cost_agg_hook(path, root, aggstrategy, aggcosts, numGroupCols, numGroups,
                            quals, input_startup_cost, input_total_cost,
                            input_tuples, input_width);
     else
         standard_cost_agg(path, root, aggstrategy, aggcosts, numGroupCols, numGroups,
                           quals, input_startup_cost, input_total_cost,
                           input_tuples, input_width);
 
     if (!tee_enable_cost_model) return;
    if (is_small_workload(input_tuples, input_width)) return;
 
     /* Apply ~20% CPU tax to aggregation work (excluding upstream input cost). */
     processing_cost = path->total_cost - input_total_cost;
     if (processing_cost < 0)
         processing_cost = 0;
 
     path->total_cost += (processing_cost * tee_cpu_overhead_pct);
 }
 
 /* 6. WindowAgg: Complex CPU Tax */
 static void
 tee_cost_windowagg(Path *path, PlannerInfo *root,
                    List *windowFuncs, int numPartCols, int numOrderCols,
                    Cost input_startup_cost, Cost input_total_cost,
                    double input_tuples)
 {
     double overhead;
 
     if (prev_cost_windowagg_hook)
         prev_cost_windowagg_hook(path, root, windowFuncs, numPartCols, numOrderCols,
                                  input_startup_cost, input_total_cost, input_tuples);
     else
         standard_cost_windowagg(path, root, windowFuncs, numPartCols, numOrderCols,
                                 input_startup_cost, input_total_cost, input_tuples);
 
     if (!tee_enable_cost_model) return;
    if (is_small_workload(input_tuples, path->pathtarget->width)) return;
 
    /* TPC-DS showed ~13% overhead. Use a softer 6% to avoid over-penalizing. */
    overhead = 0.06;
     path->total_cost *= (1.0 + overhead);
 }
 
 /* 7. Memoize: Cache Maintenance Tax */
 static void
 tee_cost_memoize_rescan(PlannerInfo *root, MemoizePath *mpath,
                         Cost *rescan_startup_cost, Cost *rescan_total_cost)
 {
    double entry_penalty = 0.0;
    double entries = 0.0;

     if (prev_cost_memoize_rescan_hook)
         prev_cost_memoize_rescan_hook(root, mpath, rescan_startup_cost, rescan_total_cost);
     else
         standard_cost_memoize_rescan(root, mpath, rescan_startup_cost, rescan_total_cost);
 
     if (!tee_enable_cost_model) return;
 
   /* Penalize large caches to avoid overusing Memoize on wide/rare keys. */
   entries = mpath->est_entries;
   if (entries > 0)
   {
       double ratio = entries / 2000.0;
       if (ratio > 2.0)
           ratio = 2.0; /* cap */
       entry_penalty = 0.08 * ratio;
   }

   /* Moderate penalty; skip heavy hits on small caches. */
   if (entries > 0 && entries < 500.0)
       return;

   *rescan_startup_cost *= (1.0 + tee_memoize_overhead_pct * 0.40 + entry_penalty);
   *rescan_total_cost   *= (1.0 + tee_memoize_overhead_pct + entry_penalty);
 }
 
 /* 8. Gather / Gather Merge: Inter-Core Communication Penalty */
 static void
 apply_gather_tax(Path *path, int num_workers)
 {
    double comms_penalty = tee_gather_overhead_pct;
    double scaled;
    double row_adj = 1.0;

    /*
     * Apply a gentler, worker-aware penalty so parallel plans are not
     * over-discouraged. We scale up slightly only when more workers are used.
     */
    if (num_workers > 4)
        scaled = comms_penalty * 1.10;
    else if (num_workers > 2)
        scaled = comms_penalty * 1.05;
    else
        scaled = comms_penalty;

    /* Discourage Gather on tiny result sets where overhead dominates. */
    if (path->rows < 1000.0)
        row_adj = 1.20;
    else if (path->rows < 10000.0)
        row_adj = 1.08;

    scaled *= row_adj;

    /* Favor keeping parallelism: modest weight on startup and total. */
    path->startup_cost *= (1.0 + scaled * 0.25);
    path->total_cost   *= (1.0 + scaled * 0.10);
 }
 
 static void
 tee_cost_gather(GatherPath *path, PlannerInfo *root,
                 RelOptInfo *rel, ParamPathInfo *param_info, double *rows)
 {
     if (prev_cost_gather_hook)
         prev_cost_gather_hook(path, root, rel, param_info, rows);
     else
         standard_cost_gather(path, root, rel, param_info, rows);
 
     if (tee_enable_cost_model)
         apply_gather_tax(&path->path, path->num_workers);
 }
 
 static void
 tee_cost_gather_merge(GatherMergePath *path, PlannerInfo *root,
                       RelOptInfo *rel, ParamPathInfo *param_info,
                       Cost input_startup_cost, Cost input_total_cost, double *rows)
 {
     if (prev_cost_gather_merge_hook)
         prev_cost_gather_merge_hook(path, root, rel, param_info, input_startup_cost, input_total_cost, rows);
     else
         standard_cost_gather_merge(path, root, rel, param_info, input_startup_cost, input_total_cost, rows);
 
     if (tee_enable_cost_model)
         apply_gather_tax(&path->path, path->num_workers);
 }
 
 /* 9. Merge Join: Pipeline Stall Penalty */
 static void
 tee_final_cost_mergejoin(PlannerInfo *root, MergePath *path,
                          JoinCostWorkspace *workspace,
                          JoinPathExtraData *extra)
 {
    double overhead = 0.10; /* Base softer */
    Path *outer;
    Path *inner;
 
     if (prev_final_cost_mergejoin_hook)
         prev_final_cost_mergejoin_hook(root, path, workspace, extra);
     else
         standard_final_cost_mergejoin(root, path, workspace, extra);
 
     if (!tee_enable_cost_model) return;
 
     /* If input relies on IndexScan, stall penalty */
     outer = path->jpath.outerjoinpath;
     inner = path->jpath.innerjoinpath;
 
    if (IsA(outer, IndexPath) || IsA(inner, IndexPath)) {
        overhead += 0.04; 
    }
 
     path->jpath.path.total_cost *= (1.0 + overhead);
 }
 
 /* 10. Hash Join: L3 Cache Spill Logic */
 static void
 tee_final_cost_hashjoin(PlannerInfo *root, HashPath *path,
                         JoinCostWorkspace *workspace,
                         JoinPathExtraData *extra)
 {
     double inner_rows;
     int    inner_width;
     double hash_table_size_kb;
 
     if (prev_final_cost_hashjoin_hook)
         prev_final_cost_hashjoin_hook(root, path, workspace, extra);
     else
         standard_final_cost_hashjoin(root, path, workspace, extra);
 
     if (!tee_enable_cost_model) return;
 
     inner_rows = path->jpath.innerjoinpath->rows;
     inner_width = path->jpath.innerjoinpath->pathtarget->width;
     hash_table_size_kb = (inner_rows * (inner_width + 16)) / 1024.0;
 
    if (hash_table_size_kb <= (double)tee_safe_cache_kb)
    {
        /* Very small: treat as cache-resident, no extra tax */
        return;
    }

    if (hash_table_size_kb > (double)tee_l3_cache_kb)
    {
        /* SPILL L3 CACHE */
        double spill_ratio;
        double penalty_factor;

        spill_ratio = hash_table_size_kb / (double)tee_l3_cache_kb;
        if (spill_ratio > 2.5) spill_ratio = 2.5;

        /* Softer ramp to avoid over-penalizing hash join */
        penalty_factor = 1.0 + (0.05 * (spill_ratio - 1.0));
        path->jpath.path.total_cost *= penalty_factor;
    }
    else
    {
        /* Fits in cache */
        path->jpath.path.total_cost *= 1.02;
    }
 }
 
 /* 11. Nested Loop: Random Access Amplification */
 static void
 tee_final_cost_nestloop(PlannerInfo *root, NestPath *path,
                         JoinCostWorkspace *workspace,
                         JoinPathExtraData *extra)
 {
    double penalty_mult = 1.02; 
     Path *inner_path;
 
     if (prev_final_cost_nestloop_hook)
         prev_final_cost_nestloop_hook(root, path, workspace, extra);
     else
         standard_final_cost_nestloop(root, path, workspace, extra);
 
     if (!tee_enable_cost_model) return;
 
     inner_path = path->jpath.innerjoinpath;
 
    /* Inner Index Scan is sensitive but often best for selective lookups. */
    if (IsA(inner_path, IndexPath) || inner_path->pathtype == T_IndexOnlyScan)
    {
        penalty_mult = 1.06; 
        if (path->jpath.outerjoinpath->rows > 1000.0)
            penalty_mult = 1.12; 
    }
 
     path->jpath.path.total_cost *= penalty_mult;
 }
 
 /* --- Init/Fini --- */
 
 void
 _PG_init(void)
 {
     DefineCustomBoolVariable("tee_cost_model.enable", "Enable TEE cost model.", NULL, &tee_enable_cost_model, true, PGC_USERSET, 0, NULL, NULL, NULL);
 
    DefineCustomRealVariable("tee_cost_model.io_overhead_pct", "Overhead for IO.", NULL, &tee_io_overhead_pct, 0.08, 0.0, 5.0, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomRealVariable("tee_cost_model.cpu_overhead_pct", "Overhead for CPU.", NULL, &tee_cpu_overhead_pct, 0.06, 0.0, 5.0, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomRealVariable("tee_cost_model.gather_overhead_pct", "Overhead for Gather.", NULL, &tee_gather_overhead_pct, 0.10, 0.0, 10.0, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomRealVariable("tee_cost_model.memoize_overhead_pct", "Overhead for Memoize.", NULL, &tee_memoize_overhead_pct, 0.12, 0.0, 5.0, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("tee_cost_model.l3_cache_kb", "L3 Cache size (KB).", NULL, &tee_l3_cache_kb, 32768, 1024, 1024*1024, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("tee_cost_model.safe_cache_kb", "Size threshold for disabling TEE tax.", NULL, &tee_safe_cache_kb, 16384, 0, 1024*1024, PGC_USERSET, 0, NULL, NULL, NULL);
 
     /* Register Hooks */
     prev_cost_seqscan_hook = cost_seqscan_hook;
     cost_seqscan_hook = tee_cost_seqscan;
 
     prev_cost_index_hook = cost_index_hook;
     cost_index_hook = tee_cost_index;
 
     prev_cost_bitmap_heap_scan_hook = cost_bitmap_heap_scan_hook;
     cost_bitmap_heap_scan_hook = tee_cost_bitmap_heap_scan;
 
     prev_cost_sort_hook = cost_sort_hook;
     cost_sort_hook = tee_cost_sort;
 
     prev_cost_agg_hook = cost_agg_hook;
     cost_agg_hook = tee_cost_agg;
 
     prev_cost_windowagg_hook = cost_windowagg_hook;
     cost_windowagg_hook = tee_cost_windowagg;
 
     prev_cost_material_hook = cost_material_hook;
     cost_material_hook = tee_cost_material;
 
     prev_cost_memoize_rescan_hook = cost_memoize_rescan_hook;
     cost_memoize_rescan_hook = tee_cost_memoize_rescan;
 
     prev_cost_gather_hook = cost_gather_hook;
     cost_gather_hook = tee_cost_gather;
 
     prev_cost_gather_merge_hook = cost_gather_merge_hook;
     cost_gather_merge_hook = tee_cost_gather_merge;
 
     prev_final_cost_nestloop_hook = final_cost_nestloop_hook;
     final_cost_nestloop_hook = tee_final_cost_nestloop;
 
     prev_final_cost_mergejoin_hook = final_cost_mergejoin_hook;
     final_cost_mergejoin_hook = tee_final_cost_mergejoin;
 
     prev_final_cost_hashjoin_hook = final_cost_hashjoin_hook;
     final_cost_hashjoin_hook = tee_final_cost_hashjoin;
 }
 
 void
 _PG_fini(void)
 {
     cost_seqscan_hook = prev_cost_seqscan_hook;
     cost_index_hook = prev_cost_index_hook;
     cost_bitmap_heap_scan_hook = prev_cost_bitmap_heap_scan_hook;
     cost_sort_hook = prev_cost_sort_hook;
     cost_agg_hook = prev_cost_agg_hook;
     cost_windowagg_hook = prev_cost_windowagg_hook;
     cost_material_hook = prev_cost_material_hook;
     cost_memoize_rescan_hook = prev_cost_memoize_rescan_hook;
     cost_gather_hook = prev_cost_gather_hook;
     cost_gather_merge_hook = prev_cost_gather_merge_hook;
     final_cost_nestloop_hook = prev_final_cost_nestloop_hook;
     final_cost_mergejoin_hook = prev_final_cost_mergejoin_hook;
     final_cost_hashjoin_hook = prev_final_cost_hashjoin_hook;
 }
 
 Datum
 tee_cost_model_activate(PG_FUNCTION_ARGS)
 {
     PG_RETURN_BOOL(true);
 }