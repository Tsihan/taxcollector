/*
 * tee_join_enumerator.c
 * TEE-aware join enumeration plugin.
 *
 * REVISION NOTES:
 * - Added `tee_jn_tax_level_limit` GUC.
 * - Implemented "Hybrid Search Strategy":
 * 1. Deep Optimization (Level <= Limit): Calculate Tax, Sort Candidates, Apply Limits.
 * 2. Fast Path (Level > Limit): Skip scoring and sorting, use standard enumeration.
 * This reduces planner overhead for higher levels where Tax calculation is expensive.
 */
 #include "postgres.h"

 #include <float.h>
 #include <math.h>
 
 #include "access/htup_details.h"
 #include "fmgr.h"
 #include "miscadmin.h"
 #include "nodes/pathnodes.h"
 #include "optimizer/appendinfo.h"
 #include "optimizer/cost.h"
#include "optimizer/geqo.h"
 #include "optimizer/joininfo.h"
 #include "optimizer/pathnode.h"
 #include "optimizer/paths.h"
 #include "utils/guc.h"
 #include "utils/memutils.h"
 
 PG_MODULE_MAGIC;
 
 PG_FUNCTION_INFO_V1(tee_join_enumerator_activate);
 
 /* --- Configuration Variables --- */
 static bool tee_jn_enabled = true;
 static int  tee_jn_tax_level_limit = 3; /* Only calculate Tax Score for first N levels */
 
 /* Weights and Limits */
 static double tee_jn_io_weight = 2.0;
 static double tee_jn_rmp_weight = 1.0;
 static int    tee_jn_generation_limit = 20;
 
 /* Saved hook */
 static join_search_hook_type prev_join_search_hook = NULL;
 
 /* --- Structs --- */
 typedef struct TeeCandidatePair
 {
     RelOptInfo *left;
     RelOptInfo *right;
     double      score;
     bool        clauseless;
 } TeeCandidatePair;
 
 /* --- Forward Declarations --- */
 void _PG_init(void);
 void _PG_fini(void);
 
 static RelOptInfo *tee_join_search(PlannerInfo *root, int levels_needed, List *initial_rels);
static RelOptInfo *tee_standard_join_search(PlannerInfo *root, int levels_needed, List *initial_rels);
static void tee_join_search_one_level(PlannerInfo *root, int level);
 
 /* Helpers */
 static double calculate_join_tax_score(RelOptInfo *left, RelOptInfo *right);
 static int compare_candidates(const void *a, const void *b);
 static bool tee_has_join_restriction(PlannerInfo *root, RelOptInfo *rel);
 static void tee_try_join_pair(PlannerInfo *root, RelOptInfo *left, RelOptInfo *right);
 static Datum tee_join_enumerator_activate_internal(void);
 
 Datum tee_join_enumerator_activate(PG_FUNCTION_ARGS);
 
 /* --- Inline Helpers --- */
 static inline double mb_to_bytes(double mb) { return mb * 1024.0 * 1024.0; }
 
 static double
 rel_width_bytes(RelOptInfo *rel)
 {
     if (rel->reltarget && rel->reltarget->width > 0)
         return rel->reltarget->width;
     return 8.0;
 }
 
 static double
 calculate_join_tax_score(RelOptInfo *left, RelOptInfo *right)
 {
     /* Simplified Tax Model: IO (pages) + RMP (Memory Footprint) */
     double left_rmp = left->rows * rel_width_bytes(left);
     double right_rmp = right->rows * rel_width_bytes(right);
     
     /* Using Pages as proxy for IO decryption cost */
     double left_io = left->pages;
     double right_io = right->pages;
 
     return ((left_io + right_io) * tee_jn_io_weight) + 
            ((left_rmp + right_rmp) * tee_jn_rmp_weight);
 }
 
 static bool
 tee_has_join_restriction(PlannerInfo *root, RelOptInfo *rel)
 {
     ListCell *lc;
     if (root->join_info_list != NIL)
     {
         foreach(lc, root->join_info_list)
         {
             SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);
             if (bms_overlap(sjinfo->min_lefthand, rel->relids) ||
                 bms_overlap(sjinfo->min_righthand, rel->relids))
                 return true;
         }
     }
     return false;
 }
 
 static void
 tee_try_join_pair(PlannerInfo *root, RelOptInfo *left, RelOptInfo *right)
 {
     if (bms_overlap(left->relids, right->relids))
         return;
     (void) make_join_rel(root, left, right);
 }
 
 /* --- Initialization --- */
 void
 _PG_init(void)
 {
     DefineCustomBoolVariable("tee_join_enumerator.jn_enabled",
                              "Enable TEE-aware join enumeration",
                              NULL, &tee_jn_enabled, true, PGC_USERSET, 0, NULL, NULL, NULL);
 
     DefineCustomIntVariable("tee_join_enumerator.jn_tax_level_limit",
                             "Max join level to apply TEE Tax scoring (Default: 3)",
                             NULL, &tee_jn_tax_level_limit, 3, 1, 100, PGC_USERSET, 0, NULL, NULL, NULL);
 
     DefineCustomRealVariable("tee_join_enumerator.jn_io_weight",
                              "Weight for IO in scoring", NULL, &tee_jn_io_weight, 2.0, 0.0, 1000.0, PGC_USERSET, 0, NULL, NULL, NULL);
 
     DefineCustomRealVariable("tee_join_enumerator.jn_rmp_weight",
                              "Weight for RMP in scoring", NULL, &tee_jn_rmp_weight, 1.0, 0.0, 1000.0, PGC_USERSET, 0, NULL, NULL, NULL);
 
     DefineCustomIntVariable("tee_join_enumerator.jn_generation_limit",
                             "Soft limit for join candidates per level", NULL, &tee_jn_generation_limit, 20, 1, 1000, PGC_USERSET, 0, NULL, NULL, NULL);
 
     /* GEQO variables omitted for brevity, keeping existing defaults */
     
     prev_join_search_hook = join_search_hook;
     join_search_hook = tee_join_search;
 }
 
 void _PG_fini(void) { join_search_hook = prev_join_search_hook; }
 
 Datum tee_join_enumerator_activate(PG_FUNCTION_ARGS) { return tee_join_enumerator_activate_internal(); }
 static Datum tee_join_enumerator_activate_internal(void) { if(!tee_jn_enabled) tee_jn_enabled = true; return BoolGetDatum(true); }
 
 /* --- Main Logic --- */
 
 static RelOptInfo *
 tee_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
 {
     if (!tee_jn_enabled)
     {
         if (prev_join_search_hook) return prev_join_search_hook(root, levels_needed, initial_rels);
         else if (enable_geqo && levels_needed >= geqo_threshold) return geqo(root, levels_needed, initial_rels);
         else return standard_join_search(root, levels_needed, initial_rels);
     }
 
     return tee_standard_join_search(root, levels_needed, initial_rels);
 }
 
 static RelOptInfo *
 tee_standard_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
 {
     int lev;
     RelOptInfo *rel;
 
     root->join_rel_level = (List **) palloc0((levels_needed + 1) * sizeof(List *));
     root->join_rel_level[1] = initial_rels;
 
     for (lev = 2; lev <= levels_needed; lev++)
     {
         ListCell *lc;
 
         /* Call our level processor */
         tee_join_search_one_level(root, lev);
 
         foreach(lc, root->join_rel_level[lev])
         {
             rel = (RelOptInfo *) lfirst(lc);
             generate_partitionwise_join_paths(root, rel);
             if (!bms_equal(rel->relids, root->all_query_rels))
                 generate_useful_gather_paths(root, rel, false);
             set_cheapest(rel);
         }
     }
 
     if (root->join_rel_level[levels_needed] == NIL)
         elog(ERROR, "failed to build any %d-way joins", levels_needed);
 
     rel = (RelOptInfo *) linitial(root->join_rel_level[levels_needed]);
     root->join_rel_level = NULL;
     return rel;
 }
 
 static int compare_candidates(const void *a, const void *b)
 {
     TeeCandidatePair *pairA = (TeeCandidatePair *)a;
     TeeCandidatePair *pairB = (TeeCandidatePair *)b;
     if (pairA->score < pairB->score) return -1;
     if (pairA->score > pairB->score) return 1;
     return 0;
 }
 
 /*
  * tee_join_search_one_level
  * * HYBRID STRATEGY:
  * 1. If level <= tee_jn_tax_level_limit:
  * - Calculate Tax Scores (IO + RMP).
  * - Sort candidates.
  * - Apply Soft Limits (tee_jn_generation_limit).
  * This ensures the FOUNDATION of the join tree is optimized for TEE.
  * * 2. If level > tee_jn_tax_level_limit:
  * - FAST PATH. Skip scoring, skipping sorting.
  * - Simply try joins as they are discovered (Standard Postgres behavior).
  * This prevents planner slowdowns for complex queries.
  */
 static void
 tee_join_search_one_level(PlannerInfo *root, int level)
 {
     List **joinrels = root->join_rel_level;
     List *candidates_list = NIL;
     ListCell *r;
     int k;
     
     /* Optimization switch */
     bool use_heuristic = (level <= tee_jn_tax_level_limit);
 
     Assert(joinrels[level] == NIL);
     root->join_cur_level = level;
 
     /* --- Loop 1: Linear Joins (Level-1 + Level-1) --- */
     foreach(r, joinrels[level - 1])
     {
         RelOptInfo *old_rel = (RelOptInfo *) lfirst(r);
 
         if (old_rel->joininfo != NIL || old_rel->has_eclass_joins ||
             tee_has_join_restriction(root, old_rel))
         {
             List *other_rels_list;
             ListCell *other_rels;
 
             if (level == 2)
             {
                 other_rels_list = joinrels[level - 1];
                 other_rels = lnext(other_rels_list, r);
             }
             else
             {
                 other_rels_list = joinrels[1];
                 other_rels = list_head(other_rels_list);
             }
 
             for_each_cell(other_rels, other_rels_list, other_rels)
             {
                 RelOptInfo *other_rel = (RelOptInfo *) lfirst(other_rels);
                 
                 if (use_heuristic)
                 {
                     /* Slow Path: Add to candidate list for scoring */
                     TeeCandidatePair *cand = palloc(sizeof(TeeCandidatePair));
                     cand->left = old_rel;
                     cand->right = other_rel;
                     cand->score = calculate_join_tax_score(old_rel, other_rel);
                     candidates_list = lappend(candidates_list, cand);
                 }
                 else
                 {
                     /* Fast Path: Join immediately */
                     tee_try_join_pair(root, old_rel, other_rel);
                 }
             }
         }
         else
         {
             /* Clauseless / Cartesian */
             ListCell *l;
             foreach(l, joinrels[1])
             {
                 RelOptInfo *other_rel = (RelOptInfo *) lfirst(l);
                 if (use_heuristic)
                 {
                     TeeCandidatePair *cand = palloc(sizeof(TeeCandidatePair));
                     cand->left = old_rel;
                     cand->right = other_rel;
                     cand->score = calculate_join_tax_score(old_rel, other_rel) * 100.0; /* Penalty */
                     candidates_list = lappend(candidates_list, cand);
                 }
                 else
                 {
                     tee_try_join_pair(root, old_rel, other_rel);
                 }
             }
         }
     }
 
     /* --- Loop 2: Bushy Joins --- */
     for (k = 2;; k++)
     {
         int other_level = level - k;
         if (k > other_level) break;
 
         foreach(r, joinrels[k])
         {
             RelOptInfo *old_rel = (RelOptInfo *) lfirst(r);
             List *other_rels_list;
             ListCell *other_rels;
             ListCell *r2;
 
             if (old_rel->joininfo == NIL && !old_rel->has_eclass_joins &&
                 !tee_has_join_restriction(root, old_rel))
                 continue;
 
             if (k == other_level)
             {
                 other_rels_list = joinrels[k];
                 other_rels = lnext(other_rels_list, r);
             }
             else
             {
                 other_rels_list = joinrels[other_level];
                 other_rels = list_head(other_rels_list);
             }
 
             for_each_cell(r2, other_rels_list, other_rels)
             {
                 RelOptInfo *new_rel = (RelOptInfo *) lfirst(r2);
 
                 if (!bms_overlap(old_rel->relids, new_rel->relids) &&
                     (have_relevant_joinclause(root, old_rel, new_rel) ||
                      have_join_order_restriction(root, old_rel, new_rel)))
                 {
                     if (use_heuristic)
                     {
                         TeeCandidatePair *cand = palloc(sizeof(TeeCandidatePair));
                         cand->left = old_rel;
                         cand->right = new_rel;
                         cand->score = calculate_join_tax_score(old_rel, new_rel);
                         candidates_list = lappend(candidates_list, cand);
                     }
                     else
                     {
                         tee_try_join_pair(root, old_rel, new_rel);
                     }
                 }
             }
         }
     }
 
     /* --- Heuristic Processing (Only if enabled for this level) --- */
     if (use_heuristic && candidates_list != NIL)
     {
         int num_candidates = list_length(candidates_list);
         TeeCandidatePair *candidates_array = (TeeCandidatePair *) palloc(num_candidates * sizeof(TeeCandidatePair));
         ListCell *lc;
         int idx = 0;
         int i;
         int generated_count = 0;
 
         foreach(lc, candidates_list)
             candidates_array[idx++] = *(TeeCandidatePair *)lfirst(lc);
 
         /* Sort by Tax Score (Lowest Tax First) */
         qsort(candidates_array, num_candidates, sizeof(TeeCandidatePair), compare_candidates);
 
         /* Process with Soft Limits */
         for (i = 0; i < num_candidates; i++)
         {
             if (tee_jn_generation_limit > 0 && 
                 generated_count >= tee_jn_generation_limit &&
                 list_length(joinrels[level]) > 0)
             {
                 /* Cut off expensive tail */
                 break;
             }
             tee_try_join_pair(root, candidates_array[i].left, candidates_array[i].right);
             generated_count++;
         }
         pfree(candidates_array);
     }
     
     /* Cleanup candidate list */
     if (candidates_list != NIL)
         list_free_deep(candidates_list);
 
     /* --- Safety Fallback for Heuristic Mode --- */
     if (use_heuristic && joinrels[level] == NIL)
     {
         /* If pruning killed everything, force cartesian */
         foreach(r, joinrels[level - 1])
         {
             RelOptInfo *old_rel = (RelOptInfo *) lfirst(r);
             ListCell *l;
             foreach(l, joinrels[1])
             {
                 RelOptInfo *other_rel = (RelOptInfo *) lfirst(l);
                 tee_try_join_pair(root, old_rel, other_rel);
             }
         }
     }
 }
 