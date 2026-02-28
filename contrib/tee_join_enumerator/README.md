TEE Join Enumerator
===================

This extension installs a `join_search_hook` that injects TEE (e.g., AMD SEV-SNP)
aware heuristics into PostgreSQL join enumeration. It applies two layers:

- Dynamic programming enumeration: prune or defer join pairs that would create
  very large intermediate results or combine two IO-heavy subtrees. A small
  shadow list is rescued if pruning becomes too aggressive.
- GEQO enumeration: replaces the cost fitness function with one that adds IO and
  RMP-style penalties and discounts highly selective “martyr” candidates so they
  are not lost.

Build & install (inside the source tree):

```
cd contrib/tee_join_enumerator
make
make install
```

Create the extension (this LOADs and activates the hook):

```
CREATE EXTENSION tee_join_enumerator;
```

Alternatively load it manually for a session or globally:

```
SET session_preload_libraries = 'tee_join_enumerator';
# or
shared_preload_libraries = 'tee_join_enumerator'
```

Optional GUCs:

- `tee_join_enumerator.jn_enabled` (bool): toggle the join enumerator hook.
- `tee_join_enumerator.jn_rmp_soft_mb` / `jn_rmp_hard_mb`: intermediate volume caps.
- `tee_join_enumerator.jn_io_pages_threshold`: IO-heavy relation threshold.
- `tee_join_enumerator.jn_rescue_floor`: minimum joins before reviving deferred pairs.
- `tee_join_enumerator.jn_geqo_io_weight` / `jn_geqo_rmp_weight`: GEQO penalty weights.
- `tee_join_enumerator.jn_geqo_rescue_discount` / `jn_geqo_rescue_rows`: GEQO martyr rescue.

The SQL script is intentionally empty; the extension activates via the shared
library load.
