EXTENSION = tee_cardinality_estimation
MODULE_big = tee_cardinality_estimation
OBJS = tee_cardinality_estimation.o

PGFILEDESC = "TEE-aware cardinality heuristics"

DATA = tee_cardinality_estimation--1.0.sql
REGRESS =

EXTRA_CLEAN = tee_cardinality_estimation--unpackaged--1.0.sql

# Build logic: support both in-tree and PGXS builds
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/tee_cardinality_estimation
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
