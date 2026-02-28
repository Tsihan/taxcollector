# tee_join_enumerator extension
MODULE_big = tee_join_enumerator
OBJS = tee_join_enumerator.o
PGFILEDESC = "tee_join_enumerator - TEE-aware join search heuristics"

EXTENSION = tee_join_enumerator
DATA = tee_join_enumerator--1.0.sql

# We rely on planner hooks; this module must be built inside the server tree.
ifndef PG_CONFIG
subdir = contrib/tee_join_enumerator
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif
