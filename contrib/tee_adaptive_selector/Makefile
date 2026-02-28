# tee_adaptive_selector extension
MODULE_big = tee_adaptive_selector
OBJS = tee_adaptive_selector.o
PGFILEDESC = "tee_adaptive_selector - Adaptive TEE optimizer orchestrator"

EXTENSION = tee_adaptive_selector
DATA = tee_adaptive_selector--1.0.sql

# We rely on planner hooks; this module must be built inside the server tree.
ifndef PG_CONFIG
subdir = contrib/tee_adaptive_selector
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif
