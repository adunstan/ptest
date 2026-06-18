# contrib/columnstore/Makefile

MODULE_big = columnstore
OBJS = \
	$(WIN32RES) \
	columnstore.o \
	cs_compact.o \
	cs_customscan.o \
	cs_insert.o \
	cs_reloptions.o \
	cs_scan.o \
	cs_slot.o \
	cs_storage.o \
	cs_vacuum.o

EXTENSION = columnstore
DATA = columnstore--1.0.sql
PGFILEDESC = "columnstore - columnar storage table access method"

REGRESS = columnstore \
	cs_encoding \
	cs_dictionary \
	cs_zonemap \
	cs_delta_columnar \
	cs_delete_update \
	cs_scan_paths \
	cs_compaction \
	cs_pushdown \
	cs_agg_pushdown \
	cs_index \
	cs_misc \
	cs_sort_key \
	cs_planner

ISOLATION = \
	cs_vacuum_inflight \
	cs_index_vacuum_window \
	cs_delete_visibility \
	cs_select_for_update \
	cs_vacuum_concurrent \
	cs_vacuum_snapshot \
	cs_on_conflict_rr \
	cs_index_visibility \
	cs_unique_dirty \
	cs_lock_for_update \
	cs_fk_locking

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/columnstore
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
