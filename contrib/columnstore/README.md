# Columnstore Table Access Method

A columnar storage engine for PostgreSQL, implemented as a built-in table
access method using the Table AM API. It stores data column-by-column in
compressed row groups, enabling efficient analytical queries on wide tables
while maintaining transactional semantics through snapshot isolation (with
the documented divergences listed under **Limitations**: SERIALIZABLE
executes with REPEATABLE READ semantics, and updated rows have no version
chains for READ COMMITTED lock waits to follow).

## Why Columnar Storage?

PostgreSQL's default heap storage format writes all columns of a row together
on a single page. This is optimal for OLTP workloads that read or update
entire rows, but it becomes a bottleneck for analytical queries that scan
millions of rows but reference only a few columns.

The columnstore AM addresses this with three complementary techniques:

- **Column pruning.** Only the columns referenced by a query are read from
  disk and decompressed. A `SELECT a, b FROM wide_table` on a 50-column
  table avoids all I/O for the other 48 columns.

- **Compression.** Homogeneous data within a single column compresses far
  better than mixed-type row data. The columnstore applies dictionary
  encoding to low-cardinality columns, then LZ4 (with a PGLZ fallback) to
  each column chunk, typically achieving 2-10x compression ratios on
  real-world data.

- **Zone maps.** Each row group records per-column min/max values. Scan
  predicates like `WHERE ts > '2025-01-01'` skip entire row groups whose
  value ranges cannot satisfy the filter, without reading any data pages.

## Architecture

```
┌──────────────────────────────────────────────────┐
│                   Executor                       │
│   (uses CSTupleTableSlot with lazy decompression)│
├──────────────────────────────────────────────────┤
│             TableAmRoutine callbacks             │
│          (columnstore_tableam_handler)           │
├───────────────────┬──────────────────────────────┤
│   Delta Store     │      Columnar Row Groups     │
│  (heap-format     │   (per-column compressed     │
│   pages for       │    chunks with zone maps,    │
│   recent writes)  │    compacted by VACUUM)      │
├───────────────────┴──────────────────────────────┤
│    Generic WAL (crash safety via page diffs)     │
├──────────────────────────────────────────────────┤
│    PostgreSQL shared buffers / SMgr / pages      │
└──────────────────────────────────────────────────┘
```

All data lives in a single relation fork composed of standard 8 KB pages.
Inserts land in the **delta store** (heap-format pages at the end of the
file) for low-latency writes. VACUUM compacts the delta store into
**columnar row groups** — per-column compressed chunks with zone maps —
enabling efficient analytical reads. Scans read both regions transparently.

## Storage Layout

Page 0 is the **metapage**, which records the relation schema, the number of
compacted row groups, the location and size of the delta store, and the location
of the row group directory.

```
Page 0             Metapage (CSMetaPageData in special space)
Pages 1..N         Row group catalog + column data + deletion bitmap pages
Pages R..R+J       Row group directory pages (BlockNumber[nrowgroups])
Pages F..F+L       Free range list pages
Pages D..D+K       Delta store (heap-format pages, including tombstones)
```

### Metapage

```c
typedef struct CSMetaPageData
{
    uint32      cs_magic;           /* 0x434F4C53 = "COLS"              */
    uint16      cs_version;         /* format version (currently 1)     */
    uint16      cs_natts;           /* number of columns                */
    uint32      cs_nrowgroups;      /* number of completed row groups   */
    BlockNumber cs_delta_start;     /* first delta store page           */
    BlockNumber cs_delta_nblocks;   /* number of delta store blocks     */
    uint64      cs_total_rows;      /* estimate of total live rows      */
    BlockNumber cs_rgdir_start;     /* first directory page             */
    uint32      cs_rgdir_npages;    /* number of directory pages        */
    BlockNumber cs_freelist_start;  /* first free range list page       */
    uint32      cs_freelist_npages; /* pages storing free ranges        */
    uint32      cs_freelist_nranges;/* number of valid free ranges      */
    BlockNumber cs_data_pages;      /* column data + delta pages (planner) */
    uint32      cs_flags;           /* CS_META_PENDING_REINDEX etc.     */
} CSMetaPageData;
```

The metapage is stored in the **pd_special area** of page 0 (via
`PageInit(page, BLCKSZ, sizeof(CSMetaPageData))`).  Storing it there
ensures the data survives GenericXLog's treatment of the free-space region
between `pd_lower` and `pd_upper` — GenericXLog preserves the special
area.  Initialization creates both the metapage and the first delta page
atomically in a single GenericXLog state under an extension lock, with a
double-check to avoid races from concurrent inserters.

### Row Group Directory

The row group directory is a flat array of `BlockNumber` values — one entry
per row group — that maps a row group ID to the block number of its catalog
page.  The array is serialized across one or more standard 8 KB pages using
the same page format as column data pages.  Each page can hold
`CS_RGDIR_ENTRIES_PER_PAGE` (~2042) entries, so directory overhead is
negligible (one page per ~2042 row groups, i.e., ~200 million rows).

During VACUUM/compaction, a new directory is written containing both old and
new entries, and the metapage is atomically updated to point to it.  Old
directory pages become dead space.

### Row Group Catalog

Each row group has a catalog page containing a `CSRowGroupDesc`:

```c
typedef struct CSRowGroupDesc
{
    uint32  rg_id;
    uint32  rg_num_rows;
    uint32  rg_num_deleted;
    BlockNumber rg_delbitmap_block;  /* InvalidBlockNumber if no deletions */
    int16   rg_natts;
    CSColumnChunkDesc rg_columns[FLEXIBLE_ARRAY_MEMBER];
    /* followed by CSZoneMap[rg_natts] (accessed via CSRowGroupGetZoneMaps) */
} CSRowGroupDesc;
```

Each per-column `CSColumnChunkDesc` records the start block, page count,
compressed and uncompressed size, compression method, and null metadata.
Zone maps (`CSZoneMap`) follow the column array and store per-column
min/max Datum values with a `zm_has_minmax` validity flag.

The total size is `CSRowGroupDescSize(natts)`.  For tables with ~200+
columns this exceeds 8 KB, so the catalog is serialized across multiple
pages using `cs_write_column_data` / `cs_read_rowgroup_catalog` (the same
multi-page spanning logic used for column data).

### Column Data Pages

Compressed column data is stored sequentially across one or more 8 KB
pages, with `CS_COLDATA_PER_PAGE` usable bytes per page (= `BLCKSZ -
MAXALIGN(SizeOfPageHeaderData)`).  Each page is initialized with
`PageInit` and `pd_lower` is advanced past the data so GenericXLog's copy
logic preserves the content (it only copies `0..pd_lower` and
`pd_special..end`).

### Deletion Bitmap

Each row group may have a dedicated deletion bitmap page, allocated on
first delete.  The bitmap is a packed bit array (1 bit per row, bit set =
deleted) stored in the page content area.  The catalog's
`rg_delbitmap_block` points to it; `InvalidBlockNumber` means no deletions.
Scans and ANALYZE check this bitmap to skip deleted rows.

### Delta Store

The delta store uses standard PostgreSQL heap-format pages with
`PageHeaderData`, `ItemIdData` line pointers, and full `HeapTupleHeader`
data including xmin/xmax for MVCC.  This reuses PostgreSQL's existing page
and tuple infrastructure with no modifications.

### Virtual TID Encoding

Delta store rows have real physical TIDs (block number, offset within page).
Columnar rows use **virtual TIDs** so the system can decode any TID back to
a (row_group_id, row_offset) pair:

```
block  = CS_COLUMNAR_BLKNO_BASE + rg_id * blocks_per_rg + row / rows_per_block
offset = (row % rows_per_block) + 1
```

`CS_COLUMNAR_BLKNO_BASE` is `0x40000000`, high enough that no real delta
block number will collide with it. Any block number at or above this
threshold is a virtual columnar TID.

## Data Flow

### INSERT

All inserts go directly into the delta store — no columnar data is ever
written during INSERT.

1. If the relation has zero blocks, `cs_init_metapage` atomically creates
   the metapage (block 0) and the first delta page (block 1) in a single
   GenericXLog state under an extension lock.
2. A `HeapTuple` is formed from the slot with xmin set to the current
   transaction, xmax set to `InvalidTransactionId`, and
   `HEAP_XMAX_INVALID` set.
3. If the table has a TOAST table, `heap_toast_insert_or_update` compresses
   and/or moves oversized varlena attributes out-of-line so the tuple fits
   in a standard 8KB delta page.  A heap-AM TOAST relation is created
   automatically by `cs_relation_needs_toast_table` whenever the relation
   has any toastable attribute and the maximum tuple length could exceed
   `TOAST_TUPLE_THRESHOLD` — the same rule heap uses.  The standard
   `toast_tuple_target` reloption is honoured.
4. Only the **last delta page** is tried — earlier pages are assumed full,
   so there is no free-space map or backwards scan.  A page that a
   running compaction has *fenced* (see Delta Compaction) is treated as
   full.  If the last page has insufficient space, a new page is
   allocated with `ExtendBufferedRel` (which takes the relation
   extension lock itself; `ReadBufferExtended(P_NEW)` must not be used
   here because it assumes the caller already holds that lock, and
   concurrent inserters would collide on the same new block).  Every
   delta page carries a special-area opaque (`CSDeltaPageOpaque`) that
   identifies it as delta content and holds the fence flag.
5. The metapage is updated once, by **merging** into the current on-disk
   values under one exclusive buffer lock (`cs_metapage_merge_insert`):
   the delta range is widened to cover the new page and the row estimate
   is incremented.  Insert paths never write back a privately held copy
   of the whole metapage — between read and write another inserter may
   have extended the delta or VACUUM may have advanced it, and a
   full-struct overwrite would silently revert that.  Because relation
   extension is shared with VACUUM's column-data writes, foreign pages
   can interleave into the nominal delta range; `cs_delta_nblocks` is
   therefore a range *length*, and all readers identify true delta pages
   by the page opaque, skipping everything else.

Multi-insert (`cs_multi_insert`) batches tuples: all tuples landing on the
same page share a single GenericXLog WAL record, reducing WAL volume from
one record per tuple to one record per page.  COPY drives this directly
via `table_multi_insert` (its own buffering decides batch size).  There
is no table-AM hook the AM can use to ask the executor to batch
`INSERT ... SELECT`, so that path goes through per-row `tuple_insert`
today.

### VACUUM (Compaction)

VACUUM performs four operations: **delta compaction**, **row group
compaction** (with merging), **file truncation**, and **statistics
reporting**.

#### Phase 1: Delta Compaction

`cs_compact_delta` converts the row-oriented delta store into compressed
columnar row groups.

It begins with `cs_materialize_tombstones`: the delta store is scanned
for tombstone tuples (zero-attribute heap tuples whose `t_ctid` points
into virtual-TID space), and for each tombstone whose xmin is committed
**and precedes the visibility horizon**
(`GetOldestNonRemovableTransactionId`) the corresponding row's deletion
bit is set in its target row group's bitmap and `rg_num_deleted` is
incremented.  The horizon gate is what keeps the (snapshot-blind)
deletion bitmap honest: a committed delete that some live snapshot must
not yet see stays behind as a tombstone and keeps answering visibility
per snapshot.  Aborted tombstones, duplicates already materialized in
earlier passes, and spent lock-only tombstones (row locks whose
transaction has ended; see **Tuple Locking**) are marked `LP_DEAD`.
Materialization happens before the data scan so the main batching loop
never has to interpret tombstones as data rows.

Delta rows are then processed in **batches** of `CS_ROWS_PER_ROWGROUP`
(100,000) to bound peak memory.  Two memory contexts are used:
`compact_ctx` (lives across all batches, accumulates catalog block numbers)
and `batch_ctx` (reset per batch, holds per-column arrays and buffers).

1. **Scan batch.** Scan delta pages from the current resume position.
   Each tuple is classified against the visibility horizon
   (`cs_classify_delta_tuple`): **movable** (committed before the
   horizon, hence visible to every present and future snapshot; any
   xmax is from a row lock or an aborted/ancient transaction), **dead**
   (aborted insert, or a delete visible to all), or **retained**
   (in-flight, not yet visible to all, locked by a live transaction, or
   an unmaterialized tombstone).  The delta is consumed strictly from
   the front in whole pages: a page may only be consumed when every
   tuple on it is movable or dead, and the first page holding a
   retained tuple ends the consumable prefix — it and everything after
   it stay in the delta untouched, with TIDs intact.  Before a clean
   page's rows are collected the page is **fenced** (a flag in the page
   opaque, set under the same exclusive lock as the cleanliness check):
   an insert targeting the page from a stale metapage view sees the
   fence and extends instead, and a DELETE or row lock aimed at a
   fenced page waits on a page-0 heavyweight lock the compaction holds
   from the first fence until the metapage flip, then re-checks whether
   its row moved.  Movable rows are collected, up to
   `CS_ROWS_PER_ROWGROUP`, into `col_values[natts][nrows]` and
   `col_nulls[natts][nrows]` arrays.  By-reference datums are deep-copied
   into `batch_ctx` because `heap_deform_tuple` returns pointers directly
   into the buffer page, which become dangling after `UnlockReleaseBuffer`.
   Varlena datums are detoasted (`detoast_attr`) to fetch any out-of-line
   TOAST data before copying.

2. **Sort (optional).**  If the table has a `sort_key` storage parameter
   (e.g., `WITH (sort_key = 'ts')`), the batch is sorted by those columns
   before serialization.  The sort uses `qsort_arg` on a permutation index
   array with btree comparison functions, then applies the permutation to
   all column arrays.  This improves delta+FOR compression (sorted
   timestamps produce constant deltas that pack in ~0 bits) and zone map
   quality (non-overlapping ranges across row groups).

3. **Write row group.** For each column, serialize into the on-disk column
   format (see **Column Serialization Format** below), compress, write to
   new pages, and compute zone maps.  Write the row group catalog entry.

4. **Free and repeat.** Reset `batch_ctx` and return to step 1.  Only the
   catalog block numbers (one `BlockNumber` per row group) accumulate in
   `compact_ctx`.

5. **Directory and flip.** After all batches, build the full row group
   directory (old entries + new), write it to new pages, and do a single
   atomic metapage update that registers all new row groups, points to
   the new directory, advances `cs_delta_start` past the consumed prefix
   (preserving retained pages and anything concurrent inserts appended),
   and sets `CS_META_PENDING_REINDEX` — from this instant until the
   index rebuild completes, indexes reference pre-move TIDs.  Only the
   actual delta pages of the consumed prefix go to the free list; any
   interleaved foreign pages belong to live data and are skipped by a
   page-identity re-check.

6. **TOAST cleanup.** If the table has a TOAST table, scan the now-consumed
   delta pages and call `heap_toast_delete` for every tuple that has
   external toast references.  This runs after the metapage commit point,
   so a crash between steps 5 and 6 only leaks orphaned toast chunks
   (harmless, cleaned by a subsequent VACUUM on the TOAST table).

The metapage update that increments `cs_nrowgroups` is the commit point.
If the server crashes before this, orphaned column and directory pages are
harmless and the delta store is intact.

After TOAST cleanup the indexes are rebuilt (`reindex_relation`) and
`CS_META_PENDING_REINDEX` is cleared.  If the rebuild is cancelled —
autovacuum is routinely auto-cancelled by conflicting DDL — the flag
survives, and the next VACUUM repairs the indexes
(`cs_repair_pending_reindex`) *before* anything can reclaim pages.
Index fetches stay correct throughout: an index TID below the current
delta range resolves through the consumed page, whose contents remain
intact until reuse, and reuse requires `AccessExclusiveLock`, which is
only taken after indexes are rebuilt.

VACUUM then **freezes** what stays behind in the delta
(`cs_freeze_delta`): tuples dead to everyone get `LP_DEAD` line
pointers, tuples visible to everyone get `HEAP_XMIN_FROZEN` and any
ended-transaction xmax cleared, and everything else holds only xids at
or above the horizon by construction.  This is what lets VACUUM report
the horizon as the new `relfrozenxid` (columnar rows carry no
transaction information at all, and no multixacts are ever stored, so
`relminmxid` advances to the current value) — without it, a
columnstore table would pin `datfrozenxid` at its creation xid forever
and eventually stall the whole cluster at the wraparound limit.

#### Phase 2: Row Group Compaction

When the GUC `columnstore.rowgroup_compaction_threshold` is set to a
non-negative value (0.0–1.0), `cs_compact_rowgroups` rewrites row groups
whose dead-row fraction meets or exceeds the threshold.  The default is
-1 (disabled).

Compaction maintains a **free page range list** stored in dedicated pages
and referenced from the metapage (`cs_freelist_start`, `cs_freelist_npages`,
`cs_freelist_nranges`).  Before rewriting a qualifying row group, all of
its old pages (catalog, column data, deletion bitmap) are added to the
free list.  New row groups are then allocated from these freed pages first,
falling back to extending the relation only when no suitable free range
exists.  This prevents the relation file from growing during repeated
compaction cycles.

For each qualifying row group:

1. **Load.** Load the row group catalog entry and all column data via
   `cs_load_rowgroup_into` and `cs_ensure_column_loaded`.

2. **Filter.** Extract surviving rows (those not marked in the deletion
   bitmap) into new column arrays.  The helper `cs_cache_get_value`
   handles all storage formats: direct-access fixed-length, variable-length,
   and dictionary-encoded columns.

3. **Accumulate and merge.**  Surviving rows are accumulated into a merge
   buffer.  When the buffer reaches `CS_ROWS_PER_ROWGROUP`, it is flushed
   as a single merged row group.  This means multiple partially-deleted
   row groups can be consolidated into fewer, fuller ones.  Undersized row
   groups (those below `CS_ROWS_PER_ROWGROUP`) adjacent to qualifying
   groups are also pulled into the merge.  Fully-deleted row groups (all
   rows deleted) are dropped from the directory entirely.

4. **Directory rebuild.** Write a new directory excluding dropped row
   groups, and atomically update the metapage.

5. **Index rebuild.** The metapage write that publishes the rewritten
   directory also sets `CS_META_PENDING_REINDEX`; `reindex_relation`
   then rebuilds the indexes with the new virtual TIDs and the flag is
   cleared.  If the rebuild is interrupted, the flag survives and the
   next VACUUM performs the rebuild before reclaiming any pages, exactly
   as in delta compaction.  (Row-group compaction additionally runs
   under a conditional `AccessExclusiveLock`, so no concurrent scan can
   observe the renumbered TIDs in the interim.)

#### Phase 3: File Truncation

`cs_truncate_trailing_freespace` scans the free list for ranges that
reach the end of the relation file.  If found, the range is removed from
the free list, the metapage is persisted (crash safety: a slightly
oversized file is harmless, but a truncated file with stale free-list
entries pointing past EOF is not), and `RelationTruncate` shrinks the
file.  The loop repeats until no trailing free range remains.

#### Phase 4: Statistics Reporting

VACUUM computes live-row count, dead-row count, and data-page count
(column data + delta pages, excluding metadata overhead) in a single
pass over the row group catalog via `cs_count_live_stats`.  It then:

- Updates `cs_total_rows` and `cs_data_pages` on the metapage so
  `cs_relation_estimate_size` returns accurate values without additional
  I/O.  Only the stats fields are written (`cs_metapage_update_stats`); a
  full metapage write here would clobber a delta extension a concurrent
  insert merged in after VACUUM re-read the metapage.
- Calls `vac_update_relstats` to update `pg_class.reltuples` and
  `relpages`, and to report the freeze horizon as the new
  `relfrozenxid` / `relminmxid` (see the freezing pass above).  The
  data-page count (not `RelationGetNumberOfBlocks`) is used for
  `relpages`, giving the planner an accurate I/O cost estimate.
- Calls `pgstat_report_vacuum` so that `pg_stat_user_tables` shows
  `last_vacuum`, `n_live_tup`, and `n_dead_tup`.
- Reports progress via `pgstat_progress_start/end_command` for
  `pg_stat_progress_vacuum`.

### SELECT (Sequential Scan)

The scan reads both storage regions in order:

1. **Delta store** (`cs_delta_getnext`).  Uses **page-mode** scanning:
   lock the page SHARE, iterate all offsets checking MVCC visibility, and
   collect visible offsets into a prescan array.  Then release the content
   lock (keeping the pin) and return tuples one at a time from the array.

   Visibility checks run on a **copy** of the tuple header, not the
   buffer page itself.  This prevents `SetHintBits` from modifying the
   buffer, which would cause WAL consistency-check failures with
   GenericXLog (which expects the buffer "before" image to match disk).

2. **Columnar row groups** (`cs_columnar_getnext`).  For each row group:
   - **Zone map check** first — read the catalog and call
     `cs_zonemap_skip_rowgroup()`.  If the zone map proves no rows can
     match, skip the entire group without reading any column data.
   - **Load row group** — reads the catalog and deletion bitmap but does
     NOT decompress columns (deferred to `getsomeattrs`).
   - **Skip deleted rows** via deletion bitmap and tombstone list (see
     **MVCC** below).
   - **Set up lazy slot** — store a back-pointer to the scan descriptor,
     column cache, and row index in the `CSTupleTableSlot`.  Set
     `tts_nvalid = 0` so columns are decompressed on demand.
   - On the first row of each row group, memset all slot values/isnull to
     NULL/true.  Since `ExecClearTuple` never zeros these arrays, the
     NULLs for unneeded columns persist across rows.

   **COUNT(\*) fast path** (`cs_columnar_getnext_countonly`): when the
   `count_optimized` flag is set (no columns needed), only the catalog
   and deletion bitmap are read — no column buffers are allocated at all.
   The function iterates per-row to check deletion bitmaps and
   tombstones, producing valid virtual TIDs for each live row (required
   by `DELETE FROM` which needs the TID even when no columns are
   projected).  The slot is marked non-empty with all attributes NULL so
   the executor can safely materialize it.

   **Selection bitmap.**  Before the per-row loop, the scan builds a
   `uint8` selection bitmap (1 bit per row) that records which rows
   satisfy all pushed-down scan keys.  For each scan key on a
   dictionary-encoded or FOR-encoded column, the bitmap is filled by a
   tight comparison loop over the decoded column array — auto-vectorized
   by the compiler — instead of a per-row qual-evaluation function call.
   The per-row loop then consults the bitmap with a single array
   lookup (`CS_ISSELECTED`) and skips deselected rows without
   re-evaluating any predicate.

   **Aggregate pushdown.**  For `SUM` / `AVG` over numeric columns
   stored with NUMERIC_INT64 encoding, the executor uses
   `scan_column_encoding` to discover the physical type (int64 +
   dscale) and `scan_get_raw_attr` to extract raw int64 values
   straight from the column cache, accumulating into an int128 sum
   and converting back to numeric only at finalization.  This bypasses
   the per-row int64-to-numeric `palloc` and multi-word arithmetic
   that standard numeric aggregation would incur.  Pushdown is
   suppressed for parallel partial aggregation, where the transition
   state would have to round-trip through the standard
   `NumericAggState` serialization that the pushdown's
   `AggPushdownAccum` does not provide.

   **Late materialization for bounded sorts.**  There is no executor
   hint for this — the columnstore registers a separate upper-path
   candidate at `UPPERREL_ORDERED` via `create_upper_paths_hook`
   (`cs_try_add_latemat_path` in `cs_customscan.c`), which emits a
   `Custom Scan (ColumnstoreLateMat)`.  The custom node scans the
   table with only the sort and filter columns projected, pushes
   "narrow" tuples carrying a packed TID into a bounded tuplesort
   (top-N), then for each of the LIMIT survivors refetches the
   remaining columns by TID via `table_tuple_fetch_row_version`.
   The planner picks this path whenever its cost beats the standard
   `Sort` over `SeqScan` plan.

### DELETE

- **Delta rows:** Mark the tuple's xmax with the current transaction ID via
  GenericXLog. Clear `HEAP_XMAX_INVALID` so MVCC visibility checks treat
  the row as deleted.

- **Columnar rows:** Two-step process for MVCC correctness.  First, a
  **tombstone** is inserted into the delta store — a minimal heap tuple
  with zero user attributes (`HeapTupleHeaderSetNatts(0)`) whose `t_ctid`
  points to the target columnar virtual TID.  The tombstone carries full
  xmin/xmax for MVCC visibility: other transactions that started before
  the delete committed still see the row.  Second, the deletion bitmap
  bit is set and `rg_num_deleted` is incremented in the row group catalog
  for the benefit of compaction and zone map skip logic.

  Tombstones are identified by the `CS_IS_TOMBSTONE()` macro (zero
  attributes + virtual TID in `t_ctid`).  During sequential and index
  scans, tombstones are collected lazily on first columnar access
  (`cs_collect_tombstones`) and checked per-row alongside the deletion
  bitmap.

Both paths perform conflict detection before modifying the row (see
**Tuple Locking** below).

### UPDATE

Implemented as DELETE of the old row followed by INSERT of the new row into
the delta store. The old row's TID is decoded and deleted (via the
appropriate delta or columnar path), and a fresh tuple is inserted into the
delta store. The executor is told to update all indexes (`TU_All`).

## Tuple Locking

`SELECT FOR UPDATE/SHARE/NO KEY UPDATE/KEY SHARE` and DML conflict
detection use a hybrid of PostgreSQL's heavyweight lock manager
(`LOCKTAG_TUPLE`) and durable per-row lock records, mirroring heap's
structure but without MultiXacts:

- The **heavyweight tuple lock is transient**: it serializes the
  check-and-record sequence (the lock manager forbids holding tuple
  locks until transaction end) and provides wait queues, deadlock
  detection, and `SKIP LOCKED` / `NOWAIT` behavior.
- What **persists** is the record it protected: a delta row gets a
  **lock-only xmax** stamped (`HEAP_XMAX_LOCK_ONLY` plus mode bits,
  `t_cid` left alone so a row inserted by the same transaction keeps its
  cmin for cursor visibility); a columnar row — physically immutable —
  gets a **lock-only tombstone** in the delta carrying the locker's xid
  and mode.  Writers find the record and wait on the locker's xid via
  `XactLockTableWait`, so the lock naturally expires with the locking
  transaction.
- The lock record also pins the row in place: compaction's classifier
  treats a row with a live lock as **retained**, so VACUUM cannot move a
  locked row to columnar (which would detach it from the TID the lock
  names).

With a single xmax and no MultiXact support, a delta row records one
live locker at a time: a second locker — even of a compatible mode such
as a concurrent `FOR KEY SHARE` — waits for the first to finish.
Columnar rows do not have this restriction (each locker inserts its own
lock tombstone, and the heavyweight conflict matrix arbitrates modes),
so concurrent key-share locks on compacted rows proceed in parallel.

### Lock mode mapping

`LockTupleMode` values are mapped to heavyweight `LOCKMODE` via a static
array (`cs_tupleLockMode`), mirroring the mapping in heapam.c:

| LockTupleMode           | LOCKMODE              |
|-------------------------|-----------------------|
| LockTupleKeyShare       | AccessShareLock (1)   |
| LockTupleShare          | RowShareLock (2)      |
| LockTupleNoKeyExclusive | ExclusiveLock (7)     |
| LockTupleExclusive      | AccessExclusiveLock (8) |

DELETE and UPDATE acquire `AccessExclusiveLock` (same strength as
`FOR UPDATE`), which conflicts with all other lock modes. The standard
lock compatibility matrix handles the interactions between share and
exclusive modes.

### Lock tag

`LockTuple` uses `(dbId, relId, blockNumber, offset)` as the lock tag.
This works for both physical TIDs (delta rows) and virtual TIDs (columnar
rows) because the block/offset values are unique identifiers regardless of
whether they correspond to real pages.

### cs_tuple_lock (SELECT FOR UPDATE/SHARE)

Acquires the heavyweight tuple lock per `wait_policy` (block,
skip → `TM_WouldBlock`, or error → "could not obtain lock on row"),
then checks the row's current state and records the lock:

**Delta path** (`blkno < CS_COLUMNAR_BLKNO_BASE`): the page must still
be a delta page and unfenced (a fence means a compaction is moving it;
wait on the page-0 lock and re-check — if the row moved, report
`TM_Deleted`).  `HeapTupleSatisfiesUpdate` classifies the tuple: on
`TM_Ok` a lock-only xmax with the requested mode is stamped via
GenericXLog and the slot is filled; `TM_BeingModified` waits on the
xmax holder and retries; a committed deleter yields `TM_Deleted`.

**Columnar path** (`blkno >= CS_COLUMNAR_BLKNO_BASE`): after deletion
bitmap and tombstone checks (`TM_Deleted` for a materialized or
committed delete, `TM_SelfModified` for our own pending delete, wait
and retry for an in-progress one), conflicting live lock tombstones
from other transactions are waited out, and a lock-only tombstone
recording our xid and mode is inserted.

In both paths the slot is filled via `cs_fetch_row_version` with
`SnapshotAny`: visibility against the caller's snapshot is the
executor's business (e.g. `ON CONFLICT DO UPDATE` locks rows its
snapshot cannot see and then raises the serialization error itself),
so the lock layer reports the current version, exactly as
`heap_lock_tuple` does.  The slot's `getsysattr` /
`is_current_xact_tuple` callbacks serve `xmin` and friends (re-read by
TID for delta rows; columnar rows behave as frozen), which
`ExecOnConflictUpdate`'s self-insertion check requires.

The `TUPLE_LOCK_FLAG_FIND_LAST_VERSION` flag is a no-op because the
columnstore has no update chains — UPDATE is always DELETE + INSERT
with no ctid link between the old and new rows.  A committed deleter
is therefore always reported as `TM_Deleted` (never a followable
`TM_Updated`), and `tmfd->traversed` is always false.  Under READ
COMMITTED this means a `FOR UPDATE` that waited out a concurrent
UPDATE sees the row as *deleted* and skips it, where heap would chase
the update chain and lock the new version — a documented divergence
(see **Limitations**).

### Conflict detection in DELETE

**Delta delete** (`cs_delta_delete`):
1. Read page under exclusive buffer lock and check xmax.
2. `SelfModified` → return with `tmfd` filled (including the combo
   cmax); a committed deleter → `TM_Deleted` (no update chains).  A
   lock-only xmax is not a delete: our own or an ended transaction's is
   simply overwritten, a live one is waited out on its xid.
3. In-progress by another transaction → release buffer, acquire a
   heavyweight `AccessExclusiveLock` on the TID to wait (with deadlock
   detection), release it, and loop back to re-check.
4. Once the row is confirmed live, acquire the heavyweight lock,
   re-check under buffer lock (including the compaction fence: a fenced
   page is being moved, so wait on the page-0 lock and re-evaluate),
   and set xmax via GenericXLog.

**Columnar delete** (`cs_columnar_delete`):
1. Check deletion bitmap — if already set, return `TM_Deleted`.
2. Acquire heavyweight `AccessExclusiveLock` on the virtual TID.
3. Re-check the deletion bitmap and tombstones after lock — a committed
   delete returns `TM_Deleted`, our own returns `TM_SelfModified`, an
   in-progress one (or a live row lock from another transaction) is
   waited out on its xid.
4. Insert the delete tombstone.

Both paths honor the `wait` parameter: when false, they use
`ConditionalLockTuple` and return `TM_WouldBlock` on failure.

## Column Pruning

The columnstore uses a custom `TupleTableSlotOps` (`TTSOpsColumnStore`)
whose `getsomeattrs` callback implements lazy column decompression.

### Projection Setup

During plan initialization, `cs_scan_set_projection` receives a 0-indexed
`Bitmapset` of needed columns (derived from the parent plan node's
references, not the scan's own targetlist which may be a physical tlist
containing all columns).  It builds a flat sorted `needed_col_list[]`
array for O(needed) iteration and, if no columns are needed, sets the
`count_optimized` flag for the COUNT(\*) fast path.

### Lazy Decompression (`tts_columnstore_getsomeattrs`)

When the executor calls `slot_getsomeattrs(slot, N)`:

1. **Delta rows**: all attributes were filled eagerly during deform, so
   just set `tts_nvalid = natts`.
2. **Columnar rows with projection**: iterate only `needed_col_list`,
   calling `cs_ensure_column_loaded` for each column not yet loaded:
   - **Dictionary-encoded** (`col_dict[col] != NULL`): the value is
     resolved as `dict_values[index_data[row]]` — a single array lookup
     with no per-row decompression.
   - **Fixed-length by-value** (`col_values[col] == NULL` sentinel):
     values are computed on the fly via `fetch_att(raw + row * attlen)`.
     The raw decompressed buffer is kept in `col_raw_data[col]`.  If the
     column has nulls, a bitmap prefix is consulted.
   - **Variable-length/by-reference** (`col_values[col] != NULL`): the
     length-prefixed stream was deserialized into a `Datum` array on
     first access.  Values point directly into the decompressed buffer
     (zero-copy).
   Unneeded columns were pre-set to NULL at row-group start and persist
   because `ExecClearTuple` does not zero the arrays.
3. **Columnar rows without projection**: loads columns sequentially from
   `tts_nvalid` to `natts`.

Column buffers are cached per row group in the `CSColumnCache`, so each
column is decompressed at most once per row group.

### Point-Read Optimization

When a column has been accessed fewer than `CS_POINT_READ_THRESHOLD` (64)
times within a row group, `cs_column_point_read` materializes individual
values on demand without decompressing the entire column.  This benefits
index scans and bounded sorts (e.g., `ORDER BY ... LIMIT N`) that touch
only a sparse subset of rows in each row group.  Once the threshold is
exceeded, the full column is decompressed and cached as usual.

Point reads are supported only for FOR and NUMERIC_INT64 encoded columns
(which allow O(1) random access into the bit-packed or flat int64 array).
Delta-encoded columns require a prefix-sum integration from row 0, and
RLE-encoded columns require scanning run headers, so both fall through
to full column decompression immediately.

### Materialization

`tts_columnstore_materialize` ensures all columns are loaded, allocates a
single contiguous buffer, deep-copies all by-reference datums into it, and
detaches the slot from the scan descriptor so it becomes self-contained.

## Compression

Column chunks are compressed during compaction using a speculative
try-all-pick-best approach.  The compaction code tries every applicable
pre-encoding, then selects the one that produces the smallest output
before applying block compression on top.

### Pipeline

1. **NUMERIC_INT64** (optional).  Numeric columns where every non-null
   value has the same display scale and fits in `int64` are converted to
   scaled fixed-point integers.  A 2-byte `dscale` header is prepended
   so the decode path can reconstruct `numeric` values.

2. **Pre-encoding candidates** (tried speculatively, smallest wins):
   - **FOR** (frame-of-reference + bit-packing) for fixed-width integer
     types.  Stores `value - min_value` using the minimum number of bits
     to cover the range.  Requires 20% savings.
   - **Delta** encoding for sorted/monotonic integer columns.  Stores
     `value[i] - value[i-1]`, reducing the range so that a subsequent
     FOR bit-packing is far more effective.
   - **Delta+FOR** — delta first, then FOR on the delta array.  The
     primary win for timeseries: monotonic timestamps with near-constant
     intervals compress to ~1 bit per value.
   - **Dictionary** encoding for low-cardinality columns (≤65,535
     distinct values, fewer than `nrows / 4`).  See **Dictionary
     Encoding** below.
   - **RLE** (run-length encoding) for columns with long runs of
     repeated values.  Requires the number of runs to be at most
     `nrows / 4`.
   - **Gorilla** — delta-of-delta for integers, XOR for floats.
     Variable-length codes adapt per-value, avoiding the worst-case
     penalty of fixed-width encodings.  See **Gorilla Encoding** below.

   NUMERIC_INT64 can stack with FOR, Delta, or Delta+FOR.  Dictionary,
   RLE, and Gorilla are mutually exclusive with all other pre-encodings.

3. **Block compression.**  The pre-encoded (or raw) byte stream is then
   compressed:
   - Try **LZ4** first (fast compression/decompression, good ratios on
     columnar data).
   - If LZ4 is unavailable or produces output larger than the input, try
     **PGLZ** (PostgreSQL's built-in LZ compression).
   - If neither produces a benefit, store uncompressed.

### `cc_compression` byte layout

The `cc_compression` field in each `CSColumnChunkDesc` is a `uint8`
that splits into two nibbles.  The low nibble is the base block
compression method; the high nibble is a **flat enum** identifying the
pre-encoding scheme (it is *not* a set of independent bit flags — the
bit pattern is decorative).  Use `CS_COMPRESS_BASE(c)` and
`CS_PREENC(c)` to extract the two halves.

```
Low nibble (bits 0-3): base compression method
  0x00 = CS_COMPRESS_NONE
  0x01 = CS_COMPRESS_PGLZ
  0x02 = CS_COMPRESS_LZ4

High nibble (bits 4-7): pre-encoding scheme ID
  0x00 = CS_PREENC_NONE
  0x10 = CS_PREENC_DICT             (dictionary)
  0x20 = CS_PREENC_FOR              (frame-of-reference + bit-packing)
  0x30 = CS_PREENC_GORILLA          (DoD for ints, XOR for floats)
  0x40 = CS_PREENC_NI64             (numeric-as-int64)
  0x50 = CS_PREENC_NI64_GORILLA     (reserved; encoder currently does
                                     not stack Gorilla on top of NI64)
  0x60 = CS_PREENC_NI64_FOR
  0x80 = CS_PREENC_DELTA
  0x90 = CS_PREENC_RLE
  0xA0 = CS_PREENC_DELTA_FOR
  0xC0 = CS_PREENC_NI64_DELTA
  0xE0 = CS_PREENC_NI64_DELTA_FOR
```

Backward-compatibility aliases (`CS_COMPRESS_DICT`, `CS_COMPRESS_FOR`,
`CS_COMPRESS_NUMERIC_INT64`, `CS_COMPRESS_DELTA`, `CS_COMPRESS_RLE`,
`CS_COMPRESS_GORILLA`) are defined as the corresponding `CS_PREENC_*`
values; the compaction code combines them with bitwise OR while
building a stacked encoding (e.g., NI64 | DELTA | FOR), and the result
must match one of the enumerated constants above.  The decode path
extracts the high nibble with `CS_PREENC(c)` and dispatches via
`switch` over those values rather than testing individual bits.

Both compressed and uncompressed sizes are stored so decompression is a
single call to `LZ4_decompress_safe` or `pglz_decompress` into a
pre-allocated buffer.

### Delta Encoding

`cs_try_delta_encode` computes successive differences for fixed-width
by-value integer types (int2, int4, int8).  The first non-null value is
stored as its absolute value; subsequent values store `value[i] -
value[i-1]`.  A heuristic rejects delta encoding when the delta range
is not smaller than the absolute value range.

On its own, delta encoding does not reduce the serialized size (same
number of integers).  The benefit comes from combining with FOR: if the
original values span a wide range but the deltas are near-constant, FOR
can bit-pack the deltas in far fewer bits.

On-disk format (before block compression):
```
[null_bitmap: (nrows+7)/8 bytes, if has_nulls]
[base_value: attlen bytes]        — first non-null absolute value
[delta array: attlen × nrows]     — per-row differences
```

Decode: base-decompress, then if DELTA+FOR the FOR decode produces the
delta array which is integrated via prefix sum; if DELTA alone the delta
array is read and integrated directly.

### RLE Encoding

`cs_try_rle_encode` replaces consecutive identical values with
(value, run_length) pairs.  Supports both by-value fixed-length types
and variable-length types (text, bytea, etc.).

On-disk format for by-value types (before block compression):
```
[null_bitmap: (nrows+7)/8 bytes, if has_nulls]
[4 bytes: num_runs (uint32)]
[per run: {value: attlen bytes} {count: uint32}] × num_runs
```

For variable-length types, a `has_null` flag byte follows `num_runs`
and each run's value is stored as `{int32 len} {len bytes of data}`,
using the same format as the non-encoded column serialization (the
data includes the varlena header).  NULL runs store only a count.

### Gorilla Encoding

`cs_try_gorilla_encode` implements two variable-length encoding schemes
from the Facebook Gorilla paper (Pelkonen et al., 2015):

**Delta-of-delta (DoD)** for integer types (int2/int4/int8).  Instead of
storing `delta[i] = value[i] - value[i-1]`, stores
`DoD[i] = delta[i] - delta[i-1]`.  For timestamps with near-constant
intervals, DoD is 0 most of the time.  Variable-length prefix codes
adapt per-value:
- DoD = 0: 1 bit
- DoD in [-63, 64]: 9 bits
- DoD in [-511, 512]: 13 bits
- DoD in [-4095, 4096]: 17 bits
- Otherwise: 4-bit prefix + full raw value (byte-aligned)

Unlike delta+FOR which sizes all values to the worst-case delta, Gorilla
DoD pays only for the individual outlier.  A single 5-second gap among
99,999 rows of 1ms intervals costs 68 bits for the outlier and 1 bit
for everything else.

**XOR** for float types (float4/float8).  Each value is XORed with the
previous one.  For slowly-changing sensor readings, the XOR has long
runs of zero bits.  Variable-length prefix codes:
- XOR = 0: 1 bit (same as previous value)
- Meaningful bits fit in prior window: 2 + N bits
- New window: 14 + N bits

Gorilla encoding is not stacked with NI64 (numeric columns use
delta+FOR instead).

On-disk format:
```
[null_bitmap: (nrows+7)/8 bytes, if has_nulls]
[1 byte: type_tag (0 = DoD, 1 = XOR)]
[attlen bytes: first non-null value]
[variable: bit-packed stream]
```

## Dictionary Encoding

During compaction, `cs_try_dict_encode` attempts to dictionary-encode each
column before general-purpose compression.  If a column has few enough
distinct values relative to the number of rows, values are replaced by
compact integer indices into a dictionary, which dramatically reduces the
data size for low-cardinality columns (status codes, country codes, boolean
flags, repeated strings, etc.).

### When dictionary encoding is used

Dictionary encoding is attempted for every column.  It is abandoned if:

- The column has more than `CS_DICT_MAX_ENTRIES` (65,535) distinct non-null
  values — the on-disk header stores the dictionary size as `uint16`.
- The number of distinct values exceeds `nrows / 4` — at that cardinality
  the index array overhead outweighs the savings.
- The encoded size is not smaller than the raw serialized size.

### On-disk format

The `CS_COMPRESS_DICT` flag (0x10) is OR'd into the column chunk's
`cc_compression` field alongside the base compression method (LZ4, PGLZ,
or none).  The dictionary-encoded payload, before base compression, is:

```
[2 bytes]  dict_count   (uint16, number of distinct values)
[1 byte]   index_width  (1, 2, or 4 bytes per index entry)
[1 byte]   has_null     (0 or 1)
[variable]  dictionary values  (dict_count entries)
[variable]  index array        (nrows × index_width bytes)
```

Index width is chosen as the smallest type that fits:
- 1 byte if `dict_count + has_null <= 255`
- 2 bytes if `dict_count + has_null <= 65535`
- 4 bytes otherwise (not currently reachable given the 65,535 limit)

NULL is encoded as `idx = dict_count` (one past the last dictionary entry).

### Encoding strategy by type

**Fixed-length by-value** (int2, int4, int8, float4, float8, etc.):
Values are sorted, deduplicated, and stored as packed `attlen`-byte entries.
Each row's index is found via binary search in the sorted dictionary.

**Variable-length / by-reference** (text, varchar, bytea, numeric, etc.):
An open-addressing hash table with FNV-1a hashing finds distinct values.
Dictionary entries are written as length-prefixed byte streams (same format
as the non-dict varlena serialization).  Byte-level `memcmp` is used for
equality during hash probing.

### Read path

On first access to a dictionary-encoded column, `cs_ensure_column_loaded`
populates a `CSDictColumn` struct in the column cache with the dictionary
values and index array.  `tts_columnstore_getsomeattrs` then resolves each
row's value as `dict_values[index_data[row]]`, avoiding per-row
decompression and length parsing.

## Zone Maps

During compaction, the columnstore computes per-column min/max values for each
row group.  Zone maps are supported for all types that have a default btree operator
class.  By-value fixed-length types (int2, int4, int8, float4, float8, date,
timestamp, oid, bool, etc.) store min/max as inline Datum values.
Variable-length by-reference types (text, numeric, uuid, etc.) store the
min/max values inline in the zone map struct when each value fits within
32 bytes (`CS_ZONEMAP_INLINE_SIZE`).  When either value exceeds 32 bytes,
the columnstore builds **prefix zone maps** (`CS_ZM_PREFIX`) for types
whose collation uses byte-wise ordering (C, POSIX, or no collation).
The min prefix is a simple truncation of the first 32 bytes (always <=
the full value in byte order); the max prefix is truncated and then
incremented at the last payload byte to produce a valid upper bound.
These prefix bounds are valid varlena values that the btree comparison
function can process, so the same skip logic applies.  For non-C
collations where byte truncation does not preserve sort order, large
values get no zone map.  Columns with NULLs still get zone maps tracking
the range of non-NULL values.

The columnstore's own CustomScan (which replaces the planner's stock
SeqScan paths) extracts simple `WHERE` comparisons (Var op Const) from
the rel's `RestrictInfo` list and hands them to the scan via
`cs_scan_set_qual_keys` — an extension-internal helper, not a table-AM
callback.  During scan, the columnstore checks each key against the zone
map for its column using the type's btree comparison function.
Supported btree strategies:

| Strategy | Condition to skip row group         |
|----------|-------------------------------------|
| `<`      | zone_min >= scan_value              |
| `<=`     | zone_min > scan_value               |
| `=`      | scan_value < zone_min OR > zone_max |
| `>=`     | zone_max < scan_value               |
| `>`      | zone_max <= scan_value              |

If any scan key proves the row group cannot contain matching rows, the
entire group is skipped without reading any column data.

### ScalarArrayOpExpr (IN-list) Pushdown

`WHERE col IN (1, 2, 3)` generates scan keys with the `SK_SEARCHARRAY`
flag.  The columnstore evaluates these against zone maps (if any array
element falls within the zone map range, the row group cannot be skipped)
and per-row during column scanning.

### Dictionary-Aware Scan Key Evaluation

For dictionary-encoded columns, scan key predicates are pre-evaluated
against the dictionary entries once per row group, producing a
`dk_match[key_idx][dict_code]` boolean array.  Per-row filtering then
reduces to a single array lookup per key — O(1) instead of calling the
comparison function for each row.  This applies to both simple predicates
and `SK_SEARCHARRAY` keys.

### Bloom Filter Pushdown

Hash join probe values can be pushed to the scan side via
`cs_scan_set_bloom_filter`.  The bloom filter is checked against zone
maps at the row-group level and per-row during scanning.  For
dictionary-encoded columns, a `bf_dk_match` array is pre-computed
analogously to scan key dictionary evaluation.

## Column Serialization Format

During compaction, each column's values are serialized into a contiguous
byte buffer.  If dictionary encoding is profitable, the dict-encoded format
(described above) replaces the raw format.  Otherwise, the raw format is
used and then block-compressed.

**Fixed-length by-value types** (`attbyval && attlen > 0`):

- If the column has nulls: a null bitmap prefix (`ceil(nrows / 8)` bytes,
  bit set = NOT NULL) followed by packed values (`attlen * nrows` bytes).
  NULL rows are stored as zero bytes.
- If no nulls: just the packed values.
- Values are stored via `store_att_byval`; they are read back via
  `fetch_att(raw + row * attlen)`.

**Variable-length / by-reference types** (`attlen == -1`, `-2`, or
fixed-length by-ref):

- A length-prefixed stream: for each row, `int32 len` followed by `len`
  bytes of data.
- NULL marker: `len = -1` (no data bytes follow).
- For varlena (`attlen == -1`): `len = VARSIZE_ANY`, data is the raw
  varlena including its header.
- For cstring (`attlen == -2`): `len = strlen + 1`, data is the
  null-terminated string.

The serialized buffer is then compressed (LZ4, PGLZ, or none) and written
across pages via `cs_write_column_data`.

## Cost Model

When `cs_set_rel_pathlist_hook` replaces the planner-supplied SeqScan
paths with a CustomPath, it applies a single AM-supplied scaling
factor:

- **`cpu_tuple_cost_factor = 0.5`** — multiplies the per-tuple
  portion (`total_cost - startup_cost`) of the inherited baseline
  path.  Columnar scans skip column decompression for unprojected
  columns and skip per-tuple `HeapTupleHeader` parsing, so per-tuple
  CPU cost is lower than heap.

`cs_relation_cost_factors` also computes a few other quantities
(`seq_page_cost_factor`, `rand_page_cost_factor`, `rand_io_pages`,
`disk_cost_parallelizable`, `index_fetch_cost`) that an integrated
table-AM cost-factor hook in `costsize.c` could consume.  None of
them are applied today: there is no AM-level cost-factor hook to plug
into, and the hook surface currently used (`set_rel_pathlist_hook`)
only sees the final cost numbers, not the page/tuple decomposition,
so the page-cost and random-I/O factors have nowhere to land.  Index
scan costs use the heap defaults; this is the limitation listed under
"Future work" below.

`cs_relation_estimate_size` reports `cs_total_rows` and
`cs_data_pages` from the metapage (both maintained by VACUUM).
`cs_data_pages` counts only column data and delta pages, excluding
metadata overhead, so the planner sees accurate I/O size.
`allvisfrac` is reported as `0` because the AM does not maintain a
visibility map fork for columnar virtual TIDs; see the Index Only
Scans section below for what this means in practice.

Before the first VACUUM, `cs_relation_estimate_size` falls back to
`RelationGetNumberOfBlocks`.

## Parallel Scan

The columnstore supports parallel sequential scans using a work-unit model.
Shared state is stored in DSM via `CSParallelScanDescData`:

- `pcs_nallocated` (atomic uint64): the next work unit to hand out.
- `pcs_rgdir_start` / `pcs_rgdir_npages` / `pcs_nrowgroups`: the
  coordinates of the row group directory as the leader saw it.  The
  directory contents are deliberately *not* copied into shared memory:
  the struct must be fixed-size because `parallelscan_estimate` and
  `parallelscan_initialize` read the metapage at different times, and a
  VACUUM committing new row groups in between would overflow a
  flexible array sized at estimate time.  Workers read the directory
  pages themselves from the captured coordinates, which is safe because
  a published directory is never modified in place and freed directory
  pages are only reused under `AccessExclusiveLock`.

Work units are coarse-grained:

| Unit | Assignment |
|------|-----------|
| 0    | Entire delta store (one worker gets all delta pages) |
| 1..N | One row group per unit |

Workers atomically increment `pcs_nallocated` to claim the next unit,
then run `cs_delta_getnext` or `cs_columnar_getnext` on the claimed unit.
The scan descriptor's `nrowgroups` is temporarily bracketed to one row
group per unit so `cs_columnar_getnext` stops after that single group.

`cs_compute_parallel_workers` returns one worker per row group, capped at
4.  Parallel scan is suppressed when the table has 0 or 1 row groups,
since the overhead of Gather exceeds the benefit for small tables.

## Index Scan

Index scans use `CSIndexFetchData` with an **LRU cache** of
decompressed row groups, dynamically sized on first use to
`min(nrowgroups, CS_INDEX_CACHE_MAX)` (cap = 256).  This amortizes the
cost of random lookups that hit the same row group repeatedly and
keeps repeated nested-loop probes warm across rescans.

For each `cs_index_fetch_tuple` call:

1. Decode the TID.  Delta TIDs are handled by reading the heap-format
   page directly (same as `cs_fetch_row_version`).
2. For columnar TIDs: check if the row group is already in the LRU cache.
   If so, reuse it.  If not, evict the oldest entry and load the new row
   group.
3. Check the deletion bitmap and tombstone list.  Check MVCC visibility
   (columnar rows are all-visible unless deleted; delta rows use standard
   visibility).
4. Fill the slot from the cached column data.

### Index-Only Scans

IOS today is a planner-shape choice, not a true "don't touch the heap"
optimisation.  `cs_relation_estimate_size` reports `*allvisfrac = 0`
because the AM does not maintain a visibility map fork for the
virtual TID range that columnar rows live in.  Reporting a non-zero
allvisfrac would let the planner pick an IOS path whose execution
still falls through to `cs_index_fetch_tuple` for every row — a
plan-shape lie with no real heap-skip behind it.

With `allvisfrac = 0` the planner can still pick IOS for covering
queries (every projected column is in the index) because IOS over a
covering index avoids tuple deformation that Index Scan would do.
Execution still calls `cs_index_fetch_tuple` for the per-tuple
visibility check on every columnar row; the work the AM does per
tuple is the same as a plain Index Scan.

A future commit could maintain a visibility map for the virtual TID
range — setting bits in `cs_compact_delta` after a row group is
written, clearing them in `cs_columnar_delete` before the tombstone
lands, and re-setting them in `cs_materialize_tombstones` once the
row group is rewritten.  That would make IOS a true heap-skip for
compacted row groups with no pending tombstones.

## Bitmap Scan

Bitmap scans iterate blocks from `tbm_iterate`:

- **Delta blocks** (`blkno < CS_COLUMNAR_BLKNO_BASE`): read the real
  heap-format page, check visibility with a copied tuple header, deform
  into the slot.
- **Columnar virtual blocks**: decode `(rg_id, row_base)` from the
  virtual block number, load the row group if different from the current
  one, and buffer the matching row indices.  Lossy pages return all rows
  in the virtual block; exact pages extract specific offsets from the
  bitmap.

## ANALYZE

ANALYZE uses the `scan_analyze_next_block` / `scan_analyze_next_tuple`
callbacks in two phases:

**Phase 1 — Delta store sampling.**  The read stream feeds blocks in
ANALYZE's random sampling order across the entire relation.  Only blocks
that fall within the delta store extent (`cs_delta_start` ..
`cs_delta_start + cs_delta_nblocks`) are accepted; metapage, catalog, and
column data pages are skipped.  Each accepted delta page is locked SHARE
and its tuples are deformed into virtual tuples in the standard way
(live rows increment `*liverows`, dead ItemIds increment `*deadrows`).

**Phase 2 — Columnar row groups (exact).**  After the delta stream is
exhausted, all compacted row groups are iterated sequentially.  Every row in
every row group is visited:

- Deleted rows (bit set in deletion bitmap) count as dead.
- Live rows are returned to ANALYZE with lazy decompression (the
  `CSTupleTableSlot` is set up with `tts_nvalid = 0` so columns are
  loaded on demand when ANALYZE's `compute_stats` functions access them).

For the columnar phase the AM reports exact `liverows`/`deadrows`
counts directly; standard ANALYZE block-sampling math handles only the
delta-phase contribution.

## WAL Strategy

All page modifications use PostgreSQL's **Generic WAL** facility
(`generic_xlog.h`). This logs page-level diffs rather than logical
operations, which is more verbose but provides correct crash recovery
without requiring custom WAL resource managers. Every buffer modification
follows the pattern:

```c
state = GenericXLogStart(rel);
page  = GenericXLogRegisterBuffer(state, buf, flags);
/* modify page */
GenericXLogFinish(state);
```

## MVCC

- **Delta store rows** carry full `HeapTupleHeader` data with xmin, xmax,
  cmin, cmax, and infomask bits. Standard visibility functions
  (`HeapTupleSatisfiesVisibility`) determine whether each row is visible to
  the current snapshot.

- **Columnar row groups** contain only committed-visible rows at the time
  of compaction. All rows in a compacted row group are treated as
  all-visible. Subsequent deletions use two mechanisms:

  - **Deletion bitmaps** — one bit per row, stored on a dedicated page
    per row group.  Set immediately on DELETE for fast skip during scans
    and compaction.

  - **Tombstones** — minimal heap tuples in the delta store with zero
    user attributes and a `t_ctid` pointing to the target virtual TID.
    Tombstones carry xmin/xmax for MVCC: a concurrent transaction whose
    snapshot predates the delete still sees the row, because the
    tombstone's xmin is not yet committed in that snapshot.  Scans
    collect committed tombstones lazily (`cs_collect_tombstones`) and
    check them per-row alongside the deletion bitmap.  VACUUM
    materializes into the deletion bitmap only tombstones committed
    before the visibility horizon (older snapshots may still need the
    row) and then consumes the fully-dead prefix of the delta.

  - **Lock-only tombstones** share the tombstone format but record a row
    lock, not a delete (`HEAP_XMAX_LOCK_ONLY` in their infomask): every
    visibility path skips them, writers treat a live one as a conflict,
    and VACUUM reaps them once the locking transaction has ended.

- **Freezing.**  VACUUM freezes whatever remains in the delta after
  compaction (`HEAP_XMIN_FROZEN` for rows visible to all, `LP_DEAD` for
  rows dead to all, ended-transaction xmax cleared) and reports the
  visibility horizon as the new `relfrozenxid`, so columnstore tables
  participate normally in wraparound prevention.  Columnar row groups
  hold no transaction ids at all.

## Files

The AM lives in `contrib/columnstore/`:

| File | Purpose |
|------|---------|
| `columnstore.control` / `columnstore--1.0.sql` | Extension control file and install script (`CREATE EXTENSION columnstore`); registers the handler function and access method |
| `cs_internal.h` | Private to the AM: on-disk layout, scan-state structs, cross-file prototypes |
| `columnstore.c` | `_PG_init` (GUC + CustomScan registration), handler, TableAmRoutine, DELETE/UPDATE, DDL stubs |
| `cs_storage.c` | Metapage init/read/write, directory/freelist read/write, page helpers |
| `cs_insert.c` | INSERT and tombstone insertion into delta store via GenericXLog |
| `cs_scan.c` | Sequential scan, zone maps, scan key pushdown, bloom filters |
| `cs_slot.c` | Custom TupleTableSlotOps with lazy column loading |
| `cs_compact.c` | Delta-to-columnar compaction, row group compaction with merging |
| `cs_vacuum.c` | VACUUM entry point, stats computation, file truncation |
| `cs_customscan.c` | CustomScan provider: replaces stock SeqScan paths, parallel-aware path generation, aggregate pushdown via `create_upper_paths_hook`, ported helpers for projection/qual extraction from plan tree |

`CREATE EXTENSION columnstore` runs `columnstore--1.0.sql`, which
defines `columnstore_tableam_handler(internal)`, runs
`CREATE ACCESS METHOD columnstore TYPE TABLE HANDLER ...`, and
installs a `ddl_command_end` event trigger
(`columnstore_set_autovacuum_defaults`) that applies a fixed
autovacuum cadence aligned with the row-group size to every
columnstore table at CREATE time.  The extension's only GUC,
`columnstore.rowgroup_compaction_threshold`, is registered in
`_PG_init`.

## Architecture

The extension lives entirely in `contrib/columnstore/`.  Four core
changes support it:

1.  Hash-join bloom-filter pushdown.  The planner may attach a bloom
    filter built from a hash join's inner side to a scan node on the
    join's outer side.  A CustomScan opts in as a recipient with the
    `CUSTOMPATH_SUPPORT_BLOOM_FILTERS` path flag; the executor then
    builds the filter eagerly (before the scan's first tuple request,
    so a column store never decompresses an unfiltered first batch)
    and, for multi-key joins, can also build one filter per join key
    alongside the combined-hash filter.  At startup the scan fetches
    the filters from the EState's bloom-producer registry; the
    columnstore CustomScan hands them to its scan loop via
    `cs_scan_set_bloom_filter`, eliminating rows at row-group,
    dictionary, or per-row granularity before decompression.

2.  `amoptions` callback added to `TableAmRoutine`, with a matching
    `table_reloptions()` entry point that dispatches to it and a
    small `add_reloption_to_kind(name, kind)` helper for opting an
    existing reloption into an additional kind.  Together these let
    the columnstore (and any future non-heap table AM) register
    AM-specific reloptions in its own `RELOPT_KIND_*` namespace
    instead of polluting `StdRdOptions` / `RELOPT_KIND_HEAP`.  The
    columnstore wires `cs_reloptions` into this callback, defines
    its own `CSRdOptions` struct where `sort_key` lives, and uses
    `add_reloption_to_kind` in `_PG_init` so its parser also
    accepts the standard heap options users expect to be able to
    set on a columnstore table (`fillfactor`, `parallel_workers`,
    the `autovacuum_*` set, `vacuum_truncate`).

3.  `create_upper_paths_hook` (and the matching `GetForeignUpperPaths`)
    fired for `UPPERREL_PARTIAL_GROUP_AGG`, mirroring the existing
    fire on `UPPERREL_PARTIAL_DISTINCT`.  Without it, an extension
    cannot register a partial-aggregate path through the standard
    hook surface in time for `gather_grouping_paths` to pick it up.

4.  `NoHintBitsBuffer`, a distinguished Buffer value the tuple
    visibility functions accept in place of the tuple's buffer to
    request the visibility verdict without any hint-bit maintenance.
    The columnstore's delta pages are WAL-logged via generic WAL, so
    a hint bit written behind the AM's back would invalidate the page
    deltas generic WAL computes; the delta visibility check
    (`cs_delta_satisfies_visibility`) passes the sentinel instead of
    the real buffer.

Everything else is built from inside the extension via the standard
planner and executor hook surface:

| Capability | Mechanism |
|---|---|
| Column projection | CustomScan walks the plan tree (`cs_extract_scan_needed_cols`) and forwards projection to the AM via a private helper |
| Qual-keys push | CustomScan extracts btree-strategy scan keys from `RestrictInfo` (`cs_extract_scan_qual_keys`) |
| Replace SeqScan paths | `set_rel_pathlist_hook` removes the planner's stock SeqScan paths (serial and partial) and installs a CustomPath in their place; the per-tuple portion of the baseline cost is scaled down by the AM's `cpu_tuple_cost_factor` to reflect the cheaper per-tuple work of a column-projected scan |
| Parallel-worker count | A second, partial-aware CustomPath sets `parallel_workers` directly; `EstimateDSMCustomScan` and friends drive `ParallelTableScanDesc` setup the same way `nodeSeqscan` does |
| Aggregate pushdown | `create_upper_paths_hook` recognises COUNT / SUM / AVG / MIN / MAX (with FILTER, GROUP BY, HAVING, WHERE) and emits a `Custom Scan (ColumnstoreAggregate)` that accumulates raw int64 → int128 directly inside the scan, finalising once per group |
| Late materialisation | `create_upper_paths_hook` at `UPPERREL_ORDERED` adds a `Custom Scan (ColumnstoreLateMat)` whose probe tuplesort holds only `(sort_key…, packed_tid)` and refetches payload columns via `table_tuple_fetch_row_version` for the LIMIT survivors |
| Autovacuum defaults | DDL event trigger applies autovacuum defaults at CREATE time |
| Insert batching | COPY uses `table_multi_insert` directly; `INSERT ... SELECT` falls back to per-row `tuple_insert` (no AM hook exists to request batching) |
| IOS fall-through | IOS on columnstore tables falls back to standard index scan |

Plan-time agg pushdown covers COUNT(*) / COUNT(col) / SUM / AVG /
MIN / MAX, dispatching by `F_*` aggregate fnoid.  NUMERIC SUM/AVG
accumulates raw int64 into int128 via the NI64 fast path when the
column is so encoded, and falls back to `numeric_add` otherwise.

## Future work

* **Aggref ORDER BY.**  The pushdown bails on
  `string_agg(x ORDER BY y)`-style aggregates -- niche in analytical
  workloads, falls through to the standard Aggregate executor.

* **GROUP BY agg pushdown with spill.**  The pushdown bails on
  high-cardinality combined keys (where the per-group hash would not
  fit in `work_mem`) to avoid OOM on bad row-count estimates.
  Adding a HashAgg-style spill-to-disk path inside the pushdown's
  TupleHashTable would let us re-enable it for the cases where the
  speedup over standard agg is meaningful.  ~200 LOC of plumbing.

* **Cost-model coverage beyond seqscan.**  Only the per-tuple CPU
  factor is currently applied to scan costs, and only on the
  CustomPath that replaces the stock SeqScan.  Index-scan and
  bitmap-scan costing still uses the heap defaults, so the planner
  may pick an index path on a columnstore relation when a
  CustomScan + filter would be cheaper.  Wiring up the page-cost and
  random-I/O factors and applying them to the appropriate path types
  would close this.  A core `relation_cost_factors` callback in
  `costsize.c` is one option; another is a path-type cost-modifier
  hook callable from each `cost_*()` routine.

* **True IOS heap-skip for columnar rows.**  Today `allvisfrac` is
  reported as 0 and the executor's `VM_ALL_VISIBLE` check therefore
  fails on every columnar virtual TID, so Index Only Scan falls
  through to `cs_index_fetch_tuple` per tuple.  Maintaining a
  visibility map fork for the columnar virtual-TID range would let
  IOS actually skip the fetch on row groups with no pending
  tombstones.  Sketch: set VM bits for every virtual block in
  `cs_compact_delta` once the row group is committed (every row is
  newly written and visible to all snapshots); clear the bit for the
  target virtual block in `cs_columnar_delete` before the tombstone
  is written; re-set the bit in `cs_materialize_tombstones` after the
  row group is rewritten without the deleted rows.  Concurrency does
  not have a heap buffer to pin against, so serialise via the
  metapage or a per-rowgroup lock.

## Usage

The extension must be listed in `shared_preload_libraries` so the
CustomScan methods are registered in every backend (including
parallel workers) before plans referencing them are deserialised:

```
# postgresql.conf
shared_preload_libraries = 'columnstore'
```

After restarting the server, run `CREATE EXTENSION columnstore;` once
per database that will use it.

```sql
-- Create a table using the columnstore AM
CREATE TABLE events (
    ts     timestamptz,
    user_id int,
    action  text,
    payload jsonb
) USING columnstore;

-- Insert data (goes to delta store)
INSERT INTO events SELECT ...;

-- Compact delta store into compressed columnar row groups
VACUUM events;

-- Analytical query — only ts and user_id columns are read
SELECT date_trunc('hour', ts), count(DISTINCT user_id)
FROM events
WHERE ts >= '2025-01-01'
GROUP BY 1;
```

## Storage Parameters

### sort_key

```sql
CREATE TABLE events (ts timestamptz, user_id int, data text)
    USING columnstore WITH (sort_key = 'ts');

-- Multi-column sort key:
CREATE TABLE events (...) USING columnstore WITH (sort_key = 'ts, user_id');

-- Change on existing table (takes effect on next VACUUM):
ALTER TABLE events SET (sort_key = 'ts');
```

When set, VACUUM sorts each batch of rows by the specified column(s)
before writing them into a columnar row group.  Each column must have a
default btree operator class.  Multiple columns are separated by commas;
the sort is lexicographic (first column is the primary key, second breaks
ties, etc.).  NULLs sort last.

**Benefits:**

- **Compression.**  Sorted data dramatically improves delta+FOR encoding.
  Monotonic timestamps with constant intervals produce identical deltas
  that bit-pack in ~0 bits per value.
- **Zone maps.**  Sorted row groups have non-overlapping min/max ranges,
  enabling aggressive row-group skip on range predicates.
- **RLE.**  Sorting a low-cardinality column creates maximal-length runs.

**Cost.**  The sort adds ~10–50% to VACUUM time for integer keys (less
for data that is already nearly sorted).  The improved compression can
reduce I/O enough to make the net effect neutral or even faster.

Tables without `sort_key` are completely unaffected.

## Autovacuum Tuning

PostgreSQL's autovacuum triggers vacuum on insert-heavy tables when:

```
inserts > autovacuum_vacuum_insert_threshold
          + autovacuum_vacuum_insert_scale_factor * reltuples
```

The defaults (threshold = 1000, scale_factor = 0.2) are designed for heap
tables where vacuum reclaims dead tuples.  For the columnstore, vacuum
compacts the delta store into columnar row groups of `CS_ROWS_PER_ROWGROUP`
(100,000) rows.  The default settings cause two problems:

1. **Base threshold too low.**  On a new table, autovacuum fires after just
   1,000 inserts, creating a tiny row group with poor compression and wasted
   catalog overhead.

2. **Scale factor too high.**  On a 50-million-row table, the threshold is
   10 million inserts before compaction — 100 row groups worth of data stuck
   in the slow heap-format delta store.

### Automatic defaults

The columnstore extension installs a `ddl_command_end` event trigger
(`columnstore_set_autovacuum_defaults`, defined in
`columnstore--1.0.sql`) that fires for every `CREATE TABLE`.  When
the new relation uses the columnstore AM and the user did not set
the option in the `WITH` clause, the trigger issues
`ALTER TABLE ... SET (...)` to fill in:

```
autovacuum_vacuum_insert_threshold   = 100000  (CS_ROWS_PER_ROWGROUP)
autovacuum_vacuum_insert_scale_factor = 0
```

Without these, the global defaults (1,000 + 0.2 x reltuples) fire
autovacuum too eagerly on small tables and far too late on large
ones.  Aligning the threshold with the row group size and disabling
the scale factor makes autovacuum fire at a fixed cadence regardless
of how big the table grows.

User-supplied WITH options always take precedence — the trigger
checks the pg_class reloptions array first and only applies a
default if that option is absent:

```sql
-- Override the threshold for a specific table
CREATE TABLE events (...) USING columnstore
    WITH (autovacuum_vacuum_insert_threshold = 50000);
```

### Minimum-rows compaction guard

As a safety net, when running under **autovacuum**, `cs_compact_delta` skips
compaction if the entire delta store contains fewer than
`CS_COMPACT_MIN_DELTA_ROWS` (currently `CS_ROWS_PER_ROWGROUP / 10` = 10,000)
visible rows and no full row groups have been written.  This prevents
poorly-compressed row groups if the automatic defaults are overridden or if
autovacuum fires between the insert threshold and a meaningful batch size.
The delta rows are preserved for the next vacuum cycle.

Manual `VACUUM` always compacts regardless of delta size, so small tables
can be compacted on demand.

## Design Lineage

The columnstore is a from-scratch Table AM implementation, not a port of any
single system.  But the architecture follows well-established columnar storage
patterns that most modern analytical engines converge on.

### Delta store + background compaction

The hybrid row/column design — buffering writes in a row-oriented delta store
and compacting them into compressed columnar row groups — comes from the
**SQL Server Columnstore Index** architecture.  SQL Server has a "deltastore"
(B-tree rowstore) for recent inserts and a "tuple mover" that compresses it
into columnar segments.  **SAP HANA** uses the same pattern (row-oriented
delta + column-oriented main store + merge process), as does **Apache Kudu**
(MemRowSets flushed to columnar DiskRowSets).

Our variant uses VACUUM as the compaction trigger rather than a background
thread, which is natural for PostgreSQL but means compaction is less
automatic.

### Row groups with per-column compressed chunks and zone maps

The internal structure of a row group — per-column compressed byte streams
with per-column min/max statistics — follows the **Apache Parquet** and
**Apache ORC** file formats.  Parquet has "row groups" containing "column
chunks" with min/max statistics; ORC has "stripes" containing "column streams"
with index data.  Our 100,000-row row groups are in the same ballpark as
**DuckDB**'s row groups (~122K rows).

Zone maps are ubiquitous under different names: "storage indexes" in Oracle
Exadata, "min/max indexes" in Vertica, "segment elimination" in SQL Server.

### Closest PostgreSQL comparison: Citus Columnar

The most directly comparable PostgreSQL implementation is **Citus Columnar**
(originally `cstore_fdw`, later rewritten as a Table AM extension).  It uses
an ORC-inspired stripe/chunk model with skip indexes and compression.  Key
differences:

- Citus Columnar stores metadata in regular PostgreSQL catalog tables; we use
  a custom metapage and directory pages within the same relation fork.
- Citus Columnar has no delta store — all writes go directly to columnar
  format, making single-row inserts expensive.
- Citus Columnar does not support indexes, bitmap scans, or parallel scans.
- Citus Columnar writes stripes eagerly on INSERT; we batch via VACUUM.

The delta store is the biggest architectural difference and is what makes
mixed read/write workloads viable.

### Deletion bitmaps

Per-row-group deletion bitmaps are standard practice.  SQL Server Columnstore
uses "deleted row bitmaps" per segment.  Apache Iceberg uses "position delete
files."  Delta Lake uses "deletion vectors."

### What is PostgreSQL-specific

Several aspects of the implementation are shaped by PostgreSQL's internal
APIs and have no direct analogue in other columnar stores:

- **Virtual TID encoding.** Other systems do not need this because they lack
  PostgreSQL's requirement that every row has a `(block, offset)` TID for
  index integration.
- **GenericXLog for crash safety.** Page-level diffs rather than a custom WAL
  resource manager.
- **Table AM callback integration.** The bitmap scan, parallel scan, and
  ANALYZE hooks are shaped by the executor's expectations.
- **Hybrid tuple locking.** A transient `LOCKTAG_TUPLE` heavyweight lock
  serializes check-and-record (providing deadlock detection and
  `SKIP LOCKED` / `NOWAIT` for free) while a durable lock-only xmax or
  lock tombstone carries the lock itself — heap's structure minus
  MultiXacts, trading concurrent compatible locks on delta rows for
  significant complexity avoided.
- **LRU row group cache for index scans.** Most columnar stores either do not
  support index scans or use different strategies (e.g., DuckDB's ART indexes
  with direct row group references).

## Limitations

Known limitations include:

- **Requires a native 128-bit integer type.**  The SUM/AVG aggregate
  accumulators use `int128`, so columnstore is built only where the platform
  provides one (`PG_INT128_TYPE`).  32-bit builds and MSVC (which has no
  `__int128`) are unsupported — the build system skips the extension there
  rather than failing the build.
- **TABLESAMPLE sampling is approximate for SYSTEM method.** The SYSTEM
  method samples at the block level; since row groups map to many virtual
  blocks, it may over- or under-represent individual row groups.  BERNOULLI
  sampling works correctly at the tuple level.
- **CLUSTER and VACUUM FULL are not supported.**  Both routes through
  `cs_relation_copy_for_cluster` raise an error
  (`"CLUSTER is not supported for columnstore tables"`).  Row-group
  ordering is instead requested at write time via the `sort_key`
  reloption — subsequent `VACUUM` compactions sort each new row group's
  rows by the configured columns.
- **Row group size is fixed** at 100,000 rows (`CS_ROWS_PER_ROWGROUP`).
- **Zone maps for variable-length types with non-C collations are
  size-limited.** By-reference types (text, numeric, etc.) with C or POSIX
  collation get zone maps for any value size via prefix truncation.  For
  non-C collations, zone maps are only stored when both min and max fit
  within 32 bytes (`CS_ZONEMAP_INLINE_SIZE`), since byte-level truncation
  does not preserve sort order for locale-aware collations.
- **UPDATE** is implemented as DELETE + INSERT, which is correct but does
  not preserve physical locality.
- **No update chains.**  Because UPDATE creates an unrelated new row,
  a READ COMMITTED `SELECT FOR UPDATE` (or EvalPlanQual recheck) that
  waits out a concurrent UPDATE sees the old row as *deleted* and skips
  it; heap would follow the ctid chain and lock the updated version.
  The same applies to a row a concurrent compaction moves while a
  writer waits on its fence: the writer reports the row deleted at that
  TID rather than chasing it into columnar storage.
- **One recorded locker per delta row.**  Row locks on delta rows are
  recorded in the single xmax (no MultiXact support), so concurrent
  *compatible* lock requests — two `FOR KEY SHARE`, for instance —
  serialize on delta rows.  Columnar rows record each locker in its own
  lock tombstone and do not have this restriction.
- **SERIALIZABLE executes with REPEATABLE READ semantics.**  The AM
  implements snapshot isolation but no SSI predicate locking
  (`PredicateLockTID` etc. are never called), so true-serializability
  anomalies that SSI would detect on heap tables are not detected on
  columnstore tables.
- **No logical decoding.**  Changes to columnstore tables go through
  GenericXLog, which logical decoding does not decode: logical
  replication of a columnstore table silently replicates nothing.
- **EXPLAIN ANALYZE counters are leader-only for parallel scans.**  The
  custom-scan instrumentation (rows skipped by zone maps, bloom-filter
  probes, and the like) is not aggregated from parallel workers.
- **BRIN indexes are not yet useful.**  BRIN indexes summarize ranges of
  physical blocks, which maps naturally to row groups in a columnstore.
  However, the columnstore already provides built-in zone maps that skip
  row groups during sequential scans — the same optimization BRIN would
  provide.  BRIN bitmap index scans do work today, but BRIN's
  block-range bounds are not aligned with the virtual-TID layout, so
  the ranges it summarizes do not map cleanly to row groups.  Once
  that alignment exists, BRIN bitmap scans could narrow row groups
  before joining or filtering; until then, the built-in zone maps
  cover the primary use case (range-predicate row group skipping on
  sequential scans).
