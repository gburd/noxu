# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Fixed

- **EV-6: an upper IN with cached (resident) children is no longer evicted.**
  Since EV-13 made full-node eviction actually detach the node from its
  parent, evicting an upper IN that still has resident children would orphan
  those children (their parent pointer would dangle). `decide_eviction` now
  skips a non-BIN node whose `find_node_full` walk found any resident child
  (`InEntry.child.is_some()`). Mirrors JE `Evictor.processTarget`
  `IN.hasCachedChildren` / the `NON_EVICTABLE_IN` skip
  (`Evictor.java:2652-2656`).

- **EV-7: the tree root IN is no longer evicted.** Noxu's `is_root` was never
  consulted in the evict decision; with EV-13's detach live this was a latent
  correctness gap for the internal ID/NAME DB roots, which JE keeps resident.
  `decide_eviction` now skips any root IN (the simplest faithful rule).
  Mirrors JE `Evictor.processTarget` `IN.isRoot()` root-protection
  (`Evictor.java:2663-2671`). `Tree::detach_node_by_id` already refused to
  detach the root, so this adds defense-in-depth at the decision layer.

- **REP-5: VLSN `lastSync`/`lastTxnEnd` now advance in production.** The
  production VLSN registration path (`VlsnIndex::put`/`register`) only called
  `VlsnRange::extend`, so a running node's `sync_vlsn` (lastSync) and
  `commit_vlsn` (lastTxnEnd) stayed at `0` (NULL_VLSN); the JE-faithful
  dispatch (`VlsnRange::update_for_new_mapping`) was reachable only from unit
  tests. Added `VlsnIndex::put_with_type`/`register_with_type` and routed the
  production register sites that know the entry type — replica
  `EnvironmentLogWriter`, master `replicate_entry`/`apply_entry`, and the
  recovered-XA/recovered-commit paths — through it so the sync/commit
  boundaries advance correctly (lag reporting, consistency, syncup substrate).
  Mirrors JE `VLSNIndex.put(LogItem)` → `VLSNTracker.track` →
  `VLSNRange.getUpdateForNewMapping(vlsn, entryTypeNum)`.

- **REP-6: a streaming replica now feeds the SHARED/persisted VLSN index.**
  `become_replica` constructed a fresh `Arc::new(VlsnIndex::new(10))` and
  handed it to the replica receive loop, so the env's shared `self.vlsn_index`
  (the one `flush_to_disk` persists and `get_vlsn_range`/election ranking read)
  never reflected received entries. The persisted `vlsn.idx`, the reported
  VLSN range, and the DTVLSN-ranking `own_vlsn` all lagged the actually-
  received stream, widening catch-up (or forcing an unnecessary network
  restore) after a clean restart. `become_replica` now passes
  `Arc::clone(&self.vlsn_index)`, matching JE where the replica's `VLSNIndex`
  IS the environment's persisted index.
