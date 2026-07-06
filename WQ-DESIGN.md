# JE Write Queue port to Noxu — design (extracted from JE FileManager.java)

## JE algorithm (FileManager.java:1738-1810, 2778-3010)
- LogEndFileDescriptor has: fsyncFileSynchronizer (ReentrantLock),
  queuedWrites (byte[writeQueueSize]), queuedWritesPosition, qwStartingOffset,
  qwFileNum. Queue holds writes for a SINGLE file, CONTIGUOUS (append-only WAL).
- writeToFile(file, data, destOffset, fileNum, flushWriteQueue):
  1. fsyncLatchAcquired = fsyncFileSynchronizer.tryLock()   // non-blocking
  2. if !acquired && useWriteQueue && !flushWriteQueue:
       enqueueSuccess = enqueueWrite(fileNum,data,destOffset,...)  // RETURN, no I/O
  3. if !enqueueSuccess:
       if !acquired: fsyncFileSynchronizer.lock()   // block
       try { dequeuePendingWrites1(); synchronized(file){ seek+write } }
       finally { fsyncFileSynchronizer.unlock() }
- enqueueWrite1: if qwFileNum<fileNum dequeue+advance; overflow(size> remaining)
  -> RelatchRequiredException -> caller dequeues & retries (2x) else overflow-fail
  -> caller does direct write. First entry sets qwStartingOffset=destOffset.
  ASSERT curPos+qwStartingOffset==destOffset (contiguous). arraycopy into queue.
- force() (the fdatasync): fsyncFileSynchronizer.lock(); dequeuePendingWrites1();
  ch.force(false); unlock. So fsync drains the queue THEN fdatasyncs.
- dequeuePendingWrites1 (holds fsyncFileSynchronizer): if pos==0 return; else
  getWritableFile(qwFileNum); seek(qwStartingOffset); write(queuedWrites,0,pos);
  reset queuedWritesPosition=0.
- checkWriteCache: a READ at end-of-log may need bytes still in the queue;
  reads must consult the queue (HA syncup / recovery tail read).
- flushWriteQueue=true forces direct write (used for file flip / header /
  forced-flush): never enqueue.

## Noxu integration points (crates/noxu-log/src/file_manager.rs)
- write_buffer_to_file(file_num,data,file_offset): currently pwrite under
  handle.acquire(). REPLACE with the tryLock/enqueue/direct-write logic.
- sync_log_end(): currently sync_data_no_latch(). REPLACE: acquire
  fsyncFileSynchronizer, dequeue pending writes (real pwrite), fdatasync, unlock.
- Add WriteQueue state to FileManager (or a LogEnd sub-struct): fsync_lock
  (parking_lot Mutex as the ReentrantLock analog -- but must be try_lock-able and
  NOT held across the committer's return; use a std Mutex or parking_lot try_lock).
  queued: Mutex<{buf: Vec<u8>|Box<[u8]>, pos, start_offset, file_num}>.
- config knob: log_use_write_queue (default TRUE like JE), log_write_queue_size
  (JE default FILEMGR_WQ_SIZE). Wire from EnvironmentConfig.
- Reads: file_reader / recovery tail read + rep syncup must consult checkWriteCache
  when reading at/after qwStartingOffset for qwFileNum. FIND all end-of-log readers.

## GUARDRAILS (must all pass)
- Durability: a committer whose write enqueued must NOT be told durable until a
  fdatasync that dequeued+synced it completes. flush_sync_if_needed keys off
  last_synced_lsn set AFTER force(). The enqueue path must ensure the leader's
  force() dequeues before fdatasync (JE does). VERIFY no commit returns before
  its bytes are both written AND fsynced.
- crash_recovery_test 12/12, recovery_correctness_test 17/17,
  test_concurrent_commit_sync_survives_sigkill, stepwise_truncation_test.
- shuttle_fsync_manager green.
- File flip: a flip must flushWriteQueue=true (drain queue to OLD file before
  switching qwFileNum), else queued writes for the old file are lost.
- Reads at end-of-log (recovery LastFileReader, rep syncup) see queued bytes.

## Perf targets (i4i.16xlarge, 8-writer 98/2 JSON SYNC, 30min)
- COMMIT_SYNC: match/beat JE ~5000 c/s (Noxu now 2500).
- NO_SYNC: keep >= JE (~36-40k).
- KEEP: p99 flat, worst-case << JE's 1024ms, 64-writer >= 7000 c/s.
