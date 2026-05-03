package com.noxu.bench;

// Run with:
//   java -server -Xmx2g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=5 \
//        -jar je-bench-1.0.0-jar-with-dependencies.jar
//
// The JVM flags suppress GC pauses to keep measurements clean.
// Results are printed as a formatted table and written to benches/results/je_results.csv.

import com.sleepycat.je.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class JeBenchmark {

    // -------------------------------------------------------------------------
    // Workload implementations
    // -------------------------------------------------------------------------

    /** W01: Insert n sequential keys (key_0 .. key_{n-1}). */
    private static long seqWrite(Database db, int n) throws DatabaseException {
        DatabaseEntry data = new DatabaseEntry(EnvHelper.VALUE);
        for (int i = 0; i < n; i++) {
            DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(i));
            db.put(null, key, data);
        }
        return n;
    }

    /** W02: Insert n keys in shuffled order (seeded shuffle for reproducibility). */
    private static long randWrite(Database db, int n) throws DatabaseException {
        List<Integer> order = new ArrayList<>(n);
        for (int i = 0; i < n; i++) order.add(i);
        Collections.shuffle(order, new Random(42));

        DatabaseEntry data = new DatabaseEntry(EnvHelper.VALUE);
        for (int i : order) {
            DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(i));
            db.put(null, key, data);
        }
        return n;
    }

    /** W03: Read all n keys sequentially (keys must already exist in db). */
    private static long seqRead(Database db, int n) throws DatabaseException {
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < n; i++) {
            DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(i));
            db.get(null, key, data, LockMode.DEFAULT);
        }
        return n;
    }

    /** W04: n random point lookups (seed 99). */
    private static long randRead(Database db, int n) throws DatabaseException {
        Random rng = new Random(99);
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < n; i++) {
            int idx = rng.nextInt(n);
            DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(idx));
            db.get(null, key, data, LockMode.DEFAULT);
        }
        return n;
    }

    /**
     * W05: 100 range scans, each reading n/100 consecutive records starting at
     * evenly-spaced boundary keys.
     */
    private static long rangeScan(Database db, int n) throws DatabaseException {
        int batchSize = Math.max(1, n / 100);
        long opsCount = 0;

        try (Cursor cursor = db.openCursor(null, null)) {
            for (int b = 0; b < 100; b++) {
                int startIdx = b * batchSize;
                DatabaseEntry startKey = new DatabaseEntry(EnvHelper.makeKey(startIdx));
                DatabaseEntry data = new DatabaseEntry();

                OperationStatus status = cursor.getSearchKeyRange(startKey, data, LockMode.DEFAULT);
                int read = 0;
                while (status == OperationStatus.SUCCESS && read < batchSize) {
                    opsCount++;
                    read++;
                    status = cursor.getNext(startKey, data, LockMode.DEFAULT);
                }
            }
        }
        return opsCount;
    }

    /**
     * W06: Write-heavy mixed workload — n ops total, cycling: 9 puts then 1 get.
     * Keys cycle sequentially.
     */
    private static long writeHeavy(Database db, int n) throws DatabaseException {
        DatabaseEntry data = new DatabaseEntry(EnvHelper.VALUE);
        DatabaseEntry readData = new DatabaseEntry();
        for (int i = 0; i < n; i++) {
            DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(i % n));
            if (i % 10 == 9) {
                // every 10th op is a read
                db.get(null, key, readData, LockMode.DEFAULT);
            } else {
                db.put(null, key, data);
            }
        }
        return n;
    }

    /**
     * W07: Read-heavy mixed workload — n ops total, cycling: 9 gets then 1 put.
     * Keys must already exist; put keys cycle sequentially.
     */
    private static long readHeavy(Database db, int n) throws DatabaseException {
        DatabaseEntry data = new DatabaseEntry(EnvHelper.VALUE);
        DatabaseEntry readData = new DatabaseEntry();
        for (int i = 0; i < n; i++) {
            DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(i % n));
            if (i % 10 == 9) {
                // every 10th op is a write
                db.put(null, key, data);
            } else {
                db.get(null, key, readData, LockMode.DEFAULT);
            }
        }
        return n;
    }

    /**
     * W08: Delete-insert pairs — for each key i in 0..n: delete key_i then re-insert key_i.
     * Returns 2*n (each pair counts as 2 ops).
     */
    private static long deleteInsert(Database db, int n) throws DatabaseException {
        DatabaseEntry data = new DatabaseEntry(EnvHelper.VALUE);
        for (int i = 0; i < n; i++) {
            DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(i));
            db.delete(null, key);
            db.put(null, key, data);
        }
        return 2L * n;
    }

    /**
     * W09: Transactional multi-op — n transactions each performing:
     *   3 gets on existing keys + 2 puts on new/overwrite keys + commit.
     * Returns 5*n (total individual operations).
     */
    private static long txnMulti(Environment env, Database db, int n) throws DatabaseException {
        Random rng = new Random(77);
        DatabaseEntry readData = new DatabaseEntry();
        DatabaseEntry writeData = new DatabaseEntry(EnvHelper.VALUE);

        for (int t = 0; t < n; t++) {
            Transaction txn = env.beginTransaction(null, null);
            try {
                // 3 gets on existing keys
                for (int g = 0; g < 3; g++) {
                    int idx = rng.nextInt(n);
                    DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(idx));
                    db.get(txn, key, readData, LockMode.DEFAULT);
                }
                // 2 puts (may overwrite existing keys or insert new ones)
                for (int p = 0; p < 2; p++) {
                    int idx = n + t * 2 + p; // use keys beyond the pre-populated range
                    DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(idx));
                    db.put(txn, key, writeData);
                }
                txn.commit();
                txn = null;
            } finally {
                if (txn != null) {
                    try { txn.abort(); } catch (Exception ignored) {}
                }
            }
        }
        return 5L * n;
    }

    /**
     * W10: Concurrent workload — reader threads do random gets; writer threads do sequential puts.
     * All threads start together via a CyclicBarrier.
     * Returns total operations across all threads.
     */
    private static long concurrent(Database db, int n, int readerThreads, int writerThreads)
            throws Exception {
        int totalThreads = readerThreads + writerThreads;
        CyclicBarrier barrier = new CyclicBarrier(totalThreads + 1); // +1 for main
        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);
        AtomicLong totalOps = new AtomicLong(0);

        int opsPerThread = Math.max(1, n / totalThreads);

        List<Future<?>> futures = new ArrayList<>();

        // Reader tasks
        for (int r = 0; r < readerThreads; r++) {
            final int seed = r;
            futures.add(pool.submit(() -> {
                try {
                    barrier.await(); // wait for all threads to be ready
                    Random rng = new Random(seed * 1000L + 99);
                    DatabaseEntry readData = new DatabaseEntry();
                    long ops = 0;
                    for (int i = 0; i < opsPerThread; i++) {
                        int idx = rng.nextInt(Math.max(1, n));
                        DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(idx));
                        db.get(null, key, readData, LockMode.DEFAULT);
                        ops++;
                    }
                    totalOps.addAndGet(ops);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            }));
        }

        // Writer tasks
        for (int w = 0; w < writerThreads; w++) {
            final int writerIdx = w;
            futures.add(pool.submit(() -> {
                try {
                    barrier.await(); // wait for all threads to be ready
                    DatabaseEntry writeData = new DatabaseEntry(EnvHelper.VALUE);
                    long ops = 0;
                    for (int i = 0; i < opsPerThread; i++) {
                        int keyIdx = n + writerIdx * opsPerThread + i;
                        DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(keyIdx));
                        db.put(null, key, writeData);
                        ops++;
                    }
                    totalOps.addAndGet(ops);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            }));
        }

        // Release all threads simultaneously
        barrier.await();

        // Wait for all to complete
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.SECONDS);

        return totalOps.get();
    }

    // -------------------------------------------------------------------------
    // Measurement helpers
    // -------------------------------------------------------------------------

    /** Functional interface for a workload that returns the op count. */
    @FunctionalInterface
    interface Workload {
        long run() throws Exception;
    }

    /**
     * Runs a single workload measurement:
     * 1. GC pause to reduce interference
     * 2. Record before-metrics
     * 3. Run workload
     * 4. Record after-metrics
     * 5. Build and return WorkloadResult
     */
    private static WorkloadResult measure(String name, int scale, int threads,
                                          File envDir, Workload workload) throws Exception {
        Metrics.gcPause();

        long rssBefore   = Metrics.rssBytes();
        long gcBefore    = Metrics.gcTimeMs();
        long[] ioBefore  = Metrics.procIo();

        long startNs = System.nanoTime();
        long ops = workload.run();
        long endNs = System.nanoTime();

        long rssAfter    = Metrics.rssBytes();
        long gcAfter     = Metrics.gcTimeMs();
        long[] ioAfter   = Metrics.procIo();
        long diskKb      = (envDir != null) ? EnvHelper.dirSizeKb(envDir) : 0;

        double elapsedMs = (endNs - startNs) / 1_000_000.0;

        WorkloadResult result = new WorkloadResult(
                name, scale, threads,
                elapsedMs, ops,
                rssBefore, rssAfter,
                gcBefore, gcAfter,
                ioBefore[0], ioAfter[0],
                ioBefore[1], ioAfter[1],
                diskKb
        );

        // Warn if GC stole more than 5% of measurement time
        if (elapsedMs > 0 && result.gcTimeMs > elapsedMs * 0.05) {
            double pct = (result.gcTimeMs / elapsedMs) * 100.0;
            System.out.printf("  WARNING: GC stole %.1f%% of measurement time for workload %s%n",
                    pct, name);
        }

        return result;
    }

    // -------------------------------------------------------------------------
    // Temp directory management
    // -------------------------------------------------------------------------

    private static File makeTempDir(String tag) throws IOException {
        Path dir = Files.createTempDirectory("je-bench-" + tag + "-");
        dir.toFile().deleteOnExit();
        return dir.toFile();
    }

    private static void deleteDir(File dir) {
        if (dir == null || !dir.exists()) return;
        try {
            Files.walk(dir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            // best effort
        }
    }

    // -------------------------------------------------------------------------
    // Output: table + CSV
    // -------------------------------------------------------------------------

    private static final String TABLE_HEADER =
            String.format("%-22s %7s %7s %10s %12s %14s %12s %12s %10s %10s %10s",
                    "workload", "scale", "threads",
                    "elapsed_ms", "ns_per_op", "ops_per_sec",
                    "rss_delta_kb", "gc_time_ms",
                    "read_kb", "write_kb", "disk_kb");

    private static void printTable(List<WorkloadResult> results) {
        System.out.println();
        System.out.println(TABLE_HEADER);
        System.out.println("-".repeat(TABLE_HEADER.length()));
        for (WorkloadResult r : results) {
            System.out.printf("%-22s %7d %7d %10.2f %12.1f %14.0f %12d %12d %10d %10d %10d%n",
                    r.workload, r.scale, r.threads,
                    r.elapsedMs, r.nsPerOp, r.opsPerSec,
                    r.rssDeltaKb, r.gcTimeMs,
                    r.readKb, r.writeKb, r.diskKb);
        }
        System.out.println();
    }

    private static void writeCsv(List<WorkloadResult> results, File outFile) throws IOException {
        outFile.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new FileWriter(outFile))) {
            pw.println("engine,workload,scale,threads,elapsed_ms,ns_per_op,ops_per_sec," +
                       "rss_delta_kb,read_kb,write_kb,disk_kb,gc_time_ms");
            for (WorkloadResult r : results) {
                pw.printf("je,%s,%d,%d,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d%n",
                        r.workload, r.scale, r.threads,
                        r.elapsedMs, r.nsPerOp, r.opsPerSec,
                        r.rssDeltaKb, r.readKb, r.writeKb, r.diskKb, r.gcTimeMs);
            }
        }
        System.out.println("CSV written to: " + outFile.getAbsolutePath());
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        // Recommended JVM flags for clean measurements:
        //   java -server -Xmx2g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=5
        //        -jar je-bench-1.0.0-jar-with-dependencies.jar

        int[] scales = {1_000, 10_000, 100_000};
        List<WorkloadResult> results = new ArrayList<>();

        // Determine output path relative to jar location or CWD
        File csvOut = new File("benches/results/je_results.csv");
        // If benches/results doesn't exist relative to CWD, try absolute from known location
        if (!csvOut.getParentFile().exists()) {
            // Try to find the workspace root by looking for a benches/ directory
            File cwd = new File(System.getProperty("user.dir"));
            File benchesDir = new File(cwd, "benches/results");
            if (!benchesDir.exists()) {
                // Fall back to a path next to the jar
                csvOut = new File(cwd, "je_results.csv");
            } else {
                csvOut = new File(benchesDir, "je_results.csv");
            }
        }

        System.out.println("=================================================");
        System.out.println("  JE Benchmark — Berkeley DB Java Edition 7.5.11");
        System.out.println("=================================================");

        for (int n : scales) {
            System.out.println("\n--- Scale: " + n + " ---");

            // ------------------------------------------------------------------
            // W01: seqWrite
            // ------------------------------------------------------------------
            {
                File dir = makeTempDir("w01");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                WorkloadResult r = measure("w01_seq_write", n, 1, dir, () -> seqWrite(db, n));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w01_seq_write       n=%-7d -> %.2f ms (%.0f ops/s)%n",
                        n, r.elapsedMs, r.opsPerSec);
            }

            // ------------------------------------------------------------------
            // W02: randWrite
            // ------------------------------------------------------------------
            {
                File dir = makeTempDir("w02");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                WorkloadResult r = measure("w02_rand_write", n, 1, dir, () -> randWrite(db, n));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w02_rand_write      n=%-7d -> %.2f ms (%.0f ops/s)%n",
                        n, r.elapsedMs, r.opsPerSec);
            }

            // ------------------------------------------------------------------
            // W03: seqRead  (pre-populate first)
            // ------------------------------------------------------------------
            {
                File dir = makeTempDir("w03");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w03_seq_read", n, 1, dir, () -> seqRead(db, n));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w03_seq_read        n=%-7d -> %.2f ms (%.0f ops/s)%n",
                        n, r.elapsedMs, r.opsPerSec);
            }

            // ------------------------------------------------------------------
            // W04: randRead  (pre-populate first)
            // ------------------------------------------------------------------
            {
                File dir = makeTempDir("w04");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w04_rand_read", n, 1, dir, () -> randRead(db, n));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w04_rand_read       n=%-7d -> %.2f ms (%.0f ops/s)%n",
                        n, r.elapsedMs, r.opsPerSec);
            }

            // ------------------------------------------------------------------
            // W05: rangeScan  (pre-populate first)
            // ------------------------------------------------------------------
            {
                File dir = makeTempDir("w05");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w05_range_scan", n, 1, dir, () -> rangeScan(db, n));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w05_range_scan      n=%-7d -> %.2f ms (%.0f ops/s)%n",
                        n, r.elapsedMs, r.opsPerSec);
            }

            // ------------------------------------------------------------------
            // W06: writeHeavy
            // ------------------------------------------------------------------
            {
                File dir = makeTempDir("w06");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                // Pre-populate so the reads in write-heavy have keys to find
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w06_write_heavy", n, 1, dir, () -> writeHeavy(db, n));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w06_write_heavy     n=%-7d -> %.2f ms (%.0f ops/s)%n",
                        n, r.elapsedMs, r.opsPerSec);
            }

            // ------------------------------------------------------------------
            // W07: readHeavy  (pre-populate first)
            // ------------------------------------------------------------------
            {
                File dir = makeTempDir("w07");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w07_read_heavy", n, 1, dir, () -> readHeavy(db, n));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w07_read_heavy      n=%-7d -> %.2f ms (%.0f ops/s)%n",
                        n, r.elapsedMs, r.opsPerSec);
            }

            // ------------------------------------------------------------------
            // W08: deleteInsert  (pre-populate first so deletes find existing records)
            // ------------------------------------------------------------------
            {
                File dir = makeTempDir("w08");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w08_delete_insert", n, 1, dir,
                        () -> deleteInsert(db, n));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w08_delete_insert   n=%-7d -> %.2f ms (%.0f ops/s)%n",
                        n, r.elapsedMs, r.opsPerSec);
            }

            // ------------------------------------------------------------------
            // W09: txnMulti  (pre-populate so gets have keys to read)
            // ------------------------------------------------------------------
            {
                File dir = makeTempDir("w09");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                final Environment fenv = env;
                WorkloadResult r = measure("w09_txn_multi", n, 1, dir,
                        () -> txnMulti(fenv, db, n));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w09_txn_multi       n=%-7d -> %.2f ms (%.0f ops/s)%n",
                        n, r.elapsedMs, r.opsPerSec);
            }

            // ------------------------------------------------------------------
            // W10: concurrent  (pre-populate so readers have keys to read)
            // ------------------------------------------------------------------
            {
                int readerThreads = 4;
                int writerThreads = 2;
                int totalThreads  = readerThreads + writerThreads;
                File dir = makeTempDir("w10");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w10_concurrent", n, totalThreads, dir,
                        () -> concurrent(db, n, readerThreads, writerThreads));
                db.close(); env.close();
                deleteDir(dir);
                results.add(r);
                System.out.printf("  w10_concurrent      n=%-7d -> %.2f ms (%.0f ops/s) [%d threads]%n",
                        n, r.elapsedMs, r.opsPerSec, totalThreads);
            }
        }

        // Print summary table
        printTable(results);

        // Write CSV
        writeCsv(results, csvOut);
    }
}
