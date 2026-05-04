package com.noxu.bench;

// Run with (G1GC — recommended for general use):
//   java -server -Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=5 \
//        -XX:+AlwaysPreTouch -XX:+DisableExplicitGC \
//        -jar je-bench-jar-with-dependencies.jar
//
// Run with (EpsilonGC — zero GC interference, requires sufficient heap):
//   java -server -Xmx8g -Xms8g \
//        -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC \
//        -XX:+AlwaysPreTouch \
//        -jar je-bench-jar-with-dependencies.jar
//
// See run_comparison.sh which chooses EpsilonGC automatically when available.

import com.sleepycat.je.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class JeBenchmark {

    // -------------------------------------------------------------------------
    // Workload implementations (unchanged logic, all scales supported)
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
     */
    private static long writeHeavy(Database db, int n) throws DatabaseException {
        DatabaseEntry data = new DatabaseEntry(EnvHelper.VALUE);
        DatabaseEntry readData = new DatabaseEntry();
        for (int i = 0; i < n; i++) {
            DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(i % n));
            if (i % 10 == 9) {
                db.get(null, key, readData, LockMode.DEFAULT);
            } else {
                db.put(null, key, data);
            }
        }
        return n;
    }

    /**
     * W07: Read-heavy mixed workload — n ops total, cycling: 9 gets then 1 put.
     */
    private static long readHeavy(Database db, int n) throws DatabaseException {
        DatabaseEntry data = new DatabaseEntry(EnvHelper.VALUE);
        DatabaseEntry readData = new DatabaseEntry();
        for (int i = 0; i < n; i++) {
            DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(i % n));
            if (i % 10 == 9) {
                db.put(null, key, data);
            } else {
                db.get(null, key, readData, LockMode.DEFAULT);
            }
        }
        return n;
    }

    /**
     * W08: Delete-insert pairs — for each key i in 0..n: delete key_i then re-insert key_i.
     * Returns 2*n.
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
     * W09: Transactional multi-op — n transactions each: 3 gets + 2 puts + commit.
     * Returns 5*n.
     */
    private static long txnMulti(Environment env, Database db, int n) throws DatabaseException {
        Random rng = new Random(77);
        DatabaseEntry readData = new DatabaseEntry();
        DatabaseEntry writeData = new DatabaseEntry(EnvHelper.VALUE);

        for (int t = 0; t < n; t++) {
            Transaction txn = env.beginTransaction(null, null);
            try {
                for (int g = 0; g < 3; g++) {
                    int idx = rng.nextInt(n);
                    DatabaseEntry key = new DatabaseEntry(EnvHelper.makeKey(idx));
                    db.get(txn, key, readData, LockMode.DEFAULT);
                }
                for (int p = 0; p < 2; p++) {
                    int idx = n + t * 2 + p;
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
     * All threads start together via CyclicBarrier.  Returns total operations.
     *
     * @param readerThreads number of reader threads (0 = write-only)
     * @param writerThreads number of writer threads (0 = read-only)
     */
    private static long concurrent(Database db, int n, int readerThreads, int writerThreads)
            throws Exception {
        int totalThreads = readerThreads + writerThreads;
        if (totalThreads == 0) return 0;

        // +1 for main thread (acts as gate)
        CyclicBarrier barrier = new CyclicBarrier(totalThreads + 1);
        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);
        AtomicLong totalOps = new AtomicLong(0);

        // Divide ops evenly; each thread does at least 1 op
        int opsPerThread = Math.max(1, n / Math.max(1, totalThreads));

        List<Future<?>> futures = new ArrayList<>();

        for (int r = 0; r < readerThreads; r++) {
            final int seed = r;
            futures.add(pool.submit(() -> {
                try {
                    barrier.await();
                    Random rng = new Random(seed * 1_000_003L + 99);
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

        for (int w = 0; w < writerThreads; w++) {
            final int wi = w;
            futures.add(pool.submit(() -> {
                try {
                    barrier.await();
                    DatabaseEntry writeData = new DatabaseEntry(EnvHelper.VALUE);
                    long ops = 0;
                    // Writers own disjoint key ranges above the pre-populated range
                    for (int i = 0; i < opsPerThread; i++) {
                        int keyIdx = n + wi * opsPerThread + i;
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

        barrier.await(); // release all threads simultaneously

        for (Future<?> f : futures) f.get();
        pool.shutdown();
        pool.awaitTermination(120, TimeUnit.SECONDS);

        return totalOps.get();
    }

    // -------------------------------------------------------------------------
    // Measurement helpers
    // -------------------------------------------------------------------------

    @FunctionalInterface
    interface Workload {
        long run() throws Exception;
    }

    /**
     * Runs one timed workload measurement with before/after snapshots of:
     * RSS, GC time, GC count, CPU time, I/O bytes, and directory disk usage.
     *
     * If GC stole >5% of wall-clock time a warning is printed.
     */
    private static WorkloadResult measure(String name, int scale, int threads,
                                          File envDir, Workload workload) throws Exception {
        Metrics.gcPause();

        long rssBefore      = Metrics.rssBytes();
        long gcTimeBefore   = Metrics.gcTimeMs();
        long gcCountBefore  = Metrics.gcCount();
        long cpuTimeBefore  = Metrics.cpuTimeMs();
        long[] ioBefore     = Metrics.procIo();

        long startNs = System.nanoTime();
        long ops = workload.run();
        long endNs = System.nanoTime();

        long rssAfter       = Metrics.rssBytes();
        long gcTimeAfter    = Metrics.gcTimeMs();
        long gcCountAfter   = Metrics.gcCount();
        long cpuTimeAfter   = Metrics.cpuTimeMs();
        long[] ioAfter      = Metrics.procIo();
        long diskKb         = (envDir != null) ? EnvHelper.dirSizeKb(envDir) : 0;

        double elapsedMs = (endNs - startNs) / 1_000_000.0;

        WorkloadResult result = new WorkloadResult(
                name, scale, threads,
                elapsedMs, ops,
                rssBefore, rssAfter,
                gcTimeBefore, gcTimeAfter,
                gcCountBefore, gcCountAfter,
                cpuTimeBefore, cpuTimeAfter,
                ioBefore[0], ioAfter[0],
                ioBefore[1], ioAfter[1],
                diskKb
        );

        if (elapsedMs > 0 && result.gcTimeMs > elapsedMs * 0.05) {
            double pct = (result.gcTimeMs / elapsedMs) * 100.0;
            System.out.printf("  ⚠ GC stole %.1f%% of wall time for %s (GC collections: %d)%n",
                    pct, name, result.gcCount);
        }

        return result;
    }

    // -------------------------------------------------------------------------
    // Temp directory helpers
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
            String.format("%-26s %8s %7s %10s %12s %14s %10s %10s %9s %9s %9s %9s %9s",
                    "workload", "scale", "threads",
                    "elapsed_ms", "ns_per_op", "ops_per_sec",
                    "cpu_ms", "rss_dkb",
                    "gc_ms", "gc_n", "read_kb", "write_kb", "disk_kb");

    private static void printTable(List<WorkloadResult> results) {
        System.out.println();
        System.out.println(TABLE_HEADER);
        System.out.println("-".repeat(TABLE_HEADER.length()));
        for (WorkloadResult r : results) {
            System.out.printf(
                "%-26s %8d %7d %10.2f %12.1f %14.0f %10d %10d %9d %9d %9d %9d %9d%n",
                r.workload, r.scale, r.threads,
                r.elapsedMs, r.nsPerOp, r.opsPerSec,
                r.cpuTimeMs, r.rssDeltaKb,
                r.gcTimeMs, r.gcCount,
                r.readKb, r.writeKb, r.diskKb);
        }
        System.out.println();
    }

    private static void writeCsv(List<WorkloadResult> results, File outFile) throws IOException {
        outFile.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new java.io.FileWriter(outFile))) {
            pw.println("engine,workload,scale,threads,elapsed_ms,ns_per_op,ops_per_sec," +
                       "cpu_time_ms,rss_delta_kb,gc_time_ms,gc_count," +
                       "read_kb,write_kb,disk_kb,disk_bytes_per_op");
            for (WorkloadResult r : results) {
                pw.printf("je,%s,%d,%d,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d,%d,%d,%.2f%n",
                        r.workload, r.scale, r.threads,
                        r.elapsedMs, r.nsPerOp, r.opsPerSec,
                        r.cpuTimeMs, r.rssDeltaKb,
                        r.gcTimeMs, r.gcCount,
                        r.readKb, r.writeKb, r.diskKb,
                        r.diskBytesPerOp);
            }
        }
        System.out.println("CSV written to: " + outFile.getAbsolutePath());
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        // Scales: 1K, 10K, 100K, 500K, 1M
        int[] scales = {1_000, 10_000, 100_000, 500_000, 1_000_000};

        // W10 concurrent configurations: {readerThreads, writerThreads}
        // label → {readers, writers}
        int[][] concurrentConfigs = {
            {1, 0},   // read-only,  1 thread
            {0, 1},   // write-only, 1 thread
            {4, 0},   // read-only,  4 threads
            {0, 4},   // write-only, 4 threads
            {4, 4},   // mixed,      8 threads
            {8, 8},   // heavy,     16 threads
        };

        List<WorkloadResult> results = new ArrayList<>();

        // Resolve output CSV path
        File csvOut = resolveOutputPath("je_results.csv");

        System.out.println("=======================================================");
        System.out.println("  JE Benchmark — Berkeley DB Java Edition 7.5.11");
        System.out.println("  JVM: " + System.getProperty("java.vm.name") +
                           " " + System.getProperty("java.version"));
        System.out.println("=======================================================");

        for (int n : scales) {
            System.out.println("\n══ Scale: " + n + " ══");

            // W01: sequential write
            {
                File dir = makeTempDir("w01");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                WorkloadResult r = measure("w01_seq_write", n, 1, dir, () -> seqWrite(db, n));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress("w01_seq_write", n, 1, r);
            }

            // W02: random write
            {
                File dir = makeTempDir("w02");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                WorkloadResult r = measure("w02_rand_write", n, 1, dir, () -> randWrite(db, n));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress("w02_rand_write", n, 1, r);
            }

            // W03: sequential read
            {
                File dir = makeTempDir("w03");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w03_seq_read", n, 1, dir, () -> seqRead(db, n));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress("w03_seq_read", n, 1, r);
            }

            // W04: random read
            {
                File dir = makeTempDir("w04");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w04_rand_read", n, 1, dir, () -> randRead(db, n));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress("w04_rand_read", n, 1, r);
            }

            // W05: range scan
            {
                File dir = makeTempDir("w05");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w05_range_scan", n, 1, dir, () -> rangeScan(db, n));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress("w05_range_scan", n, 1, r);
            }

            // W06: write-heavy mixed (90% write / 10% read)
            {
                File dir = makeTempDir("w06");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w06_write_heavy", n, 1, dir, () -> writeHeavy(db, n));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress("w06_write_heavy", n, 1, r);
            }

            // W07: read-heavy mixed (90% read / 10% write)
            {
                File dir = makeTempDir("w07");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w07_read_heavy", n, 1, dir, () -> readHeavy(db, n));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress("w07_read_heavy", n, 1, r);
            }

            // W08: delete + insert pairs
            {
                File dir = makeTempDir("w08");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                WorkloadResult r = measure("w08_delete_insert", n, 1, dir,
                        () -> deleteInsert(db, n));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress("w08_delete_insert", n, 1, r);
            }

            // W09: transactional multi-op (3 gets + 2 puts per txn)
            {
                File dir = makeTempDir("w09");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, n);
                final Environment fenv = env;
                WorkloadResult r = measure("w09_txn_multi", n, 1, dir,
                        () -> txnMulti(fenv, db, n));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress("w09_txn_multi", n, 1, r);
            }

            // W10: concurrent — run all six thread configurations
            // Skip 500K/1M for write-only configs to keep runtime sane
            for (int[] cfg : concurrentConfigs) {
                int rthreads = cfg[0];
                int wthreads = cfg[1];
                int total    = rthreads + wthreads;
                String label = String.format("w10_conc_%dr%dw", rthreads, wthreads);

                // Limit ops at scale>100K to avoid very long runtimes
                int opsN = (n > 100_000 && wthreads > 4) ? 100_000 : n;

                File dir = makeTempDir("w10");
                Environment env = EnvHelper.openEnv(dir);
                Database db = EnvHelper.openDb(env);
                EnvHelper.populate(db, opsN);
                final int finalOpsN = opsN;
                WorkloadResult r = measure(label, n, total, dir,
                        () -> concurrent(db, finalOpsN, rthreads, wthreads));
                db.close(); env.close(); deleteDir(dir);
                results.add(r);
                printProgress(label, n, total, r);
            }
        }

        printTable(results);
        writeCsv(results, csvOut);
    }

    // -------------------------------------------------------------------------
    // Output helpers
    // -------------------------------------------------------------------------

    private static void printProgress(String name, int n, int threads, WorkloadResult r) {
        System.out.printf("  %-26s n=%-8d t=%d  %8.1f ms  %11.0f ops/s  cpu=%dms gc=%dms(%d)%n",
                name, n, threads, r.elapsedMs, r.opsPerSec, r.cpuTimeMs, r.gcTimeMs, r.gcCount);
    }

    private static File resolveOutputPath(String filename) {
        // Try relative path first (when run from workspace root)
        File f = new File("benches/results/" + filename);
        if (f.getParentFile().exists()) return f;

        // Try CWD
        File cwd = new File(System.getProperty("user.dir"));
        File benchesDir = new File(cwd, "benches/results");
        if (benchesDir.exists()) return new File(benchesDir, filename);

        return new File(cwd, filename);
    }
}
