package com.noxu.bench;

import java.io.*;
import java.lang.management.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class Metrics {
    /** Returns current process RSS in bytes (reads /proc/self/status on Linux). */
    public static long rssBytes() {
        try {
            List<String> lines = Files.readAllLines(Paths.get("/proc/self/status"));
            for (String line : lines) {
                if (line.startsWith("VmRSS:")) {
                    String[] parts = line.trim().split("\\s+");
                    return Long.parseLong(parts[1]) * 1024; // kB -> bytes
                }
            }
        } catch (Exception e) { /* fall through */ }
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    /** Returns (read_bytes, write_bytes) from /proc/self/io on Linux. */
    public static long[] procIo() {
        long readBytes = 0, writeBytes = 0;
        try {
            List<String> lines = Files.readAllLines(Paths.get("/proc/self/io"));
            for (String line : lines) {
                if (line.startsWith("read_bytes:"))
                    readBytes = Long.parseLong(line.split(":")[1].trim());
                else if (line.startsWith("write_bytes:"))
                    writeBytes = Long.parseLong(line.split(":")[1].trim());
            }
        } catch (Exception e) { /* ignore on non-Linux */ }
        return new long[]{readBytes, writeBytes};
    }

    /** Returns cumulative GC time in ms across all collectors. */
    public static long gcTimeMs() {
        long total = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long t = gc.getCollectionTime();
            if (t > 0) total += t;
        }
        return total;
    }

    /** Returns cumulative GC collection count across all collectors. */
    public static long gcCount() {
        long total = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long c = gc.getCollectionCount();
            if (c > 0) total += c;
        }
        return total;
    }

    /**
     * Returns JVM process CPU time in milliseconds.
     *
     * Uses com.sun.management.OperatingSystemMXBean.getProcessCpuTime() (nanoseconds)
     * when available on HotSpot, falling back to /proc/self/stat jiffies on Linux.
     */
    @SuppressWarnings("restriction")
    public static long cpuTimeMs() {
        try {
            com.sun.management.OperatingSystemMXBean osBean =
                (com.sun.management.OperatingSystemMXBean)
                    ManagementFactory.getOperatingSystemMXBean();
            long ns = osBean.getProcessCpuTime();
            if (ns > 0) return ns / 1_000_000;
        } catch (Exception ignored) {}

        // Fallback: parse /proc/self/stat fields 14+15 (utime+stime, jiffies, USER_HZ=100)
        try {
            String stat = new String(Files.readAllBytes(Paths.get("/proc/self/stat")));
            int closeParen = stat.lastIndexOf(')');
            if (closeParen > 0) {
                String[] fields = stat.substring(closeParen + 2).split("\\s+");
                // fields[11]=utime(14), fields[12]=stime(15) relative to closeParen+2
                if (fields.length >= 13) {
                    long utime = Long.parseLong(fields[11]);
                    long stime = Long.parseLong(fields[12]);
                    return (utime + stime) * 10; // jiffies → ms (USER_HZ=100)
                }
            }
        } catch (Exception ignored) {}
        return 0;
    }

    /** Recursively compute directory size in KB. */
    public static long dirSizeKb(Path dir) {
        try {
            long[] size = {0};
            Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    size[0] += attrs.size();
                    return FileVisitResult.CONTINUE;
                }
            });
            return size[0] / 1024;
        } catch (IOException e) {
            return 0;
        }
    }

    /**
     * Force GC and sleep to drain pending GC work before a timed section.
     *
     * Under EpsilonGC (-XX:+UseEpsilonGC) System.gc() is a no-op when combined
     * with -XX:+DisableExplicitGC, so this is safe in both modes.
     */
    public static void gcPause() {
        try {
            System.gc();
            System.gc();
            Thread.sleep(150);
        } catch (OutOfMemoryError | InterruptedException e) {
            if (e instanceof InterruptedException)
                Thread.currentThread().interrupt();
        }
    }
}
