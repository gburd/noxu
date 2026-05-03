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
        // Fallback: JVM heap usage
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

    /** Force GC and wait briefly to reduce GC interference. Call before each timed section. */
    public static void gcPause() {
        System.gc();
        System.gc();
        try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
