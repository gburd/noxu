package com.noxu.bench;

public class WorkloadResult {
    public final String workload;
    public final int scale;
    public final int threads;
    public final double elapsedMs;
    public final double nsPerOp;
    public final double opsPerSec;
    public final long rssDeltaKb;
    /** Wall-clock ms consumed by GC during this workload. */
    public final long gcTimeMs;
    /** Number of GC collections that fired during this workload. */
    public final long gcCount;
    /** CPU time (user+sys) consumed during this workload, in ms. */
    public final long cpuTimeMs;
    public final long readKb;
    public final long writeKb;
    public final long diskKb;
    /** On-disk bytes written per logical operation (diskKb*1024/ops). */
    public final double diskBytesPerOp;
    /** Number of fdatasync/fsync calls during this workload (port of JE nFSyncs stat). */
    public final long fsyncCount;

    public WorkloadResult(String workload, int scale, int threads,
                          double elapsedMs, long ops,
                          long rssBefore, long rssAfter,
                          long gcTimeBefore, long gcTimeAfter,
                          long gcCountBefore, long gcCountAfter,
                          long cpuTimeBefore, long cpuTimeAfter,
                          long readBytesBefore, long readBytesAfter,
                          long writeBytesBefore, long writeBytesAfter,
                          long diskKb,
                          long fsyncsBefore, long fsyncsAfter) {
        this.workload = workload;
        this.scale = scale;
        this.threads = threads;
        this.elapsedMs = elapsedMs;
        this.nsPerOp = ops > 0 ? (elapsedMs * 1e6) / ops : 0;
        this.opsPerSec = elapsedMs > 0 ? ops / (elapsedMs / 1000.0) : 0;
        this.rssDeltaKb = (rssAfter - rssBefore) / 1024;
        this.gcTimeMs = Math.max(0, gcTimeAfter - gcTimeBefore);
        this.gcCount = Math.max(0, gcCountAfter - gcCountBefore);
        this.cpuTimeMs = Math.max(0, cpuTimeAfter - cpuTimeBefore);
        this.readKb = Math.max(0, readBytesAfter - readBytesBefore) / 1024;
        this.writeKb = Math.max(0, writeBytesAfter - writeBytesBefore) / 1024;
        this.diskKb = diskKb;
        this.diskBytesPerOp = ops > 0 ? (diskKb * 1024.0) / ops : 0;
        this.fsyncCount = Math.max(0, fsyncsAfter - fsyncsBefore);
    }
}
