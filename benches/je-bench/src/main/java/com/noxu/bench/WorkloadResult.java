package com.noxu.bench;

public class WorkloadResult {
    public final String workload;
    public final int scale;
    public final int threads;
    public final double elapsedMs;
    public final double nsPerOp;
    public final double opsPerSec;
    public final long rssDeltaKb;
    public final long gcTimeMs;
    public final long readKb;
    public final long writeKb;
    public final long diskKb;

    public WorkloadResult(String workload, int scale, int threads,
                          double elapsedMs, long ops,
                          long rssBefore, long rssAfter,
                          long gcTimeBefore, long gcTimeAfter,
                          long readBytesBefore, long readBytesAfter,
                          long writeBytesBefore, long writeBytesAfter,
                          long diskKb) {
        this.workload = workload;
        this.scale = scale;
        this.threads = threads;
        this.elapsedMs = elapsedMs;
        this.nsPerOp = ops > 0 ? (elapsedMs * 1e6) / ops : 0;
        this.opsPerSec = elapsedMs > 0 ? ops / (elapsedMs / 1000.0) : 0;
        this.rssDeltaKb = (rssAfter - rssBefore) / 1024;
        this.gcTimeMs = gcTimeAfter - gcTimeBefore;
        this.readKb = Math.max(0, readBytesAfter - readBytesBefore) / 1024;
        this.writeKb = Math.max(0, writeBytesAfter - writeBytesBefore) / 1024;
        this.diskKb = diskKb;
    }
}
