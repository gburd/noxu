package com.noxu.bench;

import com.sleepycat.je.*;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;

public class EnvHelper {

    /** 64-byte fixed value used for all benchmark records. */
    public static final byte[] VALUE = new byte[64];

    static {
        Arrays.fill(VALUE, (byte) 0x42); // fill with 'B'
    }

    /** Returns a 10-digit zero-padded key as UTF-8 bytes, matching the Noxu key format. */
    public static byte[] makeKey(int i) {
        return String.format("%010d", i).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Opens (or creates) a JE Environment in the given directory.
     * Uses a 64 MB cache and transactional mode.
     */
    public static Environment openEnv(File dir) throws DatabaseException {
        dir.mkdirs();
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setCacheSize(64 * 1024 * 1024); // 64 MB
        return new Environment(dir, envConfig);
    }

    /**
     * Opens (or creates) the "bench" database within the given environment.
     */
    public static Database openDb(Environment env) throws DatabaseException {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        return env.openDatabase(null, "bench", dbConfig);
    }

    /**
     * Pre-populates the database with n sequential records (keys 0..n-1).
     * Uses auto-commit (null transaction) for speed.
     */
    public static void populate(Database db, int n) throws DatabaseException {
        DatabaseEntry data = new DatabaseEntry(VALUE);
        for (int i = 0; i < n; i++) {
            DatabaseEntry key = new DatabaseEntry(makeKey(i));
            db.put(null, key, data);
        }
    }

    /**
     * Returns the on-disk size of a directory in KB.
     * Delegates to Metrics.dirSizeKb.
     */
    public static long dirSizeKb(File dir) {
        return Metrics.dirSizeKb(dir.toPath());
    }
}
