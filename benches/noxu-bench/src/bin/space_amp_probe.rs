//! Space-amplification characterisation probe (see
//! docs/src/internal/space-amplification-2026-09.md for the full report).
//!
//! Loads a fixed keyspace, then runs a Zipfian UPDATE-STORM phase (updates,
//! not inserts) that keeps the live dataset size constant while generating a
//! steady stream of obsolete LN versions for the cleaner — the same shape
//! that produced the ~95 GB / 2.09x space-amp finding in the v7.5.4
//! cross-engine benchmark (`.agent/archived-audits/bench/v754-vs-wt-tidesdb-2026-07.md`).
//!
//! Reports, after driving the cleaner/checkpointer to a steady state:
//!   - `du -sb` on-disk bytes (ground truth; NOT the internal stat counters,
//!     several of which — CleanerStats.total_log_size/active_log_size/
//!     min_utilization/max_utilization/probe_runs — are never written by
//!     production code, only by unit tests; see the report).
//!   - `env.stats()` (log write/read bytes, cleaner run/deletion/migration
//!     counters, checkpoint counters — all of which ARE wired).
//!   - `noxu-admin print-log -S` per-entry-type byte histogram (the
//!     per-record-overhead / BIN-delta-accumulation breakdown), invoked as a
//!     subprocess so this binary does not need to link noxu-log's reader.
//!
//! Env vars (all optional, sensible defaults for a quick smoke run):
//!   SAP_DIR              on-disk env directory (must NOT be tmpfs)
//!   SAP_RECORDS           live keyspace size (default 2_000_000)
//!   SAP_VALUE             value size bytes (default 512)
//!   SAP_CACHE_MB          cache size in MiB (default 512)
//!   SAP_UPDATE_SECONDS    update-storm phase duration (default 120)
//!   SAP_THREADS           updater threads (default 16)
//!   SAP_MIN_UTIL          cleaner_min_utilization (default 50, JE default)
//!   SAP_CKPT_BYTES        checkpointer_bytes_interval (default 20_000_000)
//!   SAP_CKPT_MS           checkpointer_wakeup_interval_ms (default 30_000)
//!   SAP_DURABILITY        SYNC|NO_SYNC|WRITE_NO_SYNC (default NO_SYNC — the
//!                         phase is disk-bandwidth/cleaner-bound, not
//!                         fsync-bound; SYNC only changes wall-clock, not the
//!                         space outcome we are measuring)
//!   SAP_ADMIN_BIN         path to the noxu-admin binary for the print-log
//!                         breakdown (optional; skipped if unset/missing)
//!   SAP_FINAL_CLEAN_PASSES  max poll rounds (poll_secs=2s each) to wait for
//!                         the already-running daemons to drain the log to
//!                         steady state at the end (default 50 => ~100s cap)

use noxu_db::{
    DatabaseConfig, Durability, Environment, EnvironmentConfig,
    EnvironmentStats,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

fn envs(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}
fn envp(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn key_bytes(id: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&id.to_be_bytes());
    k[8..].copy_from_slice(&id.wrapping_mul(2654435761).to_be_bytes());
    k
}

/// Deterministic xorshift RNG — identical to `xbench.rs` so the key
/// sequences and Zipf shape match the cross-engine benchmark exactly.
struct Rng(u64);
impl Rng {
    #[inline]
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Zipfian generator (YCSB-standard, theta=0.99) — identical to `xbench.rs`.
struct Zipf {
    n: u64,
    theta: f64,
    zetan: f64,
    alpha: f64,
    eta: f64,
}
impl Zipf {
    fn new(n: u64) -> Self {
        let theta = 0.99;
        let zetan = Self::zeta(n, theta);
        let zeta2 = Self::zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta =
            (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Zipf { n, theta, zetan, alpha, eta }
    }
    fn zeta(n: u64, theta: f64) -> f64 {
        let mut s = 0.0;
        for i in 1..=n {
            s += 1.0 / (i as f64).powf(theta);
        }
        s
    }
    #[inline]
    fn next(&self, rng: &mut Rng) -> u64 {
        let u = (rng.next() as f64) / (u64::MAX as f64);
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        let v = (self.n as f64
            * (self.eta * u - self.eta + 1.0).powf(self.alpha))
            as u64;
        v % self.n
    }
}

fn fstype(path: &str) -> String {
    let out = std::process::Command::new("stat")
        .args(["-f", "-c", "%T", path])
        .output();
    match out {
        Ok(o) => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        Err(_) => String::new(),
    }
}

fn du_sb(path: &str) -> u64 {
    let out = std::process::Command::new("du")
        .args(["-sb", path])
        .output()
        .expect("run du");
    let s = String::from_utf8_lossy(&out.stdout);
    s.split_whitespace().next().and_then(|v| v.parse().ok()).unwrap_or(0)
}

fn main() {
    let dir = envs("SAP_DIR", "/tmp/noxu-space-amp-probe");
    let records = envp("SAP_RECORDS", 2_000_000);
    let value_size = envp("SAP_VALUE", 512) as usize;
    let cache_mb = envp("SAP_CACHE_MB", 512);
    let update_seconds = envp("SAP_UPDATE_SECONDS", 120);
    let threads = envp("SAP_THREADS", 16) as usize;
    let min_util = envp("SAP_MIN_UTIL", 50) as u8;
    let ckpt_bytes = envp("SAP_CKPT_BYTES", 20_000_000);
    let ckpt_ms = envp("SAP_CKPT_MS", 30_000);
    let durability = envs("SAP_DURABILITY", "NO_SYNC");
    let admin_bin = std::env::var("SAP_ADMIN_BIN").ok();
    let final_clean_passes = envp("SAP_FINAL_CLEAN_PASSES", 50);
    let seed = envp("SAP_SEED", 0xC0FFEE);

    if fstype(&dir).contains("tmpfs") {
        eprintln!("ABORT: {dir} is tmpfs; use real NVMe (/data/...)");
        std::process::exit(2);
    }
    let _ = std::fs::create_dir_all(&dir);

    let dur = match durability.as_str() {
        "SYNC" => Durability::COMMIT_SYNC,
        "WRITE_NO_SYNC" => Durability::COMMIT_WRITE_NO_SYNC,
        _ => Durability::COMMIT_NO_SYNC,
    };

    println!(
        "=== space_amp_probe: dir={dir} records={records} value={value_size} \
cache={cache_mb}MiB update_secs={update_seconds} threads={threads} \
min_util={min_util} ckpt_bytes={ckpt_bytes} ckpt_ms={ckpt_ms} dur={durability} ==="
    );

    let mut ecfg = EnvironmentConfig::new(std::path::PathBuf::from(&dir));
    ecfg.set_allow_create(true);
    ecfg.set_transactional(true);
    ecfg.set_cache_size(cache_mb * 1024 * 1024);
    ecfg.set_durability(dur);
    ecfg.set_cleaner_min_utilization(min_util);
    ecfg.set_checkpointer_bytes_interval(ckpt_bytes);
    ecfg.set_checkpointer_wakeup_interval_ms(ckpt_ms);

    let env = Arc::new(Environment::open(ecfg).expect("open env"));
    let db = Arc::new(
        env.open_database(
            None,
            "spaceamp",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db"),
    );

    // ── Load phase: fill the live keyspace once, batched, NO_SYNC-fast ──
    println!("-- loading {records} records ({value_size}B values) --");
    let lt = Instant::now();
    let load_threads = 8usize;
    let per = records / load_threads as u64;
    std::thread::scope(|s| {
        for tid in 0..load_threads {
            let env = Arc::clone(&env);
            let db = Arc::clone(&db);
            let start = tid as u64 * per;
            let end =
                if tid == load_threads - 1 { records } else { start + per };
            s.spawn(move || {
                let value = vec![0x5Au8; value_size];
                let mut i = start;
                while i < end {
                    let batch_end = (i + 1000).min(end);
                    if let Ok(txn) = env.begin_transaction(None) {
                        let mut ok = true;
                        for j in i..batch_end {
                            if db.put_in(&txn, key_bytes(j), &value).is_err() {
                                ok = false;
                                break;
                            }
                        }
                        if ok {
                            let _ = txn.commit();
                        } else {
                            let _ = txn.abort();
                        }
                    }
                    i = batch_end;
                }
            });
        }
    });
    env.checkpoint(None).unwrap();
    let load_secs = lt.elapsed().as_secs_f64();
    let du_after_load = du_sb(&dir);
    println!(
        "   loaded in {load_secs:.1}s, du_after_load={du_after_load} bytes ({:.2} GiB)",
        du_after_load as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    let user_bytes_live = records * value_size as u64;
    println!(
        "   live user bytes (records*value_size) = {user_bytes_live} ({:.2} GiB)",
        user_bytes_live as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    // ── Update-storm phase: Zipfian overwrite of the EXISTING keyspace.
    //    No new keys are inserted, so the live dataset size stays constant;
    //    every write obsoletes exactly one prior LN version, feeding the
    //    cleaner's garbage stream at a rate proportional to write throughput.
    println!("-- update-storm for {update_seconds}s ({threads} threads) --");
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let zipf = Arc::new(Zipf::new(records));
    let (log_wb0, log_rb0) = {
        let s = env.stats().unwrap();
        (s.log.n_sequential_write_bytes, s.log.n_sequential_read_bytes)
    };
    let ut0 = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|tid| {
            let env = Arc::clone(&env);
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let writes = Arc::clone(&writes);
            let zipf = Arc::clone(&zipf);
            std::thread::spawn(move || {
                let mut rng = Rng(seed ^ (tid as u64).wrapping_mul(0x9E3779B9));
                let value = vec![0x5Au8; value_size];
                let mut local = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let id = zipf.next(&mut rng);
                    let k = key_bytes(id);
                    if let Ok(t) = env.begin_transaction(None) {
                        if db.put_in(&t, k, &value).is_ok() {
                            if t.commit().is_ok() {
                                local += 1;
                            }
                        } else {
                            let _ = t.abort();
                        }
                    }
                    if local.is_multiple_of(4096) {
                        writes.fetch_add(4096, Ordering::Relaxed);
                    }
                }
                writes.fetch_add(local % 4096, Ordering::Relaxed);
            })
        })
        .collect();
    // Track peak du during the storm itself (Part 2 asks for both
    // steady-state-after-drain AND peak-during-storm; polling here avoids
    // relying on storm-end du as a proxy, in case a daemon happens to catch
    // up mid-storm and du dips before growing again).
    let mut peak_du_during_storm: u64 = du_sb(&dir);
    let poll_deadline = ut0 + std::time::Duration::from_secs(update_seconds);
    loop {
        let now = Instant::now();
        if now >= poll_deadline {
            break;
        }
        let remaining = poll_deadline - now;
        std::thread::sleep(remaining.min(std::time::Duration::from_secs(5)));
        peak_du_during_storm = peak_du_during_storm.max(du_sb(&dir));
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    peak_du_during_storm = peak_du_during_storm.max(du_sb(&dir));
    let update_elapsed = ut0.elapsed().as_secs_f64();
    let total_writes = writes.load(Ordering::Relaxed);
    println!(
        "   update-storm done: {total_writes} writes in {update_elapsed:.1}s ({:.0} writes/s)",
        total_writes as f64 / update_elapsed
    );

    // Snapshot BEFORE the final drain — this is what "just keeps running"
    // looks like at the moment the storm stops (closest to a live system
    // sampled at an arbitrary point), rather than after we've forced every
    // possible reclaim.
    let du_before_drain = du_sb(&dir);
    let s_before = env.stats().unwrap();
    println!(
        "   du BEFORE final drain = {du_before_drain} bytes ({:.2} GiB)",
        du_before_drain as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    print_cleaner_stats("before-drain", &s_before);
    print_decomposition("before-drain", &env, du_before_drain);

    // ── Drive the cleaner + checkpointer to steady state: alternate
    //    checkpoint() (makes cleaned files reclaimable) and clean_log()
    //    (forced pass) until no file is cleaned in a round, or the round
    //    budget is exhausted. This answers "how much of the du growth is
    //    reclaimable RIGHT NOW under the configured min_utilization, given
    //    enough checkpoints" vs. "how much is genuinely retained garbage
    //    under the floor".
    println!(
        "-- draining: waiting for the (already-running) checkpointer + \
         cleaner daemons to reach steady state --"
    );
    // The checkpointer and cleaner daemons (run_checkpointer / run_cleaner,
    // both on by default) have been running continuously since env open,
    // including through the update storm. Racing them with manual
    // env.checkpoint()/env.clean_log() calls is a methodological trap: both
    // take an exclusive in-progress flag, so a manual call issued while the
    // daemon holds it returns Err and -- with the original `let _ = ...` --
    // silently did nothing, understating how much draining had actually
    // happened (this is exactly what a first pass of this probe hit: du grew
    // through this section instead of shrinking). "Draining" here means:
    // stop generating new garbage and give the SAME daemons enough
    // wall-clock time to catch up, polling `du -sb` until it stops shrinking.
    let mut last_du = du_sb(&dir);
    let mut stable_polls = 0u64;
    let poll_secs = 2u64;
    let max_polls = final_clean_passes.max(30);
    let mut poll = 0u64;
    loop {
        poll += 1;
        std::thread::sleep(std::time::Duration::from_secs(poll_secs));
        // Best-effort manual nudge; ignored on Err (daemon race).
        let _ = env.checkpoint(None);
        let _ = env.clean_log();
        let du_now = du_sb(&dir);
        println!("   poll {poll}: du={du_now} bytes");
        if du_now >= last_du {
            stable_polls += 1;
        } else {
            stable_polls = 0;
        }
        last_du = du_now;
        if stable_polls >= 5 || poll >= max_polls {
            println!(
                "   stopping after {poll} polls ({})",
                if stable_polls >= 5 { "steady state" } else { "poll budget" }
            );
            break;
        }
    }

    let du_after_drain = du_sb(&dir);
    let s_after = env.stats().unwrap();
    let (log_wb1, log_rb1) = (
        s_after.log.n_sequential_write_bytes,
        s_after.log.n_sequential_read_bytes,
    );
    println!(
        "   du AFTER final drain = {du_after_drain} bytes ({:.2} GiB)",
        du_after_drain as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    print_cleaner_stats("after-drain", &s_after);
    print_decomposition("after-drain", &env, du_after_drain);

    let phase_write_bytes = log_wb1.saturating_sub(log_wb0);
    let phase_read_bytes = log_rb1.saturating_sub(log_rb0);
    let write_amp_phase = if total_writes > 0 {
        phase_write_bytes as f64 / (total_writes * value_size as u64) as f64
    } else {
        0.0
    };

    println!(
        "RESULT du_after_load={du_after_load} du_before_drain={du_before_drain} du_after_drain={du_after_drain} \
peak_du_during_storm={peak_du_during_storm} \
user_bytes_live={user_bytes_live} space_amp_before_drain={:.4} space_amp_after_drain={:.4} space_amp_peak={:.4} \
update_storm_writes={total_writes} update_storm_secs={update_elapsed:.1} \
phase_log_write_bytes={phase_write_bytes} phase_log_read_bytes={phase_read_bytes} write_amp_phase={write_amp_phase:.4} \
cleaner_runs={} cleaner_deletions={} cleaner_lns_cleaned={} cleaner_lns_migrated={} cleaner_lns_dead={} cleaner_lns_obsolete={} \
cleaner_ins_cleaned={} cleaner_ins_migrated={} cleaner_ins_dead={} \
bin_deltas_cleaned={} bin_deltas_migrated={} bin_deltas_dead={} bin_deltas_obsolete={} \
checkpoints={} full_in_flush={} full_bin_flush={} delta_in_flush={}",
        du_before_drain as f64 / user_bytes_live as f64,
        du_after_drain as f64 / user_bytes_live as f64,
        peak_du_during_storm as f64 / user_bytes_live as f64,
        s_after.cleaner.runs,
        s_after.cleaner.deletions,
        s_after.cleaner.lns_cleaned,
        s_after.cleaner.lns_migrated,
        s_after.cleaner.lns_dead,
        s_after.cleaner.lns_obsolete,
        s_after.cleaner.ins_cleaned,
        s_after.cleaner.ins_migrated,
        s_after.cleaner.ins_dead,
        s_after.cleaner.bin_deltas_cleaned,
        s_after.cleaner.bin_deltas_migrated,
        s_after.cleaner.bin_deltas_dead,
        s_after.cleaner.bin_deltas_obsolete,
        s_after.checkpoint.checkpoints,
        s_after.checkpoint.full_in_flush,
        s_after.checkpoint.full_bin_flush,
        s_after.checkpoint.delta_in_flush,
    );

    db.close().unwrap();
    drop(db);
    if let Ok(e) = Arc::try_unwrap(env) {
        e.close().unwrap();
    }

    // ── Per-entry-type byte breakdown from the raw log (BIN-delta
    //    accumulation, LN header overhead, obsolete-vs-live not
    //    distinguishable here — this is raw bytes on disk by type, which is
    //    what remains after the drain above). Invoked as a subprocess against
    //    the closed env directory so it never races the engine's own file
    //    handles.
    if let Some(admin) = admin_bin {
        println!("-- noxu-admin print-log -S (post-drain byte breakdown) --");
        match std::process::Command::new(&admin)
            .args(["print-log", "-h", &dir, "-S"])
            .output()
        {
            Ok(out) => {
                print!("{}", String::from_utf8_lossy(&out.stdout));
                if !out.stderr.is_empty() {
                    eprint!("{}", String::from_utf8_lossy(&out.stderr));
                }
            }
            Err(e) => eprintln!("   (skipped: {e})"),
        }
    } else {
        println!(
            "-- SAP_ADMIN_BIN not set; skipping print-log byte breakdown --"
        );
    }
}

fn print_cleaner_stats(label: &str, s: &EnvironmentStats) {
    println!(
        "   [{label}] cleaner: runs={} deletions={} lns_cleaned={} lns_migrated={} lns_dead={} lns_obsolete={} \
ins_cleaned={} ins_migrated={} ins_dead={} bin_deltas_cleaned={} bin_deltas_migrated={} \
pending_ln_queue_size={} | checkpoint: checkpoints={} full_in_flush={} full_bin_flush={} delta_in_flush={} | \
log: n_sequential_write_bytes={} n_sequential_read_bytes={} n_random_reads={}",
        s.cleaner.runs,
        s.cleaner.deletions,
        s.cleaner.lns_cleaned,
        s.cleaner.lns_migrated,
        s.cleaner.lns_dead,
        s.cleaner.lns_obsolete,
        s.cleaner.ins_cleaned,
        s.cleaner.ins_migrated,
        s.cleaner.ins_dead,
        s.cleaner.bin_deltas_cleaned,
        s.cleaner.bin_deltas_migrated,
        s.cleaner.pending_ln_queue_size,
        s.checkpoint.checkpoints,
        s.checkpoint.full_in_flush,
        s.checkpoint.full_bin_flush,
        s.checkpoint.delta_in_flush,
        s.log.n_sequential_write_bytes,
        s.log.n_sequential_read_bytes,
        s.log.n_random_reads,
    );
}

/// Part 1 decomposition (space-amp Phase 2): attribute the `du_bytes`
/// on-disk figure across the file-selector pipeline states plus a
/// below-the-floor / above-the-floor split of the merged utilization
/// summary map. `None` (skipped, printed as a note) if this environment
/// exposes no cleaner (e.g. read-only).
///
/// Buckets (mutually exclusive by construction — each file is in exactly
/// one FileSelector pipeline state, or untracked):
///   - `below_floor_bytes`: total bytes of files whose summary utilization
///     is already below `min_utilization` (garbage the cleaner is entitled
///     to reclaim right now, whether or not it already has).
///   - `backlog_to_be_cleaned` / `being_cleaned`: FileSelector queue depth
///     — files the cleaner knows must be cleaned but hasn't gotten to yet
///     (a genuine backlog under write pressure) vs. mid-clean.
///   - `cleaned` / `checkpointed`: cleaned but not yet past the
///     two-checkpoint deletion barrier (checkpoint-interval lag).
///   - `above_floor_bytes`: the rest — files at or above `min_utilization`,
///     not queued for cleaning, i.e. genuinely active/live data plus
///     per-record overhead.
fn print_decomposition(label: &str, env: &Environment, du_bytes: u64) {
    let Some(diag) = env.cleaner_diagnostics() else {
        println!(
            "   [{label}] decomposition: skipped (no cleaner on this env)"
        );
        return;
    };
    let min_util = diag.min_utilization as f64 / 100.0;
    let mut below_floor_bytes: i64 = 0;
    let mut above_floor_bytes: i64 = 0;
    let mut below_floor_files = 0u64;
    let mut above_floor_files = 0u64;
    for summary in diag.file_summaries.values() {
        if summary.total_size <= 0 {
            continue;
        }
        if summary.get_utilization() < min_util {
            below_floor_bytes += summary.total_size as i64;
            below_floor_files += 1;
        } else {
            above_floor_bytes += summary.total_size as i64;
            above_floor_files += 1;
        }
    }
    let fs = diag.file_selector;
    println!(
        "   [{label}] decomposition (min_utilization={}%, du={du_bytes}B): \
below_floor={below_floor_bytes}B/{below_floor_files}files above_floor={above_floor_bytes}B/{above_floor_files}files | \
file_selector: to_be_cleaned={} being_cleaned={} cleaned={} checkpointed={} safe_to_delete={}",
        diag.min_utilization,
        fs.to_be_cleaned,
        fs.being_cleaned,
        fs.cleaned,
        fs.checkpointed,
        fs.safe_to_delete,
    );
}
