//! Witness for **BUG-REP-DISPATCHER-LEAK**: `ReplicatedEnvironment::new`
//! starts a service-dispatcher accept thread bound to the replication port;
//! before the fix neither `close()` nor `Drop` stopped the *blocking*
//! `accept()`, so the thread, its listener fd, and the bound port leaked on
//! every close/drop.
//!
//! At stack-v1 this test is **RED**: either the accept-thread / fd counts grow
//! roughly one-per-cycle, or a same-port reopen comes up with no working
//! dispatcher (the old `new` swallowed the `EADDRINUSE` and set
//! `tcp_dispatcher = None`). After the fix it is **GREEN**: N open/close
//! cycles on a fixed port all bind successfully and the thread + fd counts are
//! flat (allowing small constant scheduling noise).
//!
//! Pure safe std; no TLS feature required (plain-TCP dispatcher is the default
//! path exercised by `ReplicatedEnvironment::new`).

use std::sync::Arc;

/// Count OS threads of this process (`/proc/self/task`).
fn thread_count() -> usize {
    std::fs::read_dir("/proc/self/task").map(|rd| rd.count()).unwrap_or(0)
}

/// Count open file descriptors of this process (`/proc/self/fd`).
fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").map(|rd| rd.count()).unwrap_or(0)
}

/// Count the named accept thread specifically, for precise attribution.
fn accept_thread_count() -> usize {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir("/proc/self/task") {
        for e in rd.flatten() {
            if let Ok(mut comm) = std::fs::read_to_string(e.path().join("comm"))
            {
                comm.truncate(comm.trim_end().len());
                if comm == "noxu-tcp-accept" || comm == "noxu-tls-accept" {
                    n += 1;
                }
            }
        }
    }
    n
}

fn fixed_port_cfg(dir: &std::path::Path, port: u16) -> noxu_rep::RepConfig {
    noxu_rep::RepConfig::builder("leak_grp", "n1", "127.0.0.1")
        .node_port(port)
        .env_home(dir.to_path_buf())
        .insecure_no_auth(true)
        .build()
}

/// Pick an ephemeral port, then release it so we can rebind it deterministically.
fn pick_free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind 0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

const CYCLES: usize = 20;

/// Serialize the two witnesses: both count *process-global* named accept
/// threads, so they must not run concurrently (a sibling's in-flight accept
/// thread would cross-count and flake the leak assertion). `cargo test` runs
/// the tests in this binary in parallel by default; this lock makes the
/// thread/fd sampling deterministic without forcing `--test-threads=1`.
static WITNESS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn dispatcher_does_not_leak_across_fixed_port_cycles() {
    let _serial = WITNESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let port = pick_free_port();

    // Warm-up cycle so one-time lazy allocations don't look like a leak.
    {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let env = noxu_rep::ReplicatedEnvironment::new(fixed_port_cfg(
            tmp.path(),
            port,
        ))
        .expect("warm new");
        // Prove the dispatcher actually bound the fixed port (not swallowed).
        assert!(
            env.bound_addr().is_some(),
            "warm-up: dispatcher must be bound on the fixed port"
        );
        env.close().expect("warm close");
        drop(env);
    }
    std::thread::sleep(std::time::Duration::from_millis(300));

    let threads_before = thread_count();
    let fds_before = fd_count();
    let accept_before = accept_thread_count();

    for i in 0..CYCLES {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let env = noxu_rep::ReplicatedEnvironment::new(fixed_port_cfg(
            tmp.path(),
            port,
        ))
        .unwrap_or_else(|e| panic!("cycle {i}: new failed: {e:?}"));

        // The reopen on the SAME fixed port must come up with a *working*
        // dispatcher. At stack-v1 a leaked prior accept thread still held the
        // port, so `new` silently bound nothing (`tcp_dispatcher = None`,
        // `bound_addr == None`) — this assertion is the RED trigger for the
        // "silent no-dispatcher" shape of the leak.
        assert!(
            env.bound_addr().is_some(),
            "cycle {i}: reopen on fixed port {port} produced NO bound \
             dispatcher — a leaked accept thread from a prior cycle is still \
             holding the port (BUG-REP-DISPATCHER-LEAK)"
        );

        env.close().expect("close");
        drop(env);
        // Brief settle so a cleanly-exiting thread/fd is reclaimed before the
        // next bind.
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    std::thread::sleep(std::time::Duration::from_millis(400));
    let threads_after = thread_count();
    let fds_after = fd_count();
    let accept_after = accept_thread_count();

    eprintln!(
        "dispatcher-leak witness: threads {threads_before}->{threads_after} \
         fds {fds_before}->{fds_after} accept {accept_before}->{accept_after} \
         over {CYCLES} fixed-port cycles"
    );

    // Flat within a small constant noise budget (a cleanly-exiting thread may
    // still be mid-teardown at the instant we sample). A per-cycle leak would
    // be ~CYCLES; the budget is far below that.
    let noise = 4usize;
    assert!(
        accept_after <= accept_before + 1,
        "accept-thread leak: {accept_before} -> {accept_after} over {CYCLES} \
         cycles (expected flat; a per-cycle leak is ~{CYCLES})"
    );
    assert!(
        threads_after <= threads_before + noise,
        "thread leak: {threads_before} -> {threads_after} over {CYCLES} \
         cycles (budget +{noise}; a per-cycle leak is ~{CYCLES})"
    );
    assert!(
        fds_after <= fds_before + noise,
        "fd leak: {fds_before} -> {fds_after} over {CYCLES} cycles \
         (budget +{noise}; a per-cycle leak is ~{CYCLES})"
    );
}

/// Same leak, but the env is only *dropped* (no explicit `close()`), covering
/// the `Drop` backstop. Uses a DIFFERENT fixed port per cycle so a leaked
/// thread (pre-fix) cannot block the next bind and mask the thread growth —
/// this isolates the raw per-cycle accept-thread leak that `Drop` must prevent.
///
/// F-A hardening: the earlier version sampled the accept-thread count only
/// once, after the whole loop, with a single fixed settle. Against a genuinely
/// *leaking* impl that was near-vacuous — the drop loop out-raced the OS
/// spawning/naming the accept threads, so the end-of-loop sample under-counted
/// the leak and the test passed ~11/13 times. This version samples after
/// **each** cycle and, crucially, **polls up to a bounded ~2 s for the named
/// accept-thread count to return to the pre-loop baseline** before moving on.
/// A clean `Drop` settles back to baseline within that window; a leaking
/// `Drop` never does, so the per-cycle settle times out with the dropped
/// env's thread still present — a deterministic RED. We assert that (a) after
/// every cycle the count settles back to the baseline (no dropped-env accept
/// thread survives), and (b) the cumulative count stays within a tiny constant
/// of the baseline across all cycles.
#[test]
fn dispatcher_does_not_leak_on_drop_without_close() {
    let _serial = WITNESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let base = pick_free_port();
    // Warm-up (one-time lazy allocations must not look like a leak).
    {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let env = noxu_rep::ReplicatedEnvironment::new(fixed_port_cfg(
            tmp.path(),
            base,
        ))
        .expect("warm new");
        drop(env);
    }
    // Settle the warm-up thread/fd out before we take the baseline.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let accept_baseline = accept_thread_count();
    let fds_baseline = fd_count();

    // A clean Drop joins the accept thread, so the named count returns to the
    // baseline within this bound. A leaking Drop never settles, so the poll
    // exhausts and we observe (and assert on) the surviving thread.
    const SETTLE: std::time::Duration = std::time::Duration::from_millis(2000);
    // Small constant for benign scheduling noise (a cleanly-exiting thread may
    // be momentarily mid-teardown when sampled). Far below a per-cycle leak.
    const SLACK: usize = 1;

    let mut did_cycle = false;
    let mut worst_accept = accept_baseline;
    let mut worst_fds = fds_baseline;

    for i in 0..CYCLES {
        // Distinct port each cycle (base is ephemeral; offsets are very likely
        // free on an otherwise-idle test host).
        let port = base.wrapping_add((i as u16).wrapping_add(1));
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let env = match noxu_rep::ReplicatedEnvironment::new(fixed_port_cfg(
            tmp.path(),
            port,
        )) {
            Ok(env) => env,
            // Port happened to be taken; skip this cycle (do not count it).
            Err(_) => continue,
        };
        did_cycle = true;

        // Give the OS a moment to actually spawn AND name the accept thread so
        // a leak cannot hide behind an un-named/not-yet-spawned thread at the
        // sample instant (this is the race that made the old end-of-loop
        // sample under-count). The named accept thread must be observable
        // while the env is alive.
        let saw_live_accept = {
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(1000);
            loop {
                if accept_thread_count() > accept_baseline {
                    break true;
                }
                if std::time::Instant::now() >= deadline {
                    break false;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        };

        // Plain drop — no close(). The dispatcher's Drop must stop and join
        // the accept thread, so the named count returns to the baseline.
        let env = Arc::new(env);
        drop(env);

        // Bounded poll for the dropped env's accept thread to go away. A clean
        // Drop joins it well within SETTLE; a leaking Drop never does.
        let settle_deadline = std::time::Instant::now() + SETTLE;
        let settled_accept = loop {
            let n = accept_thread_count();
            if n <= accept_baseline + SLACK {
                break n;
            }
            if std::time::Instant::now() >= settle_deadline {
                break n;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        let settled_fds = fd_count();
        worst_accept = worst_accept.max(settled_accept);
        worst_fds = worst_fds.max(settled_fds);

        // Deterministic per-cycle witness: after this drop, no accept thread
        // from the dropped env may survive the bounded settle. A leaking Drop
        // fails here on the first cycle it leaks.
        assert!(
            settled_accept <= accept_baseline + SLACK,
            "cycle {i}: a `noxu-tcp-accept`/`noxu-tls-accept` thread from the \
             dropped env SURVIVED the {SETTLE:?} settle (count \
             {accept_baseline}->{settled_accept}); Drop did not join the \
             accept thread (BUG-REP-DISPATCHER-LEAK). (live accept observed \
             this cycle: {saw_live_accept})"
        );
        assert!(
            settled_fds <= fds_baseline + 4,
            "cycle {i}: fd leak after drop ({fds_baseline}->{settled_fds}); \
             the dropped env's listener fd was not reclaimed"
        );
    }

    assert!(
        did_cycle,
        "no drop cycle ran (all ports were busy) — test proved nothing"
    );

    // Final settle + cumulative check: the worst per-cycle count across the
    // whole run must still be within SLACK of the baseline (a per-cycle leak
    // would be ~CYCLES).
    std::thread::sleep(std::time::Duration::from_millis(300));
    let accept_final = accept_thread_count();
    let fds_final = fd_count();
    eprintln!(
        "dispatcher-leak drop witness (F-A): accept baseline={accept_baseline} \
         worst={worst_accept} final={accept_final}; fds baseline={fds_baseline} \
         worst={worst_fds} final={fds_final} over {CYCLES} drop-only cycles"
    );
    assert!(
        worst_accept <= accept_baseline + SLACK,
        "accept-thread leak on drop (no close): worst per-cycle count \
         {worst_accept} exceeded baseline {accept_baseline}+{SLACK} over \
         {CYCLES} cycles (a per-cycle leak is ~{CYCLES})"
    );
    assert!(
        accept_final <= accept_baseline + SLACK,
        "accept-thread leak on drop (no close): final count {accept_final} \
         vs baseline {accept_baseline} over {CYCLES} cycles"
    );
    assert!(
        fds_final <= fds_baseline + 4,
        "fd leak on drop (no close): {fds_baseline} -> {fds_final} over \
         {CYCLES} cycles"
    );
}

/// TLS variant of the drop-only witness (F-A, mTLS dispatcher).
///
/// Opens a *TLS* `ReplicatedEnvironment` (`transport_kind = Tls` + an
/// in-process rcgen cert + a non-empty peer_allowlist — the same PKI pattern
/// the existing `rep_auth_default_test` / `peer_allowlist_tls_test` use, so no
/// external cert fixtures are needed) and drops it per cycle. This exercises
/// `TlsTcpServiceDispatcher::Drop` → `stop_and_join`: the self-connect wakeup
/// lands in rustls `accept()` (lazy handshake), the loop re-checks
/// `running == false` and drops the throwaway channel before any handshake
/// read, and the named `noxu-tls-accept` thread exits. Before the fix the TLS
/// dispatcher used a bare, un-joined `thread::spawn` (identical leak shape to
/// the plain-TCP path), so this is RED at the base and GREEN on the fix.
#[cfg(feature = "tls-rustls")]
#[test]
fn tls_dispatcher_does_not_leak_on_drop_without_close() {
    use noxu_rep::tls::{TlsConfig, TlsIdentity, TrustedCerts};
    use noxu_rep::{RepTransportKind, ReplicatedEnvironment};

    // One CA signs every node cert; SAN == server_name == the allowlisted name.
    let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.distinguished_name.push(rcgen::DnType::CommonName, "leak-tls-ca");
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_pem = ca_cert.pem().into_bytes();

    let make_tls = |name: &str| -> TlsConfig {
        let node_key = rcgen::KeyPair::generate().unwrap();
        let node_params =
            rcgen::CertificateParams::new(vec![name.to_string()]).unwrap();
        let node_cert =
            node_params.signed_by(&node_key, &ca_cert, &ca_key).unwrap();
        TlsConfig {
            identity: TlsIdentity::PemBytes {
                cert: node_cert.pem().into_bytes(),
                key: node_key.serialize_pem().into_bytes(),
            },
            trusted_certs: TrustedCerts::CaBytes(vec![ca_pem.clone()]),
            server_name: name.to_string(),
        }
    };
    let tls_cfg = |dir: &std::path::Path, port: u16| -> noxu_rep::RepConfig {
        noxu_rep::RepConfig::builder("leak_tls_grp", "n1", "127.0.0.1")
            .node_port(port)
            .env_home(dir.to_path_buf())
            .transport_kind(RepTransportKind::Tls)
            .tls_config(make_tls("n1.cluster"))
            .peer_allowlist(vec!["n1.cluster".to_string()])
            .insecure_no_auth(false)
            .build()
    };

    let _serial = WITNESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let base = pick_free_port();

    // Warm-up.
    {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let env = ReplicatedEnvironment::new(tls_cfg(tmp.path(), base))
            .expect("warm TLS new");
        assert!(
            env.bound_addr().is_some(),
            "warm-up: TLS dispatcher must be bound"
        );
        drop(env);
    }
    std::thread::sleep(std::time::Duration::from_millis(300));

    let accept_baseline = accept_thread_count();
    let fds_baseline = fd_count();

    const SETTLE: std::time::Duration = std::time::Duration::from_millis(2000);
    const SLACK: usize = 1;

    let mut did_cycle = false;
    let mut worst_accept = accept_baseline;

    for i in 0..CYCLES {
        let port = base.wrapping_add((i as u16).wrapping_add(1));
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let env = match ReplicatedEnvironment::new(tls_cfg(tmp.path(), port)) {
            Ok(env) => env,
            Err(_) => continue,
        };
        // Only count a cycle that actually bound a TLS dispatcher.
        if env.bound_addr().is_none() {
            drop(env);
            continue;
        }
        did_cycle = true;

        let saw_live_accept = {
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(1000);
            loop {
                if accept_thread_count() > accept_baseline {
                    break true;
                }
                if std::time::Instant::now() >= deadline {
                    break false;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        };

        // Plain drop — no close(). The TLS dispatcher's Drop must stop + join.
        drop(env);

        let settle_deadline = std::time::Instant::now() + SETTLE;
        let settled_accept = loop {
            let n = accept_thread_count();
            if n <= accept_baseline + SLACK {
                break n;
            }
            if std::time::Instant::now() >= settle_deadline {
                break n;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        let settled_fds = fd_count();
        worst_accept = worst_accept.max(settled_accept);

        assert!(
            settled_accept <= accept_baseline + SLACK,
            "cycle {i}: a `noxu-tls-accept` thread from the dropped TLS env \
             SURVIVED the {SETTLE:?} settle (count \
             {accept_baseline}->{settled_accept}); TLS Drop did not join the \
             accept thread (BUG-REP-DISPATCHER-LEAK). (live accept observed \
             this cycle: {saw_live_accept})"
        );
        assert!(
            settled_fds <= fds_baseline + 4,
            "cycle {i}: fd leak after TLS drop ({fds_baseline}->{settled_fds})"
        );
    }

    assert!(did_cycle, "no TLS drop cycle ran — test proved nothing");

    std::thread::sleep(std::time::Duration::from_millis(300));
    let accept_final = accept_thread_count();
    let fds_final = fd_count();
    eprintln!(
        "dispatcher-leak TLS drop witness (F-A): accept baseline={accept_baseline} \
         worst={worst_accept} final={accept_final}; fds baseline={fds_baseline} \
         final={fds_final} over {CYCLES} TLS drop-only cycles"
    );
    assert!(
        accept_final <= accept_baseline + SLACK,
        "TLS accept-thread leak on drop: final count {accept_final} vs \
         baseline {accept_baseline} over {CYCLES} cycles"
    );
    assert!(
        fds_final <= fds_baseline + 4,
        "TLS fd leak on drop: {fds_baseline} -> {fds_final} over {CYCLES} cycles"
    );
}
