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
#[test]
fn dispatcher_does_not_leak_on_drop_without_close() {
    let _serial = WITNESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let base = pick_free_port();
    // Warm-up.
    {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let env = noxu_rep::ReplicatedEnvironment::new(fixed_port_cfg(
            tmp.path(),
            base,
        ))
        .expect("warm new");
        drop(env);
    }
    std::thread::sleep(std::time::Duration::from_millis(300));

    let accept_before = accept_thread_count();
    let fds_before = fd_count();

    for i in 0..CYCLES {
        // Distinct port each cycle (base is ephemeral; offsets are very likely
        // free on an otherwise-idle test host).
        let port = base.wrapping_add((i as u16).wrapping_add(1));
        let tmp = tempfile::TempDir::new().expect("tempdir");
        match noxu_rep::ReplicatedEnvironment::new(fixed_port_cfg(
            tmp.path(),
            port,
        )) {
            Ok(env) => {
                // Plain drop — no close(). The dispatcher's Drop must stop and
                // join the accept thread.
                let env = Arc::new(env);
                drop(env);
            }
            Err(_) => { /* port happened to be taken; skip this cycle */ }
        }
        std::thread::sleep(std::time::Duration::from_millis(40));
    }

    std::thread::sleep(std::time::Duration::from_millis(500));
    let accept_after = accept_thread_count();
    let fds_after = fd_count();
    eprintln!(
        "dispatcher-leak drop witness: accept {accept_before}->{accept_after} \
         fds {fds_before}->{fds_after} over {CYCLES} drop-only cycles"
    );

    assert!(
        accept_after <= accept_before + 1,
        "accept-thread leak on drop (no close): {accept_before} -> \
         {accept_after} over {CYCLES} cycles (a per-cycle leak is ~{CYCLES})"
    );
    assert!(
        fds_after <= fds_before + 4,
        "fd leak on drop (no close): {fds_before} -> {fds_after} over \
         {CYCLES} cycles"
    );
}
