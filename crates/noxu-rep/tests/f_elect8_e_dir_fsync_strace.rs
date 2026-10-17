// F-ELECT8-E crash-consistency witness (Linux, strace-based).
//
// The acceptor persists its promise with write + `sync_all` + `rename`. For the
// rename to survive a crash the PARENT DIRECTORY must be fsynced AFTER the
// rename (POSIX). This test proves that ordering at the syscall level without a
// FUSE layer: it re-execs itself under `strace` to perform exactly one
// persisting `try_promise`, then parses the trace and asserts that an
// `fsync`/`fdatasync` of the state file's parent directory follows the
// `rename`/`renameat` of `acceptor.state`.
//
// Feasibility: this runs ONLY on Linux when `strace` is on PATH (checked at
// runtime). Elsewhere it prints a SKIP line and passes — the deterministic
// in-crate unit test `promise_dir_fsync_follows_rename_witness` is the portable
// RED/GREEN witness; this strace check is the extra crash-consistency teeth on
// Linux CI where strace is present.

use std::path::Path;
use std::process::Command;

const PROBE_ENV: &str = "ACCDIR_FSYNC_PROBE_DIR";

// When re-exec'd with the probe env set, do the one persisting promise and
// exit(0) before the normal libtest harness runs. A ctor would be cleaner but
// is not available on stable; running this at the top of the single test fn is
// equivalent because the child is invoked with `--exact` on this test only.
fn maybe_run_probe_child() {
    if let Ok(dir) = std::env::var(PROBE_ENV) {
        use noxu_rep::elections::acceptor_state::PersistentAcceptorState;
        let st = PersistentAcceptorState::load_or_default(Path::new(&dir));
        assert!(
            st.try_promise_proposal(5, "node-a"),
            "probe promise must persist"
        );
        std::process::exit(0);
    }
}

fn strace_available() -> bool {
    Command::new("strace")
        .arg("-V")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Parse an strace line-trace and assert a directory fsync follows the rename
/// of the state file. Returns Ok(()) on success, Err(reason) on violation.
fn assert_dir_fsync_after_rename(
    trace: &str,
    state_file: &str,
    dir: &str,
) -> Result<(), String> {
    let lines: Vec<&str> = trace.lines().collect();

    // 1) Find the rename/renameat whose destination is the state file.
    let rename_idx = lines.iter().position(|l| {
        (l.contains("rename(")
            || l.contains("renameat(")
            || l.contains("renameat2("))
            && l.contains(state_file)
    });
    let rename_idx = match rename_idx {
        Some(i) => i,
        None => {
            return Err(format!(
                "no rename of state file {state_file:?} found in trace"
            ));
        }
    };

    // 2) After the rename, find an openat of the parent directory that returns
    //    a fd, then an fsync/fdatasync of that same fd.
    //    openat(AT_FDCWD, "<dir>", O_RDONLY...) = <fd>
    let dir_pat_a = format!("\"{dir}\", O_RDONLY");
    let dir_pat_b = format!("\"{dir}/\", O_RDONLY"); // tolerate trailing slash
    for l in &lines[rename_idx + 1..] {
        let is_dir_open = (l.contains("openat(") || l.contains("open("))
            && (l.contains(&dir_pat_a) || l.contains(&dir_pat_b));
        if is_dir_open {
            // Extract the returned fd: "... = <fd>"
            if let Some(eq) = l.rfind("= ") {
                let fd: String = l[eq + 2..]
                    .trim()
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                if fd.is_empty() {
                    continue;
                }
                // Now find fsync(fd) / fdatasync(fd) after this open, before the
                // dir fd is closed.
                let open_pos =
                    lines.iter().position(|x| std::ptr::eq(*x, *l)).unwrap();
                // With `strace -y` an fd appears as `3` or `3<path>`; match
                // both the bare and annotated forms.
                let fsync_bare = format!("fsync({fd})");
                let fsync_ann = format!("fsync({fd}<");
                let fdatasync_bare = format!("fdatasync({fd})");
                let fdatasync_ann = format!("fdatasync({fd}<");
                let close_bare = format!("close({fd})");
                let close_ann = format!("close({fd}<");
                for l2 in &lines[open_pos + 1..] {
                    if l2.contains(&close_bare) || l2.contains(&close_ann) {
                        break;
                    }
                    if l2.contains(&fsync_bare)
                        || l2.contains(&fsync_ann)
                        || l2.contains(&fdatasync_bare)
                        || l2.contains(&fdatasync_ann)
                    {
                        return Ok(());
                    }
                }
            }
        }
    }
    Err(format!(
        "found rename of {state_file:?} but no subsequent fsync/fdatasync of \
         the parent directory {dir:?} (the F-ELECT8-E defect would look like this)"
    ))
}

#[test]
fn dir_fsync_follows_rename_strace() {
    // Child path: do the persisting promise and exit before libtest proceeds.
    maybe_run_probe_child();

    if !cfg!(target_os = "linux") {
        eprintln!("SKIP dir_fsync_follows_rename_strace: not Linux");
        return;
    }
    if !strace_available() {
        eprintln!("SKIP dir_fsync_follows_rename_strace: strace not on PATH");
        return;
    }

    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path().to_str().unwrap().to_string();
    let state_file = format!("{dir}/acceptor.state");
    let trace_path = tmp.path().join("trace.txt");

    // Re-exec THIS test binary, running only this test, under strace. The env
    // var makes the child perform the probe workload and exit(0) immediately.
    let self_exe = std::env::current_exe().expect("current_exe");
    let status = Command::new("strace")
        .args([
            "-f", // follow threads/forks
            "-y", // annotate fds with paths (robustness; parser also works without)
            "-e",
            "trace=rename,renameat,renameat2,fsync,fdatasync,openat,open,close",
            "-o",
        ])
        .arg(&trace_path)
        .arg(&self_exe)
        .args(["--exact", "dir_fsync_follows_rename_strace", "--nocapture"])
        .env(PROBE_ENV, &dir)
        .status()
        .expect("spawn strace");
    assert!(status.success(), "strace child exited non-zero: {status:?}");

    let trace = std::fs::read_to_string(&trace_path).expect("read trace");
    assert!(
        Path::new(&state_file).exists(),
        "probe did not create {state_file}"
    );

    match assert_dir_fsync_after_rename(&trace, &state_file, &dir) {
        Ok(()) => {}
        Err(reason) => {
            // Dump the relevant trace tail to aid diagnosis.
            let tail: String = trace
                .lines()
                .filter(|l| {
                    l.contains("rename")
                        || l.contains("fsync")
                        || l.contains("fdatasync")
                        || l.contains("acceptor.state")
                })
                .collect::<Vec<_>>()
                .join("\n");
            panic!(
                "F-ELECT8-E strace crash-consistency check failed: {reason}\n--- trace excerpt ---\n{tail}"
            );
        }
    }
}
