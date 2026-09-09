//! Liveness stress for NoxuRawRwLock across mixed reader/writer populations.
//! Prints completed ops; a hang means a lost wakeup.
use lock_api::RawRwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

fn run(threads: usize, write_every: usize, secs: u64) {
    let lock = Arc::new(noxu_sync::NoxuRawRwLock::INIT);
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let mut hs = vec![];
    for t in 0..threads {
        let (l, s, o) =
            (Arc::clone(&lock), Arc::clone(&stop), Arc::clone(&ops));
        hs.push(std::thread::spawn(move || {
            let mut i = t;
            while !s.load(Ordering::Relaxed) {
                if write_every > 0 && i % write_every == 0 {
                    l.lock_exclusive();
                    unsafe { l.unlock_exclusive() };
                } else {
                    l.lock_shared();
                    unsafe { l.unlock_shared() };
                }
                i = i.wrapping_add(1);
                o.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.join().unwrap();
    }
    println!(
        "  t={threads:<3} write_every={write_every:<5} ops={}",
        ops.load(Ordering::Relaxed)
    );
}

fn main() {
    for (t, w) in
        [(2, 1), (4, 1024), (16, 32), (32, 64), (64, 1), (64, 1024), (64, 8)]
    {
        run(t, w, 3);
    }
    println!("all configurations completed (no lost wakeups)");
}
