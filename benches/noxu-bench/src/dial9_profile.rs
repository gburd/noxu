//! In-process off-CPU + CPU profiling for xbench, using the dial9
//! `perf_event_open` self-profiler.
//!
//! Purpose: diagnose *where* xbench worker threads block (off-CPU) and spend
//! CPU, without external `perf`/gdb (which is flaky over SSM-SSH). Enabled by
//! `BENCH_PROFILE=cpu` (on-CPU sampling) or `BENCH_PROFILE=offcpu` (capture the
//! stack at each deschedule — reveals lock/condvar contention). Off by default.
//!
//! Build with frame pointers so the kernel callchain walk works:
//!   RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release ...
//! and ensure `perf_event_paranoid <= 2` (sudo sysctl kernel.perf_event_paranoid=1).
//!
//! On drop it prints the top aggregated stacks (folded, sample-count sorted) so
//! the dominant blocking site / CPU site is obvious.

#[cfg(target_os = "linux")]
mod imp {
    use dial9_perf_self_profile::{
        EventSource, PerfSampler, Sample, SamplerConfig, SamplingMode,
    };
    use std::collections::HashMap;

    pub struct Profiler {
        sampler: PerfSampler,
        mode: &'static str,
    }

    impl Profiler {
        /// Starts a profiler for the given mode, or returns `None` if disabled
        /// or if the sampler can't start (e.g. perf_event_paranoid too high).
        pub fn maybe_start(mode: &str) -> Option<Profiler> {
            let (source, sampling, label) = match mode {
                "cpu" => (
                    EventSource::SwCpuClock,
                    SamplingMode::FrequencyHz(997),
                    "cpu",
                ),
                // Capture the stack at each context switch — where threads BLOCK.
                "offcpu" => (
                    EventSource::SwContextSwitches,
                    // Every context switch (period 1) — off-CPU events are far
                    // rarer than CPU cycles, so we want them all.
                    SamplingMode::Period(1),
                    "offcpu",
                ),
                _ => return None,
            };
            let cfg = SamplerConfig::default()
                .event_source(source)
                .sampling(sampling);
            match PerfSampler::start(cfg) {
                Ok(sampler) => {
                    eprintln!("-- dial9 profiler active: mode={label} --");
                    Some(Profiler { sampler, mode: label })
                }
                Err(e) => {
                    eprintln!(
                        "-- dial9 profiler DISABLED ({label}): {e} \
                         (need perf_event_paranoid<=2 + frame pointers) --"
                    );
                    None
                }
            }
        }

        /// Drains samples, symbolizes, and prints the top folded stacks.
        pub fn report(&mut self, top_n: usize) {
            // Aggregate by folded callchain (leaf→root symbol names).
            let mut folded: HashMap<String, u64> = HashMap::new();
            let mut total: u64 = 0;
            self.sampler.for_each_sample(|s: &Sample| {
                total += 1;
                let stack = fold_stack(s);
                *folded.entry(stack).or_insert(0) += 1;
            });
            let mut rows: Vec<(String, u64)> = folded.into_iter().collect();
            rows.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
            eprintln!(
                "== dial9 {} profile: {} samples, top {} stacks ==",
                self.mode, total, top_n
            );
            for (stack, count) in rows.into_iter().take(top_n) {
                let pct = 100.0 * count as f64 / total.max(1) as f64;
                eprintln!("{count:8}  {pct:5.1}%  {stack}");
            }
            eprintln!("== end dial9 {} profile ==", self.mode);
        }
    }

    /// Fold a sample's callchain into a `a;b;c` string of resolved symbols
    /// (leaf first). Keeps only the top few frames to group by the meaningful
    /// blocking/CPU site rather than the full spawn prologue.
    fn fold_stack(s: &Sample) -> String {
        let mut names: Vec<String> = Vec::new();
        for &ip in s.callchain.iter().take(8) {
            let info = dial9_perf_self_profile::resolve_symbol(ip);
            match info.name {
                Some(name) => {
                    let n = shorten(&name);
                    if !n.is_empty() {
                        names.push(n);
                    }
                }
                None => names.push(format!("{ip:#x}")),
            }
        }
        if names.is_empty() {
            format!("{:#x}", s.ip)
        } else {
            names.join(";")
        }
    }

    /// Trim generics + hashes from a demangled Rust symbol for readable folding.
    fn shorten(name: &str) -> String {
        let n = name.split("::h").next().unwrap_or(name);
        // Drop generic args to group by function.
        let n = match n.find('<') {
            Some(i) => &n[..i],
            None => n,
        };
        n.trim_end_matches("::").to_string()
    }
}

#[cfg(target_os = "linux")]
pub use imp::Profiler;

/// No-op profiler on non-Linux platforms.
#[cfg(not(target_os = "linux"))]
pub struct Profiler;

#[cfg(not(target_os = "linux"))]
impl Profiler {
    pub fn maybe_start(_mode: &str) -> Option<Profiler> {
        None
    }
    pub fn report(&mut self, _top_n: usize) {}
}
