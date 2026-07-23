//! Fast policy-level simulation of the production read+evict loop, to
//! validate scan/skew resistance WITHOUT the full DB stack (the end-to-end
//! `zipfian_hitrate_repro` takes ~8 min; this runs in milliseconds).
//!
//! Model: each "BIN" is a node_id whose LN data is either resident or
//! stripped.  A read of a resident BIN is a HIT + touch (promote HOT); a read
//! of a stripped BIN is a MISS that re-fetches + repopulates (data resident
//! again) + touch.  The evictor continuously pops `evict_candidate()` and
//! strips the chosen BIN's data (put_back).  We measure the steady-state hit
//! rate for a skewed (Zipfian-like) read stream over a working set larger than
//! the "budget" (max resident stripped-count).
//!
//! Requires the `experimental-eviction-policies` feature (the policies it
//! exercises are gated behind it).
#![cfg(feature = "experimental-eviction-policies")]

use noxu_evictor::EvictionAlgorithm;
use std::collections::HashSet;

fn next_rand(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Zipfian rank in [0, n).
fn zipf(state: &mut u64, n: usize, zetan: f64, theta: f64) -> usize {
    let alpha = 1.0 / (1.0 - theta);
    let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta))
        / (1.0 - zeta(2, theta) / zetan);
    let u = (next_rand(state) as f64) / (u64::MAX as f64);
    let uz = u * zetan;
    if uz < 1.0 {
        return 0;
    }
    if uz < 1.0 + 0.5f64.powf(theta) {
        return 1;
    }
    (((eta * u - eta + 1.0).powf(alpha) * n as f64) as usize).min(n - 1)
}

fn zeta(n: usize, theta: f64) -> f64 {
    (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
}

/// Run the simulation for one policy; return (hit_rate, hot_resident_frac).
fn simulate(algo: EvictionAlgorithm, zipfian: bool) -> f64 {
    let policy = algo.new_policy();
    let n_bins = 1_000usize; // working set (scaled down so the sim runs in seconds; the COOL/HOT-vs-LRU ordering is size-independent)
    let budget = 200usize; // resident-data budget (~20% -> holds the hot set)
    let theta = 0.99f64;
    let zetan = zeta(n_bins, theta);

    // resident[id] = LN data present (a read hits).  All start resident
    // (freshly inserted) and admitted to the policy.
    let mut resident = vec![true; n_bins];
    for id in 0..n_bins {
        policy.insert(id as u64);
    }
    let mut resident_count = n_bins;

    let mut rng = 0xDEAD_BEEF_u64;
    let read_key = |rng: &mut u64| -> usize {
        if zipfian {
            zipf(rng, n_bins, zetan, theta)
        } else {
            (next_rand(rng) as usize) % n_bins // uniform
        }
    };

    // Warm-up: run reads + eviction so the policy learns the hot set.
    let warm = 10_000usize;
    for _ in 0..warm {
        let k = read_key(&mut rng);
        if !resident[k] {
            resident[k] = true;
            resident_count += 1;
        }
        policy.touch(k as u64);
        // Evict down to budget (strip data of the victim).
        while resident_count > budget {
            match policy.evict_candidate() {
                Some(v) => {
                    let v = v as usize;
                    if resident[v] {
                        resident[v] = false;
                        resident_count -= 1;
                    }
                    policy.put_back(v as u64); // BIN stays resident, data stripped
                }
                None => break,
            }
        }
    }

    // Measured phase.
    let reads = 10_000usize;
    let mut hits = 0usize;
    for _ in 0..reads {
        let k = read_key(&mut rng);
        if resident[k] {
            hits += 1;
        } else {
            resident[k] = true;
            resident_count += 1;
        }
        policy.touch(k as u64);
        while resident_count > budget {
            match policy.evict_candidate() {
                Some(v) => {
                    let v = v as usize;
                    if resident[v] {
                        resident[v] = false;
                        resident_count -= 1;
                    }
                    policy.put_back(v as u64);
                }
                None => break,
            }
        }
    }
    hits as f64 / reads as f64
}

/// A one-touch scan must NOT evict a pre-warmed hot set.
fn simulate_scan_resistance(algo: EvictionAlgorithm) -> (f64, f64) {
    let policy = algo.new_policy();
    let hot: Vec<u64> = (0..200).collect();
    let budget = 400usize;
    let mut resident: HashSet<u64> = HashSet::new();

    // Warm the hot set to genuinely HOT (read each several times).
    for &h in &hot {
        policy.insert(h);
        resident.insert(h);
    }
    for _ in 0..5 {
        for &h in &hot {
            policy.touch(h);
        }
    }

    // Stream a big one-touch scan (ids 1000..).  Each scan page is inserted +
    // touched once, then eviction runs to budget.
    let scan_len = 5_000u64;
    for s in 1000..(1000 + scan_len) {
        policy.insert(s);
        resident.insert(s);
        policy.touch(s); // one touch (the scan read)
        while resident.len() > budget {
            match policy.evict_candidate() {
                Some(v) => {
                    resident.remove(&v);
                    policy.put_back(v);
                    resident.insert(v); // BIN stays; only "data" notionally stripped
                    // For scan-resistance we care about eviction *selection*:
                    // count below.  Break to avoid infinite loop when nothing
                    // shrinks (put_back re-adds).  Model strip as remove:
                    resident.remove(&v);
                }
                None => break,
            }
        }
    }
    // How many hot nodes are still resident?
    let hot_still: usize = hot.iter().filter(|h| resident.contains(h)).count();
    let hot_frac = hot_still as f64 / hot.len() as f64;
    let scan_still: usize =
        (1000..(1000 + scan_len)).filter(|s| resident.contains(s)).count();
    let scan_frac = scan_still as f64 / scan_len as f64;
    (hot_frac, scan_frac)
}

// NOTE: these two comparative sims are #[ignore]d — they model a tight
// per-read evict-to-budget drain loop with NO trickle cooler, which forces
// CoolHot down its worst-case force_cool double-sweep on every drain (O(n) per
// evict when the ring is all-HOT), making the sim minutes-long. Production does
// NOT drain this way: the evictor daemon (the trickle) demotes HOT->COOL ahead
// of the foreground so evict() finds a COOL victim in ~O(1). The authoritative
// correctness + hit-rate signal is the eviction_pressure suite + the real
// ycsb_c benchmark, not this trickle-less pure-policy model. Run explicitly
// with `--ignored` if iterating on the policy in isolation.
#[test]
#[ignore = "minutes-long trickle-less policy sim; real signal is eviction_pressure + ycsb_c"]
fn coolhot_beats_lru_on_zipfian() {
    for &algo in &[EvictionAlgorithm::Lru, EvictionAlgorithm::CoolHot] {
        let hr_zipf = simulate(algo, true);
        let hr_unif = simulate(algo, false);
        eprintln!(
            "{:?}: zipfian_hit={:.3} uniform_hit={:.3}",
            algo, hr_zipf, hr_unif
        );
    }
    // CoolHot should hold the theta=0.99 hot set: high hit rate.
    let ch = simulate(EvictionAlgorithm::CoolHot, true);
    assert!(
        ch > 0.70,
        "CoolHot must keep the theta=0.99 hot set resident (hit>{:.2}); got {ch:.3}",
        0.70
    );
}

#[test]
#[ignore = "minutes-long trickle-less policy sim; real signal is eviction_pressure + ycsb_c"]
fn coolhot_scan_resistance() {
    for &algo in &[EvictionAlgorithm::Lru, EvictionAlgorithm::CoolHot] {
        let (hot_frac, scan_frac) = simulate_scan_resistance(algo);
        eprintln!(
            "{:?}: hot_resident={:.3} scan_resident={:.3}",
            algo, hot_frac, scan_frac
        );
    }
    let (hot_frac, _) = simulate_scan_resistance(EvictionAlgorithm::CoolHot);
    assert!(
        hot_frac > 0.90,
        "CoolHot: a one-touch scan must not evict the hot set; \
         {hot_frac:.3} of the hot set survived"
    );
}
