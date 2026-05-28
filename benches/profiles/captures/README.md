# Wave 11-H Per-Workload `perf` Captures

Each subdirectory holds the text summaries from a `perf record` /
`perf report` session of a single Noxu DB workload (W03 sequential
read, W04 random read, W10 8r/8w concurrent mixed, W11 recovery /
re-open).

## How the captures were taken

Built with the `bench-profile` profile (release optimizations + full
debug info, no LTO, codegen-units=16) so symbols and line numbers
survive into the recorded samples:

```bash
cargo build --profile bench-profile -p noxu-perf-profiler

# Single-threaded reads — bumped to 200 repeats so the timed window
# is ~2.5 seconds and we collect ~4 K samples at 999 Hz.
perf record --call-graph dwarf -F 999 \
    -o w03/w03.perf.data \
    -- target/bench-profile/noxu-perf-profiler \
       --workload w03 --scale 10000 --repeats 200

perf record --call-graph dwarf -F 999 \
    -o w04/w04.perf.data \
    -- target/bench-profile/noxu-perf-profiler \
       --workload w04 --scale 10000 --repeats 200

# 8-thread mixed concurrent — 20 repeats over 10 K records, ~35 s.
perf record --call-graph dwarf -F 999 \
    -o w10/w10.perf.data \
    -- target/bench-profile/noxu-perf-profiler \
       --workload w10 --scale 10000 --threads 8 --repeats 20

# Recovery — populate then re-open 30 times, ~6.4 s.
perf record --call-graph dwarf -F 999 \
    -o w11/w11.perf.data \
    -- target/bench-profile/noxu-perf-profiler \
       --workload w11 --scale 10000 --repeats 30
```

## What is committed vs not

Only the text summaries are tracked.  The raw `*.perf.data` files
(~50–150 MB each) are gitignored — they're machine- and toolchain-
specific and easy to regenerate with the commands above.

For each workload:

* `top_self.txt`   `perf report --no-children --percent-limit 0.5`
                   (self-time top frames)
* `calltree.txt`   `perf report -g graph,0.5,callee --percent-limit 0.5`
                   (cumulative + caller chain for the hot frames)

## Hardware / environment of these captures

* Intel Core Ultra 7 258V, 8 physical cores, 30 GiB RAM
* Linux 7.0.9 (NixOS 25.11), `tmpfs` for the database directory
* Rust 1.95 stable, `bench-profile` Cargo profile
* `perf` from the host `linux-perf` package, `kernel.perf_event_paranoid=2`
  (user-space samples only, sufficient for self-time of Noxu code)

