# Acknowledgements

Noxu DB is built on decades of research and engineering work in embedded
database design. This page recognises the key ideas and their origins.

## Algorithms and Research

**B+tree with write-ahead logging and checkpoint recovery** follows the
structure established in the embedded database literature. The log-structured
approach to record management, BIN-delta write optimisation, and the
memory-budget accounting model are derived from published techniques for
transactional embedded stores.

**Flexible Paxos** for leader election:
Howard, H., Malkhi, D., and Spiegelman, A. (2016).
*Flexible Paxos: Quorum Intersection Revisited*.
arXiv:1608.06696.

**Phi Accrual Failure Detector** for adaptive heartbeat-based failure detection:
Hayashibara, N., Défago, X., Yared, R., and Katayama, T. (2004).
*The φ Accrual Failure Detector*.
Proceedings of the 23rd IEEE International Symposium on Reliable Distributed Systems (SRDS '04).

**Adaptive Replacement Cache (ARC)**:
Megiddo, N. and Modha, D. S. (2003).
*ARC: A Self-Tuning, Low Overhead Replacement Cache*.
Proceedings of the 2nd USENIX Conference on File and Storage Technologies (FAST '03).

**Clock with Adaptive Replacement (CAR)**:
Bansal, S. and Modha, D. S. (2004).
*CAR: Clock with Adaptive Replacement*.
Proceedings of the 3rd USENIX Conference on File and Storage Technologies (FAST '04).

**CLOCK-Pro**:
Jiang, S. and Zhang, X. (2005).
*CLOCK-Pro: An Effective Improvement of the CLOCK Replacement*.
USENIX Annual Technical Conference (USENIX ATC '05).

## Reference Archives

Reference source archives used during development are kept read-only in the
development tree at `_/je/` and `_/nosql/`. These archives informed the
design and implementation of Noxu DB's subsystems. The algorithms and data
structures in Noxu DB are Rust implementations of the concepts in those archives.

## Open Source Dependencies

Noxu DB is built with and depends on many open source libraries. Key
dependencies and their licenses:

| Crate | License | Purpose |
|---|---|---|
| `parking_lot` | Apache-2.0/MIT | Fast mutex and rwlock |
| `thiserror` | Apache-2.0/MIT | Error derive macros |
| `log` | Apache-2.0/MIT | Logging facade |
| `bytes` | MIT | Byte buffer utilities |
| `crc32fast` | Apache-2.0/MIT | CRC32 hardware acceleration |
| `byteorder` | Unlicense/MIT | Endian I/O |
| `memmap2` | Apache-2.0/MIT | Memory-mapped file I/O |
| `fs2` | MIT | File locking |
| `serde` | Apache-2.0/MIT | Serialization framework |
| `quinn` | Apache-2.0/MIT | QUIC transport (replication) |
| `tokio` | MIT | Async runtime (replication networking) |
| `hashbrown` | Apache-2.0/MIT | Fast hash tables |
| `quoracle` | MIT | Quorum system library |
