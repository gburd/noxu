# Getting Started

This chapter guides you through the core concepts and APIs of Noxu DB. By the
end you will be able to open environments and databases, read and write records,
iterate with cursors, define secondary indexes, and use serialization bindings.

The material here corresponds to the **Noxu DB Getting Started Guide**,
adapted for Rust idioms.

## Prerequisites

- Rust 1.85 or later (MSRV)
- No external server or daemon — Noxu DB is an embedded library

## In This Chapter

1. [Installation](installation.md) — adding Noxu DB to your Cargo project
2. [Environments](environments.md) — the top-level container for all databases
3. [Databases](databases.md) — named key-value stores within an environment
4. [Records](records.md) — the key/data model and byte layout
5. [Reading and Writing](reading-writing.md) — `put`, `get`, `delete`, auto-commit
6. [Cursors](cursors.md) — sequential and positioned access
7. [Secondary Databases](secondary-databases.md) — automatic index maintenance
8. [Serialization Bindings](bindings.md) — typed access via `noxu-bind`
9. [Migrating from v1.4.x](migrating.md) — breaking changes and bug fixes that surface in user code
