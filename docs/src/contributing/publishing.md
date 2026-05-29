# Publishing to crates.io

This runbook is for the project maintainer to follow when publishing
Noxu DB to crates.io at a new release. It covers one-time setup,
per-release publish steps, and rollback procedures.

The first crates.io release will be **v3.0.0**.

---

## One-time setup

1. Create a crates.io account at <https://crates.io>.
2. Generate an API token: Account Settings → API Tokens → New Token.
   Grant the token *Publish* scope only.
3. Log in locally:

   ```bash
   cargo login <your-api-token>
   ```

4. Verify you own (or are an owner of) every crate in the list below.
   For the first publish, ownership is established automatically when
   you run `cargo publish` for the first time.

### quoracle prerequisite

`noxu-rep` depends on the `quoracle` library, which is maintained in a
separate repository at <https://github.com/gregburd/quoracle>. Before
publishing `noxu-rep`, ensure `quoracle` is published to crates.io:

```bash
cd path/to/quoracle
cargo publish -p quoracle
```

Verify: `https://crates.io/crates/quoracle`

---

## Per-release publish process

### Step 0 — Verify the workspace

```bash
git checkout main && git pull
```

Confirm the workspace version matches the release tag:

```bash
grep '^version' Cargo.toml   # should print: version = "X.Y.Z"
```

Run the full CI suite locally before publishing:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace
RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps
```

### Step 1 — Publish in dependency order

Publish one crate at a time, **in the exact order below**. Wait
approximately 60 seconds between each publish so crates.io has time to
index the new version before the next dependent crate is published.

```bash
# Layer 0 — no noxu-* deps
cargo publish -p noxu-util
sleep 60
cargo publish -p noxu-sync
sleep 60

# Layer 1 — depends on layer 0
cargo publish -p noxu-latch
sleep 60
cargo publish -p noxu-config
sleep 60

# Layer 2 — depends on layers 0-1
cargo publish -p noxu-log
sleep 60

# Layer 3 — depends on layers 0-2
cargo publish -p noxu-tree
sleep 60
cargo publish -p noxu-txn
sleep 60
cargo publish -p noxu-evictor
sleep 60

# Layer 4 — depends on layers 0-3
cargo publish -p noxu-cleaner
sleep 60
cargo publish -p noxu-recovery
sleep 60
cargo publish -p noxu-dbi
sleep 60
cargo publish -p noxu-engine
sleep 60

# Layer 5 — public API + higher-level layers
cargo publish -p noxu-db
sleep 60
cargo publish -p noxu-bind
sleep 60
cargo publish -p noxu-collections
sleep 60
cargo publish -p noxu-persist-derive
sleep 60
cargo publish -p noxu-persist
sleep 60
cargo publish -p noxu-xa
sleep 60

# Layer 6 — replication (requires quoracle on crates.io — see above)
cargo publish -p noxu-rep
sleep 60
```

### Step 2 — Verify docs.rs builds

After publishing each crate, docs.rs starts building it automatically.
Check the build status at:

```
https://docs.rs/<crate-name>/<version>
```

For example: `https://docs.rs/noxu-db/2.4.1`

If a docs.rs build fails, check the build log for missing feature flags
and update `[package.metadata.docs.rs]` accordingly, then publish a
patch release.

### Step 3 — Update README badges

Once all 19 crates are indexed, update `README.md` to add the
crates.io and docs.rs badges:

```markdown
[![crates.io](https://img.shields.io/crates/v/noxu-db.svg)](https://crates.io/crates/noxu-db)
[![docs.rs](https://docs.rs/noxu-db/badge.svg)](https://docs.rs/noxu-db)
```

### Step 4 — Tag the release

```bash
git tag -a "v3.0.0" -m "Release v3.0.0"
git push origin "v3.0.0"
```

---

## Notes on private crates

The following crates are **not** published to crates.io and remain
`publish = false`:

| Crate | Reason |
|---|---|
| `noxu-spec` | Stateright executable specifications, dev-only. Not part of the public API. |
| `noxu-observe` | Optional observability glue. The `observability` feature of `noxu-db` will not work for crates.io users until `noxu-observe` is also published. Publish decision deferred to a future release. |

---

## Rollback / yank procedure

If a broken release reaches crates.io, yank it immediately:

```bash
cargo yank --version <version> -p <crate-name>
```

For example:

```bash
cargo yank --version 3.0.0 -p noxu-db
```

Yanking does not remove the package — it prevents new projects from
resolving it as a dependency. Existing lock files that already pin the
yanked version continue to work.

After yanking, fix the issue and publish a patch release (`3.0.1`).

To un-yank a version (if the yank was a mistake):

```bash
cargo yank --version <version> -p <crate-name> --undo
```

---

## Crate ownership

Add or remove co-owners:

```bash
cargo owner --add <github-username> -p <crate-name>
cargo owner --remove <github-username> -p <crate-name>
```

To add all 19 crates to an owner in one shot:

```bash
for crate in noxu-util noxu-sync noxu-latch noxu-config noxu-log \
             noxu-tree noxu-txn noxu-evictor noxu-cleaner noxu-recovery \
             noxu-dbi noxu-engine noxu-db noxu-bind noxu-collections \
             noxu-persist-derive noxu-persist noxu-xa noxu-rep; do
  cargo owner --add <username> -p "$crate"
done
```
