# Admin Tooling: `dump` / `load` / `print-log`

Noxu DB ships a single administrative binary, **`noxu-admin`**, with three
subcommands. They are faithful ports of the Berkeley DB JE utilities
[`DbDump`], [`DbLoad`], and [`DbPrintLog`], adapted to a Rust CLI.

```text
noxu-admin dump      -h <env> [-s <db>] [-f <file>] [-p] [-l] [-D]
noxu-admin load      -h <env> [-s <db>] [-f <file>] [-n]
noxu-admin print-log -h <env> [-S]
```

Build it from the workspace:

```bash
cargo build --release -p noxu-db --bin noxu-admin
# binary at target/release/noxu-admin
```

`dump` and `print-log` open the environment **read-only**, so they are safe to
run against a copy or backup and do not perturb a live environment's state.
`print-log` reads the raw `.ndb` log files directly (no recovery), so it works
even on a cleanly closed environment.

## `dump` — export a database

`dump` opens a database and writes every record to a portable text format
(the classic `db_dump` format). With no `-f`, output goes to stdout.

| Flag | Meaning |
|---|---|
| `-h <dir>` | Environment home directory (**required**) |
| `-s <db>` | Database name to dump |
| `-f <file>` | Output file (default: stdout) |
| `-p` | Emit printable characters where possible (else hex) |
| `-l` | List the database names in the environment and exit |
| `-D` | The database has sorted duplicates (see caveat below) |

The output starts with a header and ends with a `DATA=END` marker:

```text
VERSION=3
format=print
type=btree
dupsort=0
HEADER=END
 key1
 value1
 key2
 value2
DATA=END
```

Each record is two lines — the key then the data — each prefixed by a single
space. The encoding mirrors JE `CmdUtil.formatEntry`:

- **`format=print`** (`-p`): printable ASCII bytes (33–126) are written
  literally, a literal backslash is doubled (`\\`), and any other byte is
  written as `\HH` (a backslash plus two lowercase hex digits).
- **`format=bytevalue`** (default): every byte is two lowercase hex digits.

Both formats are **binary-safe**: keys and values containing arbitrary
non-UTF-8 bytes (NUL, newlines, `0xff`, …) round-trip exactly.

```bash
# Dump to stdout as hex
noxu-admin dump -h /var/lib/mydb -s users

# Dump to a file in printable form
noxu-admin dump -h /var/lib/mydb -s users -p -f users.dump

# List databases
noxu-admin dump -h /var/lib/mydb -l
```

## `load` — import a database

`load` reads a dump file (or stdin) and inserts every record into a database,
creating it if necessary. All records are inserted in a single transaction, so
a malformed file leaves the target database unchanged.

| Flag | Meaning |
|---|---|
| `-h <dir>` | Environment home directory (**required**) |
| `-s <db>` | Database name to load into (or use a `database=` header line) |
| `-f <file>` | Input file (default: stdin) |
| `-n` | No-overwrite: skip keys that already exist |

The format (`print` vs `bytevalue`) and the `dupsort` flag are taken from the
dump header. If `-s` is omitted, the database name is read from a
`database=<name>` header line.

```bash
# Round-trip: dump then load into a fresh environment
noxu-admin dump -h /var/lib/mydb -s users -f users.dump
noxu-admin load -h /var/lib/newdb -s users -f users.dump

# Pipe directly
noxu-admin dump -h /var/lib/mydb -s users | noxu-admin load -h /var/lib/newdb -s users
```

The `dump | load` round-trip reproduces the source database exactly — every
key/value pair, including binary data and duplicate keys.

### Duplicates caveat (`-D`)

Noxu does not currently persist the *sorted-duplicates* property of a database
across a reopen. As a result, `dump` cannot auto-detect that a database was
created with duplicates: reading a duplicates database back without declaring
the flag returns the raw internal two-part slot keys rather than the logical
key/data pairs.

To dump a database that has sorted duplicates, pass **`-D`** so `dump` opens
the source with the dup-sort flag and iterates it correctly. The resulting
dump header carries `dupsort=1`, and `load` then re-creates the duplicates
database faithfully. (This is symmetric to JE `DbLoad -c dupsort=true`, which
declares the property on the load side.)

## `print-log` — inspect the write-ahead log

`print-log` walks the WAL entry by entry, in LSN order, printing each entry's
LSN, type, and size — plus the transaction id and key/data sizes for record
(LN) and transaction-end entries. It is useful for debugging and forensics.

| Flag | Meaning |
|---|---|
| `-h <dir>` | Environment home directory (**required**) |
| `-S` | Print a per-type summary instead of per-entry detail |

Per-entry output:

```text
lsn=0x0/0x24 type=NameLN size=29
lsn=0x0/0x4f type=INS_LN_TX size=35 db=1 txn=1 keylen=5 datalen=5
lsn=0x0/0x2af type=Commit size=37 txn=1 commit
```

Summary output (`-S`):

```text
Log summary:
  total entries: 9
  total payload bytes: 521
  by type:
    Commit         1
    INS_LN_TX      7
    NameLN         1
```

LSNs are printed in JE form, `0x<file>/0x<offset>`.

## Error handling

All three subcommands fail with a clean `noxu-admin: <message>` on stderr and a
non-zero exit code (never a panic) for a missing environment, a missing
database, or a malformed dump file. This makes them safe to use in scripts.

[`DbDump`]: https://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/util/DbDump.html
[`DbLoad`]: https://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/util/DbLoad.html
[`DbPrintLog`]: https://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/util/DbPrintLog.html
