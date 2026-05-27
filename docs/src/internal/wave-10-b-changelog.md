# Wave 10-B — `CHANGELOG.md` generation

## Why this exists

Through v2.2.1 the canonical record of each Noxu DB release lived
in the annotated git tag (`git tag -l vX.Y.Z --format='%(contents)'`).
The annotations are dense — they include sprint and wave attribution,
audit-finding IDs, full test-gate counts, and a list of deferred
items per release.  That is the right format for a maintainer
reading the project, but it is the wrong format for an external user
who just wants to know:

- "Is the version I'm on affected by a known correctness bug?"
- "What changed in the version I am about to upgrade to?"
- "Which version introduced a feature I want?"

[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) is the
de-facto format for that audience.  Wave 10-B generates a single
`CHANGELOG.md` at the repository root that aggregates the tag
annotations into Keep-a-Changelog form.

## Format

The file follows Keep a Changelog 1.1.0:

- Most-recent release first.
- One H2 per release: `## [X.Y.Z] - YYYY-MM-DD`.
- H3 buckets within each release: `### Added`, `### Changed`,
  `### Fixed`, `### Removed`, `### Deprecated`, `### Security`,
  plus two non-standard buckets the audit-driven sprint cycle
  needs: `### Compatibility` (for on-disk format / API breakage
  notes) and `### Known Issues` (for `#[ignore]`'d regressions
  shipped against a release).
- Breaking changes are flagged in `### Compatibility — BREAKING`
  with a pointer to `docs/src/getting-started/migrating.md`.
- Bug fixes that the JE TCK port surfaced are attributed to the
  discovering wave, e.g. "Discovered during JE TCK port (Wave 4-B)".

Noxu DB adheres to [Semantic Versioning](https://semver.org/) starting
with v2.0.0.  Pre-v2.0 releases were the audit-driven remediation
phase and contain breaking changes between minor versions —
the CHANGELOG calls them out explicitly.

## What is, and is not, in the file

In:

- One section per release from v1.5.0 through the current latest
  tag, in prose form (5-15 bullets each).
- A consolidated paragraph summarising v1.4.0 through v1.4.3 (the
  pre-audit baseline).
- A `References` section with links to the migration guide, audit
  reports, decisions doc, and per-wave reports.

Out:

- Sprint and wave attribution beyond the bullet level.
- Test-gate counts for every wave (the most recent release's count
  is sufficient signal; the per-wave reports have the rest).
- Audit-finding IDs except where they help the reader (e.g. C2 / C3
  in v1.6.0, F-numbers for the rep GA blockers).
- Full lists of deferred items — the file points the reader at the
  release that closed each deferral.

The line budget is ~800 lines; the v2.2.1 generation is 524 lines.

## How to update on a new release

When tagging a new vX.Y.Z:

1. Write the annotated tag as you would normally
   (`git tag -s vX.Y.Z -F .../release-notes`).
2. Open `CHANGELOG.md`.
3. Replace the `## [Unreleased]` section's body with a new
   `## [X.Y.Z] - YYYY-MM-DD` section, condensing the tag annotation
   into the H3 buckets above.  Keep prose; do not paste the tag.
4. Re-add an empty `## [Unreleased]` section above it.
5. If new wave reports were added, append them to the
   *Wave reports* list under `References`.
6. Run the gate: `make docs-check` covers `docs/src/`; lint the
   CHANGELOG itself with
   `markdownlint-cli2 CHANGELOG.md && typos CHANGELOG.md`.
7. Commit alongside the version bump that the release commit
   already touches.

The file is sorted most-recent-first; never reorder it.

## Why the file lives at the repository root, not in `docs/src/`

External users discover the changelog from `README.md`, from package
registries (crates.io renders root-level `CHANGELOG.md` on the crate
page), and from VCS UIs (Codeberg and GitHub both surface a top-level
`CHANGELOG.md` automatically).  Putting it in `docs/src/` would mean
those audiences never see it.

The mdBook (under `docs/src/`) is the place for design context;
`CHANGELOG.md` is the place for release facts.  This wave-10-b
note exists in `docs/src/internal/` because it documents *how the
file is maintained* — that is contributor process, not release facts.

## Relationship to README.md

`README.md` is intentionally not modified by this wave.  Wave 10-C
adds the README hyperlink to `CHANGELOG.md`; that is a separate
workstream.  Wave 10-B's deliverable is the CHANGELOG itself plus
this note.

## Gate

At the close of Wave 10-B:

- `markdownlint-cli2 CHANGELOG.md` — clean.
- `typos CHANGELOG.md` — clean.
- `make docs-check` — clean (this lints `docs/src/`, including the
  wave-10-b note and the updated `SUMMARY.md`).
- `wc -l CHANGELOG.md` — 524 lines (within the ~800-line budget).

## Releases covered in the initial generation

v1.4.0, v1.4.1, v1.4.2, v1.4.3 (consolidated baseline paragraph),
then full sections for v1.5.0, v1.5.1, v1.6.0, v2.0.0-rc1, v2.0.0,
v2.1.0, v2.2.0, v2.2.1.
