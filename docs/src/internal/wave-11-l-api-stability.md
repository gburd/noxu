# Wave 11-L — API Stability Commitment + SemVer Policy + cargo-semver-checks CI Gate

**Status**: In progress  
**Branch**: `fix/wave11-l-api-stability`  
**Baseline**: v2.4.1 (commit `1b8e344`)

## Objective

Produce the documentation, audit, and CI infrastructure that enforces the v3.0.0
API stability commitment. This is a cataloguing and policy wave — no public-API
changes are made.

## Work items

- [ ] Enumerate public API per crate → `docs/src/contributing/api-stability.md`
- [ ] Write SemVer policy → `docs/src/contributing/semver-policy.md`
- [ ] Add `#[deprecated]` markers for pre-v3.0 surface going away
- [ ] Add `cargo-semver-checks` CI job to both workflow files
- [ ] Update SUMMARY.md, roadmap, and CHANGELOG

## Findings (populated during execution)

_To be filled in._

## Judgement calls

_To be filled in._
