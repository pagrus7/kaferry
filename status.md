# Maintainability status

Started 2026-11-13 14:29:23 UTC. Baseline stack top: `e4ef4eb93ec6e117325f815157aeff82679187a3`. PR #193 is OPEN. Phase 2 waits until it is MERGED; poll at 30-minute intervals through 2026-11-13 22:29:23 UTC.

| Item | Branch | PR URL | Head SHA | CI | Reviews | Blocked by |
| --- | --- | --- | --- | --- | --- | --- |
| M0 inventory | notes/maintainability | No PR | pending | N/A | scouts running | exhaustive inventory and baseline measurements |
| M1 Python tests outside CI | pending | pending | pending | pending | pending | M0 |
| M2 dead Python outside CI | pending | pending | pending | pending | pending | M0 reachability confirmation |
| M3 unused system Go helpers | pending | pending | pending | pending | pending | M0 staticcheck |
| M4 harness tests | pending | pending | pending | pending | pending | M0 and product coverage |
| M5 harness selection | pending | pending | pending | pending | pending | #193 merge |
| M6 workflow reuse | pending | pending | pending | pending | pending | #193 merge |
| M7 CI and audit tests | pending | pending | pending | pending | pending | #193 merge |

## Line counts

Physical tracked source lines; exact inclusion rules and measurements pending.

| Category | Before | After |
| --- | ---: | ---: |
| Product Go | pending | pending |
| Unit Go tests | pending | pending |
| System Go | pending | pending |
| Python tooling, including tests | pending | pending |
| Python tests (subset) | pending | pending |
| Workflow YAML | pending | pending |
| Support / product | pending | pending |

## Product coverage

Before/after statement coverage of `./src/internal/... ./src/cmd/...`, combining unit and harness tests: pending. Maximum allowed decrease per Go removal PR: 0.1 percentage points.

## Owner decisions (report only)

- Review-process cost: assess independent docs/tests reviews and mandatory Codecov changed-line inspection; measurements pending. No policy changes in this run.
- Support-to-product ratio: decide whether to set a target after reviewing measured before/after counts.
- Expensive tests retained solely for oracle/gate or release safety: inventory pending.

## Phase 2 plan

Blocked until #193 is MERGED. Then merge origin/main into every open branch, change PR bases to main, and push a new reviewed commit for final-base CI. M5 compares exact selected test lists before/after; M6 consolidates setup/steps repeated at least three times using existing composite actions; M7 applies the deletion criterion to tools/ci and replicated_audit tests. If #193 is still open at the deadline, retain this plan and stop after Phase 1 readiness.
