# Maintainability status

Started 2026-11-13 14:29:23 UTC. Baseline stack top: `e4ef4eb93ec6e117325f815157aeff82679187a3`. PR #193 is OPEN. Phase 2 waits until it is MERGED; poll at 30-minute intervals through 2026-11-13 22:29:23 UTC.

| Item | Branch | PR URL | Head SHA | CI | Reviews | Blocked by |
| --- | --- | --- | --- | --- | --- | --- |
| M0 inventory | notes/maintainability | No PR | branch tip | N/A | three read-only scouts complete | none |
| M1 docs tests | maintainability/python-doc-tests | https://github.com/pagrus7/kaferry/pull/195 | e914c01deba7b292a045e3917ebb046a1520d811 | Passed; final-head raw evidence independently verified | Docs Passed; Tests Passed | Stacked on #193; ready |
| M1 release tests | maintainability/python-release-tests | https://github.com/pagrus7/kaferry/pull/196 | ffdd57f2b1e0b60e2aef9d59877bedecf8d46e1e | watching; local 214 Python tests passed | Docs source complete; Tests source complete | final-head CI verification |
| M1 qualification accounting | maintainability/python-accounting-tests | https://github.com/pagrus7/kaferry/pull/197 | 04461399416d06269e8c6ffad56762185f99b6e5 | watching; local full 204 tests passed | Docs source complete; Tests source complete | final-head CI verification |
| M1 qualification runner | maintainability/python-runner-tests | https://github.com/pagrus7/kaferry/pull/198 | 6efcad82c760c9b6aeaf39151f6a0a86b916e324 | watching; local full 204 tests passed | Pending | CI and independent reviews |
| M1 qualification storage | maintainability/python-storage-tests | https://github.com/pagrus7/kaferry/pull/199 | 06d34e2648419e2f638ed0224628ce586cb91658 | watching; local full 205 tests passed | Pending | CI and independent reviews |
| M2 dead Python outside CI | no branch | No PR needed | baseline | N/A | inventory complete | no justified removal: dependency probe and callback bindings are used |
| M3 unused system Go helpers | no branch | No PR needed | baseline | N/A | inventory complete | no justified removal: all 141 partial-tag findings are live in full harness builds |
| M4 soak/load diagnostics | maintainability/harness-soak-tests | https://github.com/pagrus7/kaferry/pull/200 | f3b271c6a31c6813b4a6cd9975a29c5e264adb9a | watching; all local checks passed | Pending | CI and independent reviews |
| M4 faults | maintainability/harness-fault-tests | https://github.com/pagrus7/kaferry/pull/201 | 35942bfae9bac785b60dca89be58bcb30f869225 | watching; all local checks passed | Pending | CI and independent reviews |
| M4 fixtures/process | maintainability/harness-fixture-tests | https://github.com/pagrus7/kaferry/pull/202 | 05d4492ba83069358311c83a4f5d59a40ad78a44 | watching; all local checks passed | Pending | CI and independent reviews |
| M5 harness selection | pending | pending | pending | pending | pending | #193 merge |
| M6 workflow reuse | pending | pending | pending | pending | pending | #193 merge |
| M7 CI and audit tests | pending | pending | pending | pending | pending | #193 merge |

## Line counts

Physical tracked source lines at the baseline stack top. Product is non-test Go in `src/internal` and `src/cmd`; unit tests are test Go in those trees; system Go is all Go in `src/tests`. Support sums unit tests, system Go, all Python tooling (including its tests once), and workflow YAML.

| Category | Before | After (proposed combined state) |
| --- | ---: | ---: |
| Product Go | 12,502 | 12,502 |
| Unit Go tests | 20,099 | 20,099 |
| System Go | 18,437 | 17,619 |
| Python tooling, including tests | 9,827 | 8,943 |
| Python tests (subset) | 5,151 | 4,269 |
| Workflow YAML | 1,468 | 1,468 |
| Support / product | 3.985842 | 3.849704 |

## Product coverage

The eight proposed patches were combined in a detached analysis worktree. Support lines fall from 49,831 to 48,129: **1,725 unique lines removed / 23 added**. Individual PR deltas sum to 1,727 removed / 24 added; shared import/blank-line changes overlap. Combining them required mechanical union of deleted methods/imports in `test_qualify.py` and 23 deleted helper names in `suites.py`. These overlaps can require merge resolution as PRs land. No PR was merged and no product source changed.

The combined tree passed all 172 remaining Python tests, unit tests and all 98 remaining Darwin harness tests. Retained Python method ASTs are unchanged. Combined product statement coverage is 5,514/5,750 = 95.895652%, identical to baseline (0.000000 percentage-point change).

Baseline statement coverage of `./src/internal/... ./src/cmd/...`, combining unit and every harness test: 5,514 / 5,750 = **95.895652%**. Go 1.27.4, Darwin arm64, race/atomic profiles, measured at the baseline stack top. Unit-only coverage is identical. Both runs passed. Per-PR after measurements pending. Maximum allowed decrease per Go removal PR: 0.1 percentage points.

| Go removal | Before | After (proposed combined state) | Change (percentage points) | Harness selection, Darwin |
| --- | ---: | ---: | ---: | --- |
| M4 soak/load | 95.895652% (5,514/5,750) | 95.895652% (5,514/5,750) | 0.000000 | 121 → 112; exact nine-test difference |
| M4 faults | 95.895652% (5,514/5,750) | 95.895652% (5,514/5,750) | 0.000000 | 121 → 112; exact nine-test difference |
| M4 fixtures/process | 95.895652% (5,514/5,750) | 95.895652% (5,514/5,750) | 0.000000 | 121 → 116; exact five-test difference |

Both branches reran unit and all selected harness tests under race/atomic before and after. Patch coverage is N/A: no non-test product Go changed.

## Inventory evidence

Worker inspection added twelve explicit nested callback verdicts, bringing the current inventory to 541 unique rows. Five pre-existing storage helper rows were deduplicated. Docs and release candidate rows now link to their PRs.

Fault-worker inspection adds one local wrapper verdict and reclassifies the orphan queue type, bringing the inventory to 542 unique rows. Qualification candidate rows now link to PRs #197–#199.

M0 classifies 529 unique rows: all 217 Python tests, 121 harness tests (120 Linux plus one Darwin-only), the integration-only faults routing test, inspected helpers, all three vulture hits, and every one of 141 unique conditional staticcheck findings. Twenty-eight overlapping helper records are consolidated. Vulture 2.16 at confidence 80 found no dead function or module. Staticcheck 2026.2.2 covered all 12 Linux CI tag combinations and four Darwin modes; full harness mode has no U1000 findings. Exact baseline EXPECTED selection equals the current harness set, rather than about 160 names. The historical stack moved 121 Linux/122 Darwin tests, then removed two legacy SQL tests and added one delayed-input test. The stack's other starting claims were verified.

## Owner decisions (report only)

- Review-process cost: each removal PR requires two independent source reviews and a final-head CI evidence verification. Codecov changed-line review is N/A while no product implementation changes. Decide separately whether the recurring review cost is proportionate; no policy changes in this run. Personal usage evidence remains local.
- Support-to-product ratio: decide whether to set a target after reviewing measured before/after counts.
- Expensive tests retained solely for oracle/gate or release safety: broker-evidence tests (345 function lines), ownership-evidence tests (405), publisher reconciliation (347), and Darwin RSS calibration (73) retain unique false-green rejection. They require large fixtures or platform-sensitive calibration; measured runtime and monetary maintenance cost are unknown. Python rejection and release-safety fixtures remain for the same criterion; see the inventory's KEEP reasons.

## Phase 2 plan

Blocked until #193 is MERGED. Then merge origin/main into every open branch, change PR bases to main, and push a new reviewed commit for final-base CI. M5 compares exact selected test lists before/after; M6 consolidates setup/steps repeated at least three times using existing composite actions; M7 applies the deletion criterion to tools/ci and replicated_audit tests. If #193 is still open at the deadline, retain this plan and stop after Phase 1 readiness.
