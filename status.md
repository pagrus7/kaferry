# Maintainability status

Started 2026-11-13 14:29:23 UTC. Baseline stack top: `e4ef4eb93ec6e117325f815157aeff82679187a3`. PR #193 is OPEN. Phase 2 waits until it is MERGED; poll at 30-minute intervals through 2026-11-13 22:29:23 UTC.

| Item | Branch | PR URL | Head SHA | CI | Reviews | Blocked by |
| --- | --- | --- | --- | --- | --- | --- |
| M0 inventory | notes/maintainability | No PR | branch tip | N/A | three read-only scouts complete | none |
| M1 docs tests | maintainability/python-doc-tests | https://github.com/pagrus7/kaferry/pull/195 | e914c01deba7b292a045e3917ebb046a1520d811 | Passed; final-head raw evidence independently verified | Docs Passed; Tests Passed | Stacked on #193; ready |
| M1 release tests | maintainability/python-release-tests | https://github.com/pagrus7/kaferry/pull/196 | ffdd57f2b1e0b60e2aef9d59877bedecf8d46e1e | Passed; final-head raw evidence independently verified | Docs Passed; Tests Passed | Stacked on #193; ready |
| M1 qualification accounting | maintainability/python-accounting-tests | https://github.com/pagrus7/kaferry/pull/197 | 04461399416d06269e8c6ffad56762185f99b6e5 | watching; local full 204 tests passed | Docs source complete; Tests source complete | final-head CI verification |
| M1 qualification runner | maintainability/python-runner-tests | https://github.com/pagrus7/kaferry/pull/198 | 6efcad82c760c9b6aeaf39151f6a0a86b916e324 | watching; local full 204 tests passed | Docs source complete; Tests source complete | final-head CI verification |
| M1 qualification storage | maintainability/python-storage-tests | https://github.com/pagrus7/kaferry/pull/199 | 06d34e2648419e2f638ed0224628ce586cb91658 | watching; local full 205 tests passed | Docs source complete; Tests source complete | final-head CI verification |
| M2 dead Python outside CI | no branch | No PR needed | baseline | N/A | inventory complete | no justified removal: dependency probe and callback bindings are used |
| M3 unused system Go helpers | no branch | No PR needed | baseline | N/A | inventory complete | no justified removal: all 141 partial-tag findings are live in full harness builds |
| M4 soak/load diagnostics | maintainability/harness-soak-tests | https://github.com/pagrus7/kaferry/pull/200 | f3b271c6a31c6813b4a6cd9975a29c5e264adb9a | old-head CI running; fix pending | Docs source complete; Tests Failed: restore late drain guard | reviewer rework and new-head CI |
| M4 faults | maintainability/harness-fault-tests | https://github.com/pagrus7/kaferry/pull/201 | 35942bfae9bac785b60dca89be58bcb30f869225 | watch exited nonzero; diagnosis pending | Pending | CI diagnosis and independent source reviews |
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

## Retained tests with costly maintenance surfaces

Source lines and fixture complexity are cost proxies; human effort, runtime and monetary maintenance cost are unknown. These are owner-decision items only.

# Owner decisions: costly retained rejection and release tests

Baseline `e4ef4eb93ec6e117325f815157aeff82679187a3`. Report only; keep these protections. Function lines include signatures and closing braces (Python includes decorators); file sizes include shared fixtures/imports. Source size and fixture complexity are maintenance-surface observations, not measured runtime, money or human hours. Those costs are unknown. No test or tool execution was performed for this report.

The earlier Go group totals (broker evidence 345, ownership evidence 405, publisher 347, Darwin RSS 73) count whole files. Exact per-function names and counts follow so the owner can identify each protection. The 15-line release binary substitution test remains inexpensive and is listed separately for context.

## Go tests retained only for criterion (b)

| path | exact test | function lines | maintenance surface |
| --- | --- | ---: | --- |
| src/tests/system/replicated_broker_evidence_harness_test.go | `TestBrokerWorkCapsRequireActualSamplesAndKeepMissingPeaksUnavailable` | 23 | Raw progress/summary evidence schema; sampled work versus unavailable exact peaks. |
| src/tests/system/replicated_broker_isr_harness_test.go | `TestBrokerISRSelectionRequiresActiveSameLeaderBacklog` | 47 | Ten synthetic routing/leader/ISR/backlog/timing scenarios tied to actual fault exposure. |
| src/tests/system/replicated_broker_lifecycle_harness_test.go | `TestBrokerExitWitnessRetainsUnsupportedAndIntentionalExitsBeforeAssertions` | 96 | Real subprocess exits/signals, immutable log digests, restart authorization and driver intent. |
| src/tests/system/replicated_broker_progress_test.go | `TestBrokerRestartRechecksFailureAfterHealthObservation` | 42 | Subprocess lifetime, allowed exit classification, active fault window and live RSS/recovery contracts. |
| src/tests/system/replicated_broker_progress_test.go | `TestBrokerRestartRequiresCurrentLifetimeInsideRecoveryWindow` | 22 | Subprocess lifetime, allowed exit classification, active fault window and live RSS/recovery contracts. |
| src/tests/system/replicated_broker_progress_test.go | `TestNetworkOutageRestartRequiresFirstWorkerRSSAndActiveCut` | 24 | Subprocess lifetime, allowed exit classification, active fault window and live RSS/recovery contracts. |
| src/tests/system/replicated_broker_readiness_harness_test.go | `TestBrokerReadinessRequiresFreshPinnedMetadataWithinSharedBudget` | 9 | Nineteen helper-driven Docker ownership/state and pinned Kafka metadata/deadline scenarios; 9-line wrapper understates shared fixture cost. |
| src/tests/system/replicated_ownership_cases_harness_test.go | `TestOwnershipGenerationsReuseOneAuditSnapshot` | 44 | Synthetic raw audit generations, complete/disjoint assignments and killed-worker cap evidence schemas. |
| src/tests/system/replicated_ownership_cases_harness_test.go | `TestOwnershipCaseRejectsUnprovedMembership` | 34 | Synthetic raw audit generations, complete/disjoint assignments and killed-worker cap evidence schemas. |
| src/tests/system/replicated_ownership_cases_harness_test.go | `TestOwnershipKilledWorkerKeepsEarlierPeak` | 36 | Synthetic raw audit generations, complete/disjoint assignments and killed-worker cap evidence schemas. |
| src/tests/system/replicated_ownership_exit_harness_test.go | `TestOwnershipCoordinatorRestartRequiresRecordedScope` | 44 | Subprocess STOP/CONT intentions and recorded coordinator/rebalance/recovery timing. |
| src/tests/system/replicated_ownership_observation_harness_test.go | `TestOwnershipObservationFollowsFreshCoordinatorBehindAcceptingProxy` | 93 | Forwarding TCP proxies plus kfake coordinator/group/offset protocol fixtures; shared raw audit decoding. |
| src/tests/system/replicated_ownership_observation_harness_test.go | `TestReplicatedAuditCohortUsesOneCompleteParsedSnapshot` | 28 | Forwarding TCP proxies plus kfake coordinator/group/offset protocol fixtures; shared raw audit decoding. |
| src/tests/system/replicated_workload_harness_test.go | `TestReplicatedPublisherAcknowledgesAllPartitionsAndStops` | 45 | kfake source partitions, controlled protocol rejections/transport EOF, scheduled offers and actual ACK/offset reconciliation. |
| src/tests/system/replicated_workload_harness_test.go | `TestReplicatedBrokerPublisherKeepsHealthyPartitionsAndDrainsUncertainWrites` | 54 | kfake source partitions, controlled protocol rejections/transport EOF, scheduled offers and actual ACK/offset reconciliation. |
| src/tests/system/replicated_workload_harness_test.go | `TestReplicatedBrokerPublisherCatchesUpDelayedSchedule` | 31 | kfake source partitions, controlled protocol rejections/transport EOF, scheduled offers and actual ACK/offset reconciliation. |
| src/tests/system/replicated_workload_harness_test.go | `TestReplicatedBrokerPublisherCancellationDrainsDelayedOffers` | 21 | kfake source partitions, controlled protocol rejections/transport EOF, scheduled offers and actual ACK/offset reconciliation. |
| src/tests/system/replicated_workload_harness_test.go | `TestReplicatedBrokerPublisherErrorDrainsDelayedOffers` | 23 | kfake source partitions, controlled protocol rejections/transport EOF, scheduled offers and actual ACK/offset reconciliation. |
| src/tests/system/replicated_workload_harness_test.go | `TestReplicatedBrokerPublisherRetriesNegotiationEOFAfterConnectionLoss` | 37 | kfake source partitions, controlled protocol rejections/transport EOF, scheduled offers and actual ACK/offset reconciliation. |
| src/tests/system/replicated_workload_harness_test.go | `TestReplicatedPublisherStopsAfterRejectedBatch` | 48 | kfake source partitions, controlled protocol rejections/transport EOF, scheduled offers and actual ACK/offset reconciliation. |
| src/tests/system/replicated_workload_harness_test.go | `TestReplicatedBrokerPublisherRetriesSafeLeaderResponsesWithinDeliveryBound` | 59 | kfake source partitions, controlled protocol rejections/transport EOF, scheduled offers and actual ACK/offset reconciliation. |
| src/tests/system/rss_harness_darwin_test.go | `TestRSSSampleOwnedChild` | 56 | Darwin native RSS units, 64-MiB live child allocation, process lifetime and platform syscall behavior. |

## Python tests retained only for criterion (b) or (c) in explicitly costly scopes

These scopes come from the Python inventory’s explicit maintenance-cost notes; only retained test methods are included. The network retry test is individually identified there.

| path | exact test method | criterion | method lines | maintenance surface |
| --- | --- | --- | ---: | --- |
| tools/ci/test_replicated_helpers.py | `ReplicatedHelpersTest.test_shared_fast_target_requires_exact_completed_helper_passes` | (b) | 63 | 86 file lines/1 test baseline; subprocess/Make regression with generated fake Go executable. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_wire_reader_normalizes_absent_headers_and_preserves_header_values` | (b) | 23 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_initialization_records_transient_setup_then_exact_baseline_before_ready` | (b) | 5 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_initialization_never_retries_unsafe_progress_or_changed_records` | (b) | 15 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_initialization_deadline_and_nontransient_failures_never_report_ready` | (b) | 31 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_same_driver_error_during_a_measured_sample_is_fatal_without_retry` | (b) | 14 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_wire_timeout_remains_failure_and_snapshot_readers_pause_at_the_cut` | (b) | 15 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_lagging_independent_consumer_catches_up_only_to_captured_output` | (b) | 9 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_later_output_cannot_repair_an_unsafe_saved_snapshot` | (b) | 10 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_prefix_requires_every_partition_position_not_just_a_count` | (b) | 8 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_unsafe_save_on_one_partition_cannot_hide_behind_another` | (b) | 10 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_identical_source_records_are_rejected_instead_of_ambiguous_matching` | (b) | 8 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_every_repeated_copy_is_checked_for_corruption` | (b) | 10 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_duplicate_offset_cuts_ignore_observer_lag_and_check_consecutive_pairs` | (b) | 15 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_unavailable_observation_is_a_gap_and_cannot_finish` | (b) | 13 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_no_fault_precondition_still_reconciles_and_rejects_unsafe_samples` | (b) | 25 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_receipt_contains_event_derived_proof_and_checked_partition_coverage` | (b) | 13 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_exact_registered_budgets_are_required_even_when_the_sum_is_consistent` | (b) | 27 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_producer_rejects_unregistered_budget_before_sampling_or_injection` | (b) | 13 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_bootstrap_proof_rejects_missing_baseline_ready_and_changed_setup_state` | (b) | 12 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_collector_rejects_missing_cases_mismatches_and_tampered_samples` | (b) | 24 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_complete_cycle_requires_matching_binary_and_receipts` | (b) | 30 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_sample_proof_rejects_invalid_numbers_cuts_and_missing_phases` | (b) | 19 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_independent_observations_require_each_phase_and_reject_overlap_or_unavailable_baselines` | (b) | 45 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_fault_oracle.py | `FaultOracleTest.test_checker_prerequisites_fail_without_running_brokers` | (b) | 12 | 620 file lines/25 tests baseline; SQLite reconciliation plus bootstrap/sample JSONL evidence fixtures. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_actual_rejection_and_legitimate_rejoin_are_distinguished` | (b) | 8 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_local_suppression_needs_complete_audit_and_peer_progress` | (b) | 8 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_fatal_periodic_save_exit_requires_actual_rejection_before_exit` | (b) | 17 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_rejoin_without_new_generation_progress_cannot_replace_fencing` | (b) | 10 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_transport_exit_or_traffic_absence_without_positive_fencing_fails` | (b) | 15 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_old_close_stimulus_short_rpc_or_old_recovery_receipt_fails` | (b) | 19 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_both_workers_must_live_through_peer_progress_and_peer_must_keep_running` | (b) | 10 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_accepted_stale_commit_fails_even_when_final_progress_is_safe` | (b) | 6 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_commit_partition_payloads_remain_strict_on_both_sides` | (b) | 30 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_membership_alone_and_peer_progress_after_healing_do_not_qualify` | (b) | 6 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_unobserved_rejoin_and_wrong_worker_route_fail` | (b) | 11 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_legitimate_rejoin_can_finish_during_the_heal_operation` | (b) | 5 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_explicit_leave_cannot_be_reported_as_session_expiry` | (b) | 4 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_missing_response_clock_or_preserved_log_fails_closed` | (b) | 18 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_shared_safety_and_exact_binary_identity_remain_required` | (b) | 7 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_membership_transfer_without_actual_session_expiry_does_not_qualify` | (b) | 7 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_pending_or_missing_network_receipts_prevent_complete_audit_claim` | (b) | 15 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_audit.py | `OwnershipAuditTest.test_filters_async_logging_rotation_deletion_and_logging_errors_fail` | (b) | 21 | 398 file lines/20 tests baseline; 19 audit/config files per fixture. |
| tools/qualification/test_ownership_cases.py | `OwnershipCasesTest.test_all_three_cases_keep_independent_raw_group_generations` | (b) | 8 | 378 file lines/8 tests baseline; large synthetic raw-protocol and CLI lifecycle fixture. |
| tools/qualification/test_ownership_cases.py | `OwnershipCasesTest.test_selected_members_and_protocol_must_belong_to_retained_lifetimes` | (b) | 25 | 378 file lines/8 tests baseline; large synthetic raw-protocol and CLI lifecycle fixture. |
| tools/qualification/test_ownership_cases.py | `OwnershipCasesTest.test_retained_dead_member_metadata_is_not_live_ownership_proof` | (b) | 15 | 378 file lines/8 tests baseline; large synthetic raw-protocol and CLI lifecycle fixture. |
| tools/qualification/test_ownership_cases.py | `OwnershipCasesTest.test_missing_altered_and_rechecksummed_invalid_audits_fail` | (b) | 15 | 378 file lines/8 tests baseline; large synthetic raw-protocol and CLI lifecycle fixture. |
| tools/qualification/test_ownership_cases.py | `OwnershipCasesTest.test_shared_observer_is_revalidated_and_exact_run_selection_is_enforced` | (b) | 10 | 378 file lines/8 tests baseline; large synthetic raw-protocol and CLI lifecycle fixture. |
| tools/qualification/test_ownership_cases.py | `OwnershipCasesTest.test_raw_sync_assignment_must_match_the_valid_described_member` | (b) | 15 | 378 file lines/8 tests baseline; large synthetic raw-protocol and CLI lifecycle fixture. |
| tools/qualification/test_ownership_cases.py | `OwnershipCasesTest.test_empty_intermediate_members_allow_null_only_with_matching_raw_empty_group` | (b) | 37 | 378 file lines/8 tests baseline; large synthetic raw-protocol and CLI lifecycle fixture. |
| tools/qualification/test_ownership_cases.py | `OwnershipCasesTest.test_independent_generation_assignment_and_commit_chain_reject_self_attestation` | (b) | 12 | 378 file lines/8 tests baseline; large synthetic raw-protocol and CLI lifecycle fixture. |
| tools/qualification/test_qualify.py | `MatrixTest.test_network_precondition_reruns_once_and_keeps_each_attempt_under_one_lease` | (b) | 111 | 111 method lines/14 outcome scenarios; temporary Git repository and mocked runner/oracle seams; product/safety/identity failures must not become inconclusive retries. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_retains_measurements_and_renders_matrix_and_throughput` | (c) | 26 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_rejects_incomplete_failed_and_mismatched_measurements` | (c) | 39 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_rejects_mislabeled_profile_and_unobserved_compression` | (c) | 16 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_rejects_every_omitted_recorded_parameter` | (c) | 15 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_rejects_invalid_parameter_types_ranges_and_unknown_metadata` | (c) | 41 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_rejects_unknown_or_noninteger_observed_codec_counts` | (c) | 13 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_normalizes_unused_zero_parameters_and_retains_allowed_entropy` | (c) | 14 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_retains_fixed_file_and_endurance_recipe_parameters` | (c) | 19 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_requires_each_profile_measurement_for_recorded_workloads` | (c) | 6 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_retains_unrecorded_workload_status_for_original_receipts` | (c) | 17 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |
| tools/release/test_release_results.py | `ReleaseResultsTest.test_rejects_mixed_recorded_and_unrecorded_workloads` | (c) | 8 | 311 file lines/11 tests baseline; synthetic qualification matrix/metrics and tar archive fixture. |

## Inexpensive release-safety context

`src/tests/system/binary_hash_test.go`: `TestExpectedBinaryHashRejectsSubstitution`, 15 function lines/28 file lines, criterion (c); temporary file SHA-256 mismatch and missing-file rejection. This guards a release safety boundary without the large fixture surface above.

## Decision requested

Keep the rejection/release guarantees. The owner may decide whether their current independent evidence schemas and platform/protocol fixtures merit focused consolidation in later work. No runtime reduction, billing savings or defect-prevention probability has been measured.

## Phase 2 poll log

| Time (UTC) | PR #193 state |
| --- | --- |
| 2026-11-13 14:29:23 | OPEN |
| 2026-11-13 14:59:23 | OPEN |

## Active review correction

M4 soak/load must retain `TestConsumeOnlyStartAllowsLatePublishingGrace` and its required-list entry. It prevents a late final submission from shortening the required ten-second drain period while still passing reconciliation and the five idle-sample gate. Restoration and updated combined line counts are pending; the composition figures above describe the initial frozen heads.
