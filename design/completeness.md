# Implementation completeness

Maps `decisions.md` and `grammar.md` to implementation and executable evidence. **Covered** means tested at the cheapest useful layer, not qualified on every deployment. Tests below run in `make check` or `make integration`, except explicitly separate jobs. Final run results belong in `implementation-results.md`.

## Copy and time semantics

| Rule | Implementation | Evidence |
| --- | --- | --- |
| All four directions; only `SELECT *`, no transformation | app/engine, sql/parser | `TestTopicToTopicSlice`, `TestAllFileDirectionsAndResume`, `TestInvalidStatements` |
| Raw/null/empty key/value, ordered duplicate headers, timestamp/type, diagnostic provenance | copy/record, Kafka source/sink, file/codec | `TestRecordFidelityAndTopicID`, `TestRecordCodecFullFidelity`, `TestAllFileDirectionsAndResume` |
| Payload/schema bytes remain opaque; user ensures compatibility | Kafka sink, file/codec | `TestCrossClusterOpaqueSchemaBytes`; no schema lookup or remapping component |
| At least once; output precedes progress; failed later work leaves earlier output | copy/progress, runner, file/output | `TestRunnerDoesNotCommitPastFailedWrite`, `TestFileCrashAfterFsyncBeforeCheckpoint`, `TestWorkerCrashBeforeDeliveryProcessingReplaysWithoutLoss` |
| Millisecond record time; half-open bounds, inclusive BETWEEN, checked intervals | plan/window, runner/filter | `TestWindowBoundaries`, `TestTimeBoundariesOutOfOrderAndTriggerReplay`, `TestInvalidTimeExpressions` |
| NOW resolves once per positioned partition/file; saved windows survive restart/reassignment | Kafka positioning/commit, file/source/checkpoint | `TestConcurrentNowWindowsPersistAcrossWorkersAndRestart`, `TestPositioningBatchesBrokerRequests`, `TestCheckpointAndSourceSnapshot` |
| Missing partition adopts lowest saved window; concurrent workers may resolve differently | Kafka positioning | `TestPositioningBatchesBrokerRequests`, `TestConcurrentNowWindowsPersistAcrossWorkersAndRestart` |
| All expired progress is a new group/window; missing progress is logged | Kafka positioning, app/engine | `TestConcurrentNowWindowsPersistAcrossWorkersAndRestart` (clear all group progress, resolve fresh window) |
| Commit first; otherwise lower timestamp lookup, upper-only earliest, or START | Kafka positioning/source | `TestPositioningBatchesBrokerRequests`, `TestTopicToTopicSlice`, `TestCLIInterruptCommitsAndResumeDoesNotRecopy` |
| START applies only to a group with no valid progress; new partitions read retained records | Kafka positioning | `TestCLIInterruptCommitsAndResumeDoesNotRecopy` (source expansion under LATEST) |
| No-match timestamp lookup starts at LSO; protected read_committed and explicit offsets | Kafka source/positioning | `TestNoMatchLookupOpenTransactionAndTrailingMarker`, `TestTransactionalTailAndTimestampLookup` |
| Below-start commits resume at log start and log gap; above-end commits fail | Kafka positioning | `TestExpiredAndAboveEndCommits`, `TestPositioningBatchesBrokerRequests` |
| Retention overtaking a running cursor fails; restart resumes retained data | Kafka source/positioning | `TestRetentionOvertakesRunningConsumerFailsAndRestartUsesLogStart` |
| Retained-only time-window warning | Kafka source/checkRetention | `TestRetentionWarningOnFirstRetainedRecord` |
| Timestamp stop is strictly past hi+cutoff; future trigger omits later records | runner/stopAtTimestamp | `TestTimeBoundariesOutOfOrderAndTriggerReplay`, `TestTimestampTriggerReplaysTriggerOffset` |
| Tip samples H once, includes control/aborted gaps, resolves prior work, times out | runner/tips | `TestTipWaitUsesSampledHighWatermarkAndTimesOut`, `TestCapturedTipFinishesWithoutAnotherEOF`, `TestNoMatchLookupOpenTransactionAndTrailingMarker` |
| Timestamp stop saves trigger offset; tip saves next offset; no upper bound follows | runner | `TestTimestampTriggerReplaysTriggerOffset`, `TestTransactionalTailAndTimestampLookup`, `TestFollowStandbySurvivesLongQuiet` |
| File snapshot is size at open; partial EOF ends input; appends wait for another run | file/source | `TestCheckpointAndSourceSnapshot`, `TestFileSourceUsesDecoderAndRemembersOnlyDecodedFrames` |

## Workers, identity and delivery

| Rule | Implementation | Evidence |
| --- | --- | --- |
| Named GROUP required; ONE_OFF generates one logged ID; successful file run deletes checkpoint, retains lock | plan/options, app/engine, file/checkpoint | `TestInvalidPlanning`, `TestOneOffUsesOneGroupAndDeletesOnlyOnSuccess`, `TestFileCLIAndOneOffCleanup` |
| Classic cooperative-sticky, local completion, no done markers or coordinator | Kafka source, runner | `TestCooperativeRevocationCommitAndGroupInspection`, `TestDistributedWorkersFinishAndSoleWorkerConfirms` |
| Invalid progress/foreign members fail before joining; unavailable check fails | Kafka metadata/source | `TestForeignProgressAndSelfCopyFailBeforeJoining`, `TestInspectorRejectsMissingOffsetResultsAndForeignMembers`, startup mock failure tests |
| Empty assignment is not completion; bounded standbys idle-exit; follow standbys remain | runner | `TestEmptyAssignmentIdleExitRequiresCompletedAssignment`, `TestFollowStandbySurvivesLongQuiet`, `TestMoreWorkersThanPartitionsIdleAndFinishIndependently` |
| Sole-worker rerun after members leave confirms bounded group; may process late data | runner, group offsets | `TestDistributedWorkersFinishAndSoleWorkerConfirms`, `TestMoreWorkersThanPartitionsIdleAndFinishIndependently` |
| Revocation saves confirmed prefix without waiting; lost ownership never saves; stale outcomes fenced | runner, progress, Kafka commit | `TestRevokeFencesLateOutcomesAndLostOwnershipNeverCommits`, `TestSourceReleasesLostOwnershipThroughInjectedConsumer`, `TestAssignmentLostOnPollDeadline` (fault job) |
| Recoverable revocation errors release for replay; initial/periodic or terminal errors fail | runner/save, Kafka/classifyCommit | `TestRevocationCommitFailures`, `TestCommitClassification`, `TestSourceCommitsInitialIdentityBeforeAssigningAndStopsOnFailure` |
| New source partitions may be assigned; no seeding from another group | Kafka positioning | `TestCLIInterruptCommitsAndResumeDoesNotRecopy`; no external-group initialization path |
| Fingerprint includes semantic endpoints/window/cutoff, excludes aliases/paths/policies/tuning | plan/Fingerprint | `TestFingerprintIdentity`, `TestMissingTimestampSkipAndResumeWithDifferentPolicy` |
| Commit metadata validates version, fingerprint, source topic ID, relative window and output identity/extent | Kafka metadata/commit | `TestInvalidProgressNeverBecomesAStartFallback`, `TestSourceValidatesProgressThroughMetadataInterface`, `TestKafkaFileCommitUsesDurableExtent` |
| Actual source recreation between runs fails even when saved offset is in replacement log | Kafka metadata | `TestRecreatedSourceFailsBeforeJoiningEvenWithValidOffsets` |
| One idempotent async producer; original key, partition unset, configurable Java-compatible default | Kafka sink | `TestSinkRoutesRawRecordsAndAttributesDeliveryFailures`, `TestDestinationPartitionChangeWarnsWithoutChangingRecordRouting`, `TestDistributedWorkersFinishAndSoleWorkerConfirms` |
| Queue full retries same record first; native queues bounded; no retry behind later records | runner/submitPending, Kafka sink | `TestRunnerQueueFullPreservesSubmissionOrder`, `TestNativeQueueBoundsAndGoPropertyTypes`, small-cap load case |
| Destination config preflight: timestamp bounds/type and producer size cap; denied config warns | Kafka metadata/sink | `TestTimestampAndHeaderPreflightAreRecordAttributable`, `TestSynchronousSizeLimit`, `TestSizeBoundariesWithCompression`; `TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight` (qualification job) |
| Broker/batch failures never falsely skip innocent records; fatal outcomes fail under every policy | runner/deliveries, Kafka sink | `TestMixedTimestampBatchHasNoRecordAttribution`, `TestSinkRoutesRawRecordsAndAttributesDeliveryFailures` |
| Stable-key routing and ordering within stable producer session; null keys may spread | Kafka sink, runner | `TestDistributedWorkersFinishAndSoleWorkerConfirms`, `TestRunnerQueueFullPreservesSubmissionOrder`; crash/revocation replay exceptions documented |
| LogAppendTime restamps; absent timestamp without filter gets producer clock, counted | Kafka sink | `TestLogAppendTimeDestinationRestampsUnpreservableValues`, `TestMissingTimestampWithoutFilterUsesProducerClockAndCountsIt` |
| Resolve filtered/skipped/delivered records; save contiguous consumed prefix, including numeric gaps | copy/progress, runner | `TestConfirmedPrefixIncludesNumericGaps`, `TestControlRecordCursorAndTimestampStop`, `TestRunnerDoesNotCommitPastFailedWrite` |
| Per-worker/per-partition caps include confirmed work awaiting commit; oversized record alone | runner, config | `TestRunnerCapsIncludeConfirmedWorkAndOversizedAlone`, default/small-cap load cases |
| One coalesced commit; timer, worker/partition half-cap, paused progress, shutdown/revocation; partial results | runner/save/shouldCommit | `TestPartialCommitReleasesSuccessfulPartitionsOnly`, cap/shutdown/revocation runner tests |
| Signal stops intake immediately, ten-second drain saves confirmed prefix, interrupted bounded run fails | app/CLI, runner/drain | `TestSignalDuringPollDoesNotSubmitReturnedRecord`, `TestShutdownDrainDeadlineCommitsConfirmedPrefix`, `TestCLIInterruptCommitsAndResumeDoesNotRecopy` |

## Errors, files and command language

| Rule | Implementation | Evidence |
| --- | --- | --- |
| FAIL default; ON_ERROR mutually exclusive with named policies; SKIP attributable validation only | plan/options, runner/recordError | `TestInvalidPlanning`, `TestMissingTimestampSkipAndResumeWithDifferentPolicy`, Kafka timestamp/header/size tests |
| Corruption, identity changes, authentication, out-of-range, fatal/unattributed failures cannot skip | runner/deliveries, Kafka/source, file/source/codec | `TestDeterministicErrorsNeverRetry`, `TestMixedTimestampBatchHasNoRecordAttribution`, file corruption/identity tests |
| Transient consumer failures have no total deadline; producer/queue wait bounded | Kafka/source, runner/submitPending | `TestTransientSourceErrorsHaveNoDeadline`, queue-full runner tests; tip_wait tests |
| File ID, source shortening or changed last-frame CRC fail; appends accepted | file/source | `TestSourceReplacementFails`, `TestCheckpointAndSourceSnapshot`, `TestSourceCheckpointDetectsChangedLastFrameAndShortenedSource` |
| Any FILE endpoint requires one process on one machine; exclusive output and stable group locks | file/lock/checkpoint/output | `TestOutputLocksAndUnownedHeader`, `TestCheckpointAndSourceSnapshot`, factory lock-release tests; multi-machine files unsupported |
| Magic, bounded canonical CBOR maps/frames, CRC32C, required fields, no compression/segmentation | file/codec | `TestRecordCodecFullFidelity`, `TestHeadersRequireNameAndValue`, `TestEncoderDoesNotCreateUnreadableHeaderArrays`, `FuzzFraming`, `FuzzRecordDecoder` |
| Write/short-write/flush/fsync failure poisons output; no acknowledgment or extent advance | file/output | `TestFileOutputAcknowledgesOnlyAfterBufferAndSyncSucceed`, `TestFailedSyncIsNeverAcknowledged`, `TestOutputWriteFailureCannotAcknowledge` |
| Restart validates all data before E; bad tail after E truncates; too-short output or invalid extent fails | file/output/recoverOutput | `TestOutputAcknowledgmentAndRecovery`, `TestOutputLocksAndUnownedHeader`, `TestZeroFilledOutputTailIsTruncatedOnlyBeyondDurableExtent`, `TestLowerFrameLimitCannotTruncateCompleteUnacknowledgedData` |
| Existing output needs stored matching ID/fingerprint; missing output cannot be recreated from progress | file/factory/output | `TestOutputLocksAndUnownedHeader`, `TestOutputFactoryRejectsMissingIdentityBeforeCreatingFile`, `TestOutputRequiresExistingIdentityAndFrameBoundaryExtent` |
| Durable header, exclusive creation, no-clobber publish, directory fsync, initial progress before records | file/publication, source/Start | `TestCrashAtEveryOutputInitializationBoundary` (13 boundaries), `TestPublicationFailuresNeverPermitUnownedOutput`, Kafka initial-commit tests, `TestSimultaneousOutputCreatorsCannotReplaceWinner` |
| Temporary cleanup respects locks; checkpoint names safe, replacement atomic, parent directory synced | file/publication/checkpoint/lock | `TestTemporaryCleanupRespectsActiveLocks`, `TestCheckpointAndSourceSnapshot`, `TestCheckpointRenameFailurePreservesSavedPositionAndPendingFrames`, initialization crash tests |
| TOML defaults, inline precedence, ENV resolution/redaction, rejected protected native settings | config, plan/connections, Kafka constructors | `TestConfiguration`, `TestLoaderUsesInjectedReaderAndAvoidsReadsForDefaults`, `TestInlinePrecedenceAndSecretRedaction`, queue bounds/preflight mock tests |
| Lexical rules, one statement, SELECT *, topic@alias/FILE cbor(), WHERE/START restrictions, required GROUP | sql/parser/lexer, plan | `TestDocumentedExamples`, `TestLexicalRules`, `TestInvalidStatements`, `TestInvalidPlanning`, `TestInvalidTimeExpressions`, `FuzzParse`, `FuzzWindowNormalization` |
| Exactly one CLI input, size bound, stage-aware exit codes, first failure preserved | app/CLI/command, runner/exit | `TestCLIInputsAndExitCodes`, `TestCommandPlansBeforeExecutingAndStopsOnFailure`, `TestCommandPreservesExecutionExitCode`, `TestExitCodeFollowsExecutionStage`, `TestSignalCannotHideKnownFailure` |

## Engineering and release gates

| Gate | Evidence / remaining work |
| --- | --- |
| SRP structs/interfaces, constructor injection, Fx only at composition | Boundary tests and Fx replacement tests; publication now independently injected; passive models and pure helpers stay concrete |
| Testing pyramid; correctness before optimization | Unit/mock → real-file/Fx → broker/CLI → separate fault/load; no new runtime dependencies |
| Simplicity measured by LOC/complexity | `make metrics`; record updated totals and changes in results |
| Example-based user docs under 10,000 characters | README + docs/file-format char-count CI gate |
| Final platform/static/race/integration/fault/fuzz checks | Passed macOS ARM64, Linux ARM64 and emulated Linux AMD64 vet/race/integration; fault/four fuzz targets; identical macOS rebuild from empty cache |
| Sustained load, memory stability, commit lag, replay and shutdown exposure | `TestStreamingSoakFidelityCommitLagMemoryAndShutdown` (separate `make soak`); passed two-minute local stream, RSS/commit-lag sampling, full fidelity and shutdown checks |
| Restricted ACL/SASL/TLS; multiple brokers with failover | Passed `TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight`, `TestReplicatedClusterLeaderFailurePreservesCopy` (`make qualification`, disposable three-broker TLS/SASL/ACL cluster) |
| Hosted CI | **External blocker:** no Git remote configured; local CI-equivalent gates can run |
| Archive freeze/release candidate | Candidate `v0.1.0-rc.1`; version 1 remains unfrozen until the first final release; broader deployment qualification remains explicit |

## Accepted boundaries

No transformation, Schema Registry remapping, topic auto-creation, external coordinator, done markers, segmented files or viewer. Completion may omit records after a timestamp cutoff; concurrent relative windows may differ. Unassigned subscriptions, deletion during consumption and destination recreation are not fully detectable. File locks are local. Native commit/close and filesystem calls can exceed the write-drain budget. These are recorded design limits, not unchecked claims of complete coverage.
