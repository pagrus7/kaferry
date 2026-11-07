# Endurance test results

**[Open the endurance results page](https://pagrus7.github.io/kaferry/)** for the latest published run, run history, delivery and resource measurements, workload limits and downloadable evidence.

The page is built from the `endurance-results.json` assets uploaded to stable GitHub releases. It verifies each JSON against its published checksums and retains failed, interrupted and rehearsal results with their status. Missing measurements remain unavailable. New runs require no Markdown report or result snapshot committed to the repository.

Publishing requests a refresh of the [endurance-results workflow](https://github.com/pagrus7/kaferry/actions/workflows/endurance-results.yml). Hourly discovery also picks up uploads from older runners or manual uploads. A failed build leaves the previously deployed page in place; check the workflow for refresh failures.

[Run and publish an endurance test](design/qualification.md#publish-endurance-results). The [development rehearsal](endurance/2026-11-06-linux-arm64-rehearsal/endurance-results.md) is separate from release evidence. A run shorter than 24 hours does not establish 24 hours of continuous uptime.
