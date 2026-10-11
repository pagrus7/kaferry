# Contributor instructions

- Keep changes focused. Follow the boundaries and testing conventions in [design/components.md](design/components.md).
- For release work, read [releases/README.md](releases/README.md). Commit authored notes before tagging; notes must describe the tagged source and assets, not later main changes. Do not rewrite published tags or binaries when editing notes.
- Known issues lead with the shipped mitigation: the first sentence states the protected cases, resulting behavior and remaining risk, or that no mitigation ships. Then give scope, impact, action and limits; link upstream defects. State when frequency is unknown; passing tests do not establish a production failure rate. Avoid broad readiness labels and investigation backstories. Keep README known issues in their own section after Install.
- Functional tests use Given/When/Then. User documentation stays concise and example-based.
