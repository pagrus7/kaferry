# Contributor instructions

- Keep changes focused. Follow [component boundaries](docs/design/components.md) and the [contributor guide](CONTRIBUTING.md) for testing and documentation.
- Describe current behavior in plain words. Verify changed claims against implementation and evidence; put release history in release notes, commits or PRs.
- Keep user workflows self-contained, with prerequisites and relevant caveats beside examples. Users must not need to read design documents to operate Kaferry. Follow the [documentation guidance](CONTRIBUTING.md#documentation).
- Follow the shared [review process](docs/development/reviews/README.md), including reviewer selection, PR descriptions and follow-ups. Keep scope, evidence and reports locally.
- For releases, follow the [release procedure](docs/releases/README.md). Never rewrite published tags or binaries when correcting notes.
- Prefer deleting or simplifying implementation, tests and docs. Extra layers, options, state or future features need a current requirement, demonstrated failure or compatibility contract. Preserve correctness and useful component boundaries; fewer lines alone do not justify a change.
