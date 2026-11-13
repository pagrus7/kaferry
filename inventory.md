# Maintainability inventory

Baseline: `origin/hardening/make-cleanup` at `e4ef4eb93ec6e117325f815157aeff82679187a3`.
Started 2026-11-13 14:29:23 UTC. Inventory in progress; no removal PR is authorized by an incomplete row.

Keep tests that assert product behavior, reject evidence that could otherwise create a false green, or guard release safety. Keep uncertain cases with an explanation.

| Path | Test or function | Lines | Category | Verdict | One-line reason | PR |
| --- | --- | ---: | --- | --- | --- | --- |
