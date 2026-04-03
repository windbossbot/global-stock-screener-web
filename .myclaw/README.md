# dividend-screener-v3 myclaw Nest

This folder is the project-local `myclaw` operations nest for `dividend-screener-v3`.

## Purpose

1. keep project-local operations docs and state under `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw`
2. keep the shared `myclaw` core protected at `C:\Users\KGWPC\workspace\myclaw`
3. let new Codex sessions reopen from a small local read set instead of scanning the whole project root
4. keep ordinary project work out of the shared engine unless the task explicitly becomes engine admin maintenance

## Local Write Defaults

1. current docs:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs`
2. live state:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state`
3. generated runs:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\runs`
4. small verification outputs:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\smoke`
5. rendered summaries or exports:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\output`
6. reversible local backups:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\backups`

## Core Engine

The shared engine still lives at:

1. `C:\Users\KGWPC\workspace\myclaw`

Use the core engine when reusable scripts, templates, or session/loop helpers are needed.
Treat the shared engine as an admin-only maintenance surface, not as the default project truth root.
Do not copy the full engine into this project-local nest by default.

## Start Here

1. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\START_HERE_CURRENT.md`
2. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\PROJECT_TRUTH_CURRENT.md`
3. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\CONTEXT_ENGINEERING_CURRENT.md`
4. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\SUPPORT_MEANS_POLICY_CURRENT.md`
5. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\SESSION_CONTINUITY_CURRENT.md`
6. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\PROJECT_BOOTSTRAP_CURRENT.md`
