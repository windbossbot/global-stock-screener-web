# PROJECT_BOOTSTRAP_CURRENT

Updated: 2026-04-03

## Purpose

Provide a small copy-paste bootstrap surface for fresh sessions that should work inside the project-local `myclaw` nest first.
The standard order is:

1. complete the minimal local bootstrap
2. save the default local docs and state immediately
3. start the local bootstrap-direction state for the early question window
4. ask the user a few short direction-setting questions
5. then continue with real work

## Copy-Paste Prompt

```text
Read C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\START_HERE_CURRENT.md first.

Treat C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw as the project-local myclaw operations home for dividend-screener-v3.
Keep new current operations docs under C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs and local volatile state under C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state.
Treat the shared engine at C:\Users\KGWPC\workspace\myclaw as an admin-only maintenance surface.
Use it only when reusable scripts, templates, or deeper loop/session helpers are actually needed, and do not treat it as the default truth root for this project.
Use context engineering by default: write carry-forward facts to local docs/state, select only the relevant local files and helpers, compress long history into compact notes, and isolate this project from the shared engine and unrelated scopes.
During the first roughly 5 to 10 user questions, recommend useful scripts, skills, templates, or helper agents a bit more proactively, and mark actual use with short square-bracket tags like `[judge_request.py 스크립트]` or `[myclaw-admin-mode 스킬]`.
Keep the early question window stateful through `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state\bootstrap_direction_state.json` instead of relying only on chat memory.
Do not create new repo-level operations docs by default.
Complete the minimal local bootstrap and save the default local docs/state first.
After that, ask the user only the short direction questions listed at C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\BOOTSTRAP_DIRECTION_QUESTIONS_CURRENT.md before deeper work begins.
Return short Korean completion-first reports: current goal, what was actually done, confirmed facts, impact or changes, and risks or judgment only when needed.
```

## Practical Commands

Project-local nest only:

```powershell
python "C:\Users\KGWPC\workspace\myclaw\scripts\bootstrap_myclaw_nest.py" --project-root "C:\Users\KGWPC\workspace\dividend-screener-v3" --apply
```

If the project later also needs repo-level ops docs:

```powershell
python "C:\Users\KGWPC\workspace\myclaw\scripts\install_ops_ready_seed.py" --project-root "C:\Users\KGWPC\workspace\dividend-screener-v3" --project-name "dividend-screener-v3" --apply
```

## Rule

Bootstrap the local nest first.
Save the default local current docs and local state immediately.
Start the local bootstrap-direction state before deeper routing.
Only after that ask the user short direction-setting questions.
Only add wider repo-level ops material if the project proves that the extra surface is worth it.
Do not switch the active truth root back to the shared engine unless the task explicitly becomes engine admin maintenance.
