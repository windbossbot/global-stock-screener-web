# CONTEXT_ENGINEERING_CURRENT

Updated: 2026-04-03

## Purpose

Keep `dividend-screener-v3` operating on the smallest high-signal context package for each bounded step.

## Default Rule

1. treat `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw` as the local context home
2. do not treat the shared engine at `C:\Users\KGWPC\workspace\myclaw` as the default truth root for this project
3. prefer explicit context packages over broad transcript carry-forward

## Four Operating Verbs

1. `write`
   - persist durable facts to `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs` and `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state`
2. `select`
   - read only the files, tools, and examples needed for the current bounded order
3. `compress`
   - summarize long histories into current docs, continuity notes, or workline reports
4. `isolate`
   - keep this project separate from the shared engine and unrelated project scopes

## Local Context Package

Before real work, keep these explicit:

1. current goal
2. bounded path scope
3. active truth docs/state
4. minimal helper/tool set
5. deferred roots that should stay out unless needed later

## Practical Rule

1. write important carry-forward facts out of chat memory early
2. prefer exact path reads and scoped search over broad rereads
3. compress stale history before it crowds out active work
4. if the task changes projects or modes, reopen or split context instead of blending scopes
