# rymeDB Agent Instructions

## Original project prompt (verbatim)

> so previously codex worked on this project ive given the promt pasted text file: /home/deyo/.codex/attachments/9c367494-6352-4658-b802-f3cdd9c977c3/pasted-text-1.txt. Read this file before continuing. and i also gave it pasted text file: /home/deyo/.codex/attachments/9c367494-6352-4658-b802-f3cdd9c977c3/pasted-text-2.txt. Read this file before continuing. but on the way of coding it ive stered it manytimes and now the currrent modules and shit are done only code database features need to be made / modified i want u to check the codebase and continue this project and for each fix/feature i want u to commit as deyoyk auther and with meaning full commit message like u did previouslyt check git log and also i want u to priotize something like i want this database to be fucking ballzingly faster than any other database in the world existing while sstill maintaining the postgress, vulky, realtime, auth shits ok..? u can check web for papers and articles for new algorithm and shit to make this project go ok? also security and vuln need to put at first asw since its an opensource project

## Project-specific working rules

- Before substantial work, read the two project briefs named in the original prompt when available. Use them as product and architecture context; inspect the current repository before assuming a proposed subsystem is still missing.
- Work on database features and fixes in the existing architecture. Preserve PostgreSQL protocol/SQL compatibility, RESP/Valkey compatibility, realtime behavior, auth, and existing module boundaries unless the requested feature requires a coherent change.
- Put security, correctness, data durability, isolation, and safe failure behavior before performance. Treat every network, disk, snapshot, and serialized input as untrusted; validate sizes and bounds before allocating or mutating state.
- Pursue performance through profiling and reproducible, comparable benchmarks. Report hardware, protocol, durability, workload, and latency percentiles. Do not claim universal “fastest” or unsupported multiplicative wins.
- Trace behavior through the layer that owns the invariant. Reuse existing types and helpers; avoid speculative features, unnecessary dependencies, duplicate guards, and unrelated refactors.
- Keep work focused and runnable. Run the smallest relevant tests/checks first, then expand only when risk warrants it. This machine has limited RAM: run expensive builds/tests serially, use Cargo `-j 1` where applicable, and do not launch multiple heavy checks concurrently.
- Preserve all pre-existing worktree changes. Inspect `git status` before editing; stage only files belonging to the completed task. Never discard user data or use destructive cleanup/reset commands without explicit direction.
- For public API changes, update the relevant API specification/OpenAPI and documentation, and validate them. For internal changes, do not add unrelated docs churn.
- After each completed fix or feature, create a descriptive commit authored as `deyoyk <karthikw7755@gmail.com>` and push it to the repository’s current intended branch. Check `git log` and the current branch first. If a repository hook fails on unrelated existing drift, report that evidence; bypass only that unrelated hook after focused checks pass.
- Keep commits scoped. Do not include local data, generated artifacts, secrets, or unrelated user changes.

## Product direction from the briefs

rymeDB is a Rust-first transactional database/backend platform. The briefs describe a memory-optimized hot path plus durable storage, MVCC transactions, replicated/sharded operation, PostgreSQL-facing compatibility, realtime change delivery, authentication/authorization, branching, and operational safety. They are strategic goals, not proof that any capability is complete; verify each feature in current code and tests before changing or documenting it.

Benchmark apples to apples: equivalent semantics, payloads, topology, security, durability/acknowledgement, and client concurrency. Prioritize user-visible tail latency and correctness under failures over synthetic headline QPS.
