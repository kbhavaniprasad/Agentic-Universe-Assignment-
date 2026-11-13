# AI Usage Disclosure

AI tools are allowed. I used GitHub Copilot as a coding assistant while building the workflow dispatcher solution.

## Tools used
GitHub Copilot (agent + inline suggestions)

## What I used them for
- Reviewed the assignment prompt and mock telephony contract.
- Scaffolded the durable dispatcher and SQLite-backed state model.
- Helped draft the scheduling logic for callback, retry, and fresh-lead priority.
- Suggested and corrected the unit tests around window math and candidate ordering.
- Helped verify import/setup issues and project structure for a clean pytest run.

## What I wrote or decided myself
I designed the lead lifecycle around persisted SQLite state, idempotent request IDs, and a priority-ordered scheduler. I also decided on the final-state precedence and the explicit handling of DND and retry windows in the dispatcher logic.

## Where the AI was wrong or unhelpful
The initial generated scheduler test asserted a priority ordering that ignored the actual retry/callback business rules. I rejected that version and replaced it with a test that matches the assignment semantics and the implemented `scheduler_sort_key_time` rules.

## How I verified AI-generated code
I ran the project test suite with pytest after installing the dependencies and fixed the project import/setup so the solution package resolves from the repo root. I also sanity-checked the scheduling logic against the mock provider's calling-window assumptions.
