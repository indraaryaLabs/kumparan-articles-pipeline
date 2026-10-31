# AI usage

The assessment allows AI tools as long as their use is stated. I used an AI
assistant (Claude, by Anthropic) as a pair: it did a large share of the
implementation and drafting; I set the direction, ran and verified the
project, and own every decision in this submission.

| Area | My role | AI assistance |
| --- | --- | --- |
| Scope | Defined the target: hourly incremental load, safe re-runs, both bonuses implemented (not only described), and a setup a reviewer can start with one command | Walked through the brief with me and pointed out its traps (soft deletes without `updated_at` changes, late commits, hard deletes) |
| Approach and stack | Chose the approach and the stack (PostgreSQL, Airflow 3, Parquet, Docker Compose) from the options we compared | Compared alternatives (warehouses, orchestrators, CDC vs trigger vs reconciliation) and recommended one |
| Implementation | Directed the work and the constraints | Wrote the code, SQL, DAGs, tests and Docker setup |
| Verification | Ran the full stack on my machine with Docker (`make up`, `make seed`, the DAGs) | Ran the test suite and the timing scenarios in the README in its own environment; the issues under "Found while building" in DECISIONS.md came up there |
| Documentation | Reviewed and approved the final documents | Drafted README.md, DECISIONS.md and docs/bigquery.md |

I can walk through any part of the code and explain why each decision was made.
