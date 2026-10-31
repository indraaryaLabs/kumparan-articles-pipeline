# Shortcuts. Everything runs inside the Airflow container, so the only thing
# you need on your machine is Docker (on Windows: run these from WSL2).

COMPOSE = docker compose
RUN     = $(COMPOSE) exec airflow-scheduler

.PHONY: help env up down reset seed simulate test hourly backfill reconcile report psql-source psql-warehouse logs

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  make %-15s %s\n", $$1, $$2}'

env: ## create .env with your user id (Linux file permissions)
	@test -f .env || (cp .env.example .env && sed -i.bak "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env && rm -f .env.bak)

up: env ## build and start everything (UI: http://localhost:8080, airflow/airflow)
	mkdir -p data logs
	$(COMPOSE) up -d --build

down: ## stop containers, keep data
	$(COMPOSE) down

reset: ## stop and delete ALL data (databases, parquet files, logs)
	$(COMPOSE) down -v
	rm -rf data logs

seed: ## load ~100k synthetic articles from 2016 until now (ROWS=... to change)
	$(RUN) python source_db/seed.py --rows $(or $(ROWS),100000)

simulate: ## one hour of newsroom activity, stamped now (AT=2026-10-30T09:30:00Z to choose)
	$(RUN) python source_db/simulate.py $(if $(AT),--at $(AT),)

test: ## run the automated tests against the test databases
	$(RUN) python -m pytest -q tests

hourly: ## run one hourly window from the CLI: make hourly START=2026-10-30T09:00Z END=2026-10-30T10:00Z
	$(RUN) python -m pipeline incremental --start $(START) --end $(END)

backfill: ## run the historical backfill from the CLI (the DAG does the same)
	$(RUN) python -m pipeline backfill

reconcile: ## run the daily reconciliation from the CLI
	$(RUN) python -m pipeline reconcile

report: ## run log, table counts, busiest days in the mart
	$(RUN) python -m pipeline report

psql-source: ## open psql on the source database
	$(COMPOSE) exec source-db psql -U postgres -d kumparan

psql-warehouse: ## open psql on the warehouse
	$(COMPOSE) exec warehouse-db psql -U postgres -d warehouse

logs: ## follow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler
