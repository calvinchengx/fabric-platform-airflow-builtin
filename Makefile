# The platform, driven from a product repo:  make witness PRODUCT=../my-product
#
# PRODUCT is a PATH, not a name. This Makefile contains no product identifier,
# which is the property that makes "a second product can use this unchanged" a
# fact rather than an aspiration.
SHELL := /bin/bash
PRODUCT ?= ./tests/fixture-product
PROJECT ?= fab-airflow-builtin
COMPOSE := docker compose -f compose/docker-compose.yml --env-file versions.env -p $(PROJECT)

.PHONY: help up down witness test lint logs
help: ## This list
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t12

up: ## Start Fabric's control plane and the Airflow it hosts
	$(COMPOSE) up -d --wait
	@echo "platform: Airflow UI on http://localhost:$${AIRFLOW_UI_PORT:-18085}"

down: ## Stop and remove everything, volumes included
	$(COMPOSE) down -v

witness: ## Publish a product's DAGs as an ApacheAirflowJob and run one
	uv run --frozen python scripts/witness.py $(PRODUCT)

logs: ## Follow the built-in Airflow's logs
	$(COMPOSE) logs -f airflow

test: ## Repo-boundary tests -- no Docker, no emulator, no credentials
	uv run --frozen --group dev pytest tests -q

lint: ## ruff over the platform
	uv run --frozen --group dev ruff check platform scripts tests
