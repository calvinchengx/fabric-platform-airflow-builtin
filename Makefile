# The platform, driven from a product repo:  make witness PRODUCT=../my-product
#
# PRODUCT is a PATH, not a name. This Makefile contains no product identifier,
# which is the property that makes "a second product can use this unchanged" a
# fact rather than an aspiration.
SHELL := /bin/bash
PRODUCT ?= ./tests/fixture-product
PROJECT ?= fab-airflow-builtin
# PRODUCT is a PATH. Its parent is the build context and its basename is what
# the Dockerfile copies from, so a product anywhere on disk works and this
# Makefile still names none.
export PRODUCT_ABS := $(abspath $(PRODUCT))
export PRODUCT_NAME := $(notdir $(PRODUCT_ABS))
# The vendors are contoso-sources', mounted rather than copied.
SOURCES ?= ../contoso-sources
export SOURCES_ABS := $(abspath $(SOURCES))
# THE VENDOR PUBLISHES ITS OWN KEY. Read from the fixture the vendor serves
# rather than written here: this platform stores no credential, and the value
# is the vendor's to change.
export CONTOSO_POS_API_KEY := $(shell cat $(SOURCES_ABS)/_data/contoso-pos/.api-key 2>/dev/null)
COMPOSE := PRODUCT=$(PRODUCT_ABS) PRODUCT_NAME=$(PRODUCT_NAME) SOURCES=$(SOURCES_ABS) \
           CONTOSO_POS_API_KEY=$(CONTOSO_POS_API_KEY) PWD=$(CURDIR) \
           docker compose -f compose/docker-compose.yml --env-file versions.env -p $(PROJECT)

.PHONY: help up down witness test lint logs
help: ## This list
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t12

up: ## Start Fabric's control plane and the Airflow it hosts
	# --build, ALWAYS. The sidecar carries the product's dependencies, so a
	# product that adds one and a platform that reuses a cached image disagree
	# silently: the DAG imports something the image does not have and fails at
	# run time with ModuleNotFoundError, three steps from the pyproject.toml
	# that changed. Measured -- adding contoso-data-product did exactly this.
	$(COMPOSE) up -d --wait --build
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
