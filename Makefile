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
# The pins, as Make variables. `--env-file` below hands them to COMPOSE, but
# `make sources` passes PYTHON_VERSION to a script -- and Make cannot expand a
# variable from a file it never included. Measured: without this the generator
# refused with "PYTHON_VERSION is unset" while versions.env plainly set it.
include versions.env
export

# GENERATED, NOT WRITTEN. The vendors this platform starts, their memory
# budgets, their credentials and the addresses the DAG reads all come from
# `contoso-sources/sources.yaml` -- see scripts/sources.py for what a
# hand-written copy of that cost. Gitignored, because it is derived.
FRAGMENT := compose/.sources.generated.yml
COMPOSE := PRODUCT=$(PRODUCT_ABS) PRODUCT_NAME=$(PRODUCT_NAME) SOURCES=$(SOURCES_ABS) PWD=$(CURDIR) \
           docker compose -f compose/docker-compose.yml -f $(FRAGMENT) \
           --env-file versions.env -p $(PROJECT)

.PHONY: help up down witness test lint logs sources doctor
help: ## This list
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t12

sources: ## Generate the vendor stack from whatever sources.yaml declares
	@test -f "$(SOURCES_ABS)/sources.yaml" || { \
	  echo "no sources.yaml at $(SOURCES_ABS) -- the product's vendors cannot be started"; exit 1; }
	@PYTHON_VERSION=$(PYTHON_VERSION) python3 scripts/sources.py \
	  "$(SOURCES_ABS)/sources.yaml" "$(SOURCES_ABS)" > $(FRAGMENT)
	@echo "platform: $$(python3 -c "import json;print(len(json.load(open('$(FRAGMENT)'))['services'])-1)") vendor service(s) declared"

doctor: ## Refuse to start against a product that cannot work
	@command -v docker >/dev/null || { echo "docker is required"; exit 1; }
# THE IMAGE SET, BEFORE ANYTHING STARTS. emulator-sail and
# emulator-spark-agent are packaged BY a fabric-emulator release but carry
# their own upstream versions, so versions.env holds both facts per image.
# Bumping the emulator and missing the two _RELEASE fields leaves every digest
# valid, every service starting, and the components not the set that was
# tested together -- nothing goes red on its own. Stdlib only, so it can
# refuse before a single image is pulled.
	@python3 scripts/check_release_pins.py
	@test -d "$(PRODUCT_ABS)" || { echo "no product at $(PRODUCT_ABS)"; exit 1; }
	@test -f "$(PRODUCT_ABS)/pyproject.toml" || { \
	  echo "platform: $(PRODUCT_ABS) has no pyproject.toml -- the sidecar is built"; \
	  echo "          from the product's dependencies, so set PRODUCT to a real one:"; \
	  echo "          make up PRODUCT=/path/to/a/product"; exit 1; }
	@test -d "$(PRODUCT_ABS)/dags" || { echo "$(PRODUCT_ABS) has no dags/ -- nothing to publish"; exit 1; }
	@echo "doctor: docker present, product $(PRODUCT_NAME) has a pyproject.toml and dags/"

up: sources doctor ## Start Fabric's control plane and the Airflow it hosts
	# THE DEFAULT PRODUCT CANNOT BUILD, and should say so in one line rather
	# than 40 of buildkit output. `tests/fixture-product` exists for the
	# repo-boundary tests, which never start Docker; it carries a DAG and no
	# pyproject.toml, so the sidecar build fails on a missing file with no hint
	# that the real cause is an unset variable. Measured, on the first teardown
	# and rebuild this platform has had.
	# --build, ALWAYS. The sidecar carries the product's dependencies, so a
	# product that adds one and a platform that reuses a cached image disagree
	# silently: the DAG imports something the image does not have and fails at
	# run time with ModuleNotFoundError, three steps from the pyproject.toml
	# that changed. Measured -- adding contoso-data-product did exactly this.
	$(COMPOSE) up -d --wait --build
	@echo "platform: Airflow UI on http://localhost:$${AIRFLOW_UI_PORT:-18085}"

down: ## Stop and remove everything, volumes included
	# NO `-f` HERE, deliberately. Compose can remove everything labelled with the
	# project without being told what the project contains -- and requiring the
	# generated fragment would mean a stack could not be torn down when the
	# sources repo is absent or the fragment was never written. Measured: `make
	# down` failed with "no such file or directory" on a clean checkout, which
	# is the one moment you most want it to work.
	docker compose -p $(PROJECT) down -v

witness: ## Publish a product's DAGs as an ApacheAirflowJob and run one
	uv run --frozen python scripts/witness.py $(PRODUCT)

logs: ## Follow the built-in Airflow's logs
	$(COMPOSE) logs -f airflow

test: ## Repo-boundary tests -- no Docker, no emulator, no credentials
	uv run --frozen --group dev pytest tests -q

lint: ## ruff over the platform
	uv run --frozen --group dev ruff check platform scripts tests
