# Kafka Event Platform — common operations
# `make help` lists every target.

.DEFAULT_GOAL := help
.PHONY: help install up down nuke logs topics plan verify-cluster health verify clean

COMPOSE := docker compose -f infra/docker/docker-compose.yml

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pnpm install

up: ## Start infrastructure and wait for health
	$(COMPOSE) up -d
	@echo "Waiting for brokers…"
	@until $(COMPOSE) ps kafka-1 | grep -q healthy; do printf '.'; sleep 2; done
	@echo " ready"

down: ## Stop infrastructure, keep data
	$(COMPOSE) down

nuke: ## Stop infrastructure and DELETE all volumes
	$(COMPOSE) down -v

logs: ## Tail all logs
	$(COMPOSE) logs -f

topics: ## Provision topics from tools/topics.config.ts
	pnpm topics:bootstrap

plan: ## Show topic changes without applying them
	pnpm topics:plan

verify-cluster: ## Assert cluster durability guarantees
	pnpm cluster:verify

health: up topics verify-cluster ## Full bring-up: start, provision, verify

verify: ## Format, lint, typecheck and test
	pnpm verify

clean: ## Remove build artefacts
	rm -rf **/dist **/*.tsbuildinfo coverage
