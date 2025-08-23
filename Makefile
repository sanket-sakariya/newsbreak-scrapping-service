.PHONY: help install dev prod test lint format clean docker-build docker-run docker-stop

help: ## Show this help message
	@echo "NewsBreak Scraper API - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements/base.txt

dev: ## Install development dependencies
	pip install -r requirements/dev.txt

prod: ## Install production dependencies
	pip install -r requirements/prod.txt

test: ## Run tests
	pytest

lint: ## Run linting
	flake8 app/
	mypy app/

format: ## Format code
	black app/

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type f -name "*.log" -delete

docker-build: ## Build Docker image
	docker build -f docker/Dockerfile -t newsbreak-scraper-api .

docker-run: ## Run with Docker Compose
	docker-compose -f docker/docker-compose.yml up -d

docker-stop: ## Stop Docker Compose services
	docker-compose -f docker/docker-compose.yml down

run: ## Run the application
	python run.py

run-dev: ## Run the application in development mode
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
