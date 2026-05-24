.PHONY: test lint typecheck run pre-commit install docker-build docker-push

help:
	@echo "Доступные команды:"
	@echo "  make install      - Установить все зависимости"
	@echo "  make test         - Запустить тесты pytest"
	@echo "  make run          - Запустить приложение"
	@echo "  make lint         - Запустить линтер ruff"
	@echo "  make typecheck    - Запустить проверку типов mypy"
	@echo "  make pre-commit   - Запустить все проверки (lint, typecheck, test)"
	@echo "  make docker-build  - Собрать образ локально (текущая платформа)"
	@echo "  make docker-push   - Собрать multi-platform и запушить в Docker Hub"

install:
	uv sync

test:
	uv run pytest -v

lint:
	uv run ruff check .

typecheck:
	uv run mypy .

run:
	uv run main.py 

pre-commit: lint typecheck test

docker-build:
	docker buildx bake local

docker-push:
	docker buildx bake release
