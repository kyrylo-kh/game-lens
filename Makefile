init:
	poetry install
	cp -n .env.example .env || true
	poetry run pre-commit install

format:
	poetry run ruff check . --fix

check:
	poetry run ruff check .
	poetry run mypy .
	poetry run pytest

test:
	poetry run pytest
