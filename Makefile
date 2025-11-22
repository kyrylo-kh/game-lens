init:
	poetry install
	cp -n .env.example .env || true
	poetry run pre-commit install

format:
	poetry run ruff check . --fix

check:
	poetry run ruff check .
	make test

test:
	PYTHONPATH=. poetry run pytest

build-airflow:
	docker build -t gamelens-airflow:latest -f airflow/Dockerfile .

run-airflow:
	docker-compose up airflow-webserver airflow-scheduler -d

run-all:
	docker-compose up -d

run-production:
	docker-compose up airflow-webserver airflow-scheduler superset clickhouse -d

run-%:
	docker-compose up $* -d

airflow-bash:
	docker-compose exec airflow-webserver bash

stop:
	docker-compose stop

restart-airflow:
	docker-compose restart airflow-webserver airflow-scheduler

sync-s3-to-minio:
	aws s3 sync s3://gamelens ./s3_sync
	docker run --rm \
		--network game-lens_default \
		--env-file .env \
		-v ./s3_sync:/data \
		amazon/aws-cli \
		s3 sync /data s3://gamelens \
		--endpoint-url http://minio:9000

clean-duckdb:
	docker-compose exec airflow-webserver rm -f /opt/airflow/storage/etl.duckdb
	@echo "DuckDB cache cleared. S3 secret will be recreated on next connection."
