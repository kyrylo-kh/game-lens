FROM apache/airflow:2.11.0-python3.12

RUN pip install poetry poetry-plugin-export

WORKDIR /opt/airflow

COPY pyproject.toml poetry.lock ./

RUN poetry export --without-hashes -f requirements.txt -o requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY gamelens /opt/airflow/gamelens
COPY .env /opt/airflow/.env
COPY airflow/dags /opt/airflow/airflow/dags

ENV PYTHONPATH=/opt/airflow
