from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # project
    environment: str = "local"
    # steam
    steam_api_key: str
    # aws
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str = "eu-central-1"
    aws_endpoint_url: str | None = None
    # postgres
    postgres_user: str
    postgres_password: str
    postgres_db: str
    # superset
    superset_secret_key: str
    superset_load_examples: str = "no"
    superset_database_uri: str
    # clickhouse
    clickhouse_host: str
    clickhouse_db: str
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_default_access_management: str = "1"

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
