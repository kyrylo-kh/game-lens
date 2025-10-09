from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    steam_api_key: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str = "eu-central-1"

    class Config:
        env_file = ".env"

settings = Settings()
