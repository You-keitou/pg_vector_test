from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSetting(BaseSettings):
    TIMESCALE_SERVICE_URL: str = "postgres://postgres:passoword@db:5432/postgres"
    PGPASSWORD: str = "password"
    PGUSER: str = "tsdbadmin"
    PGDATABASE: str = "tsdb"
    PGHOST: str = "db"
    PGPORT: int = 5432
    PGSSLMODE: str = "require"
    OPENAI_API_KEY: str = "your-openai-api-key"

    @property
    def DATABASE_URL_SYNC(self) -> str:
        return f"postgresql+psycopg://{self.PGUSER}:{self.PGPASSWORD}@{self.PGHOST}:{self.PGPORT}/{self.PGDATABASE}"

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.PGUSER}:{self.PGPASSWORD}@{self.PGHOST}:{self.PGPORT}/{self.PGDATABASE}"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = AppSetting()
