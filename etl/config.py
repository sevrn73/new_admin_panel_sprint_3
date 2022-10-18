from pydantic import BaseSettings, Field


class DSL(BaseSettings):
    dbname: str = Field('', env="POSTGRES_DB")
    user: str = Field('', env="POSTGRES_USER")
    password: str = Field('', env="POSTGRES_PASSWORD")
    host: str = Field("db", env="DB_HOST")
    port: int = Field(5432, env="DB_PORT")

class ES(BaseSettings):
    es_host: str = Field('', env="ES_HOST")
    es_user: str = Field('', env="ES_USER")
    es_password: str = Field('', env="ES_PASSWORD")