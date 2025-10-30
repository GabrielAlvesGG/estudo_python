from typing import Optional
from pydantic import BaseSettings

_V2 = False
try:
    from pydantic import BaseSettings, ConfigDict as SettingsConfigDict
    _V2 = True
except ImportError:
    from pydantic import BaseSettings

class Settings(BaseSettings):
    app_name: str = "Teste API"
    env: str = "dev"
    # Config DB
    db_type: str = "sqlserver"  # opções: "sqlserver" | "sqlite"
    sqlserver_driver: str = "{ODBC Driver 17 for SQL Server}"
    sqlserver_server: str = "localhost"
    sqlserver_database: str = "WESLEY_TESTE"
    sqlserver_trusted_connection: bool = False
    sqlserver_user: Optional[str] = "sa"
    sqlserver_password: Optional[str] = "masterkey"
    sqlserver_encrypt: str = "yes"
    sqlserver_trust_server_certificate: str = "yes"
    sqlite_path: Optional[str] = None

    if _V2:
        model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    else:
        class Config:
            env_file = ".env"
            env_file_encoding = "utf-8"

settings = Settings()