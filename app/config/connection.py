
import os

from fastapi import FastAPI, HTTPException
import pyodbc

def build_sqlserver_conn_str():
    driver = os.getenv("SQLSERVER_DRIVER", "{ODBC Driver 17 for SQL Server}")
    server = os.getenv("SQLSERVER_SERVER", "localhost")
    database = os.getenv("SQLSERVER_DATABASE", "WESLEY_TESTE")
    trusted = os.getenv("SQLSERVER_TRUSTED_CONNECTION", "no").lower() in ("1", "true", "yes", "y")
    trust_cert = os.getenv("SQLSERVER_TRUST_SERVER_CERTIFICATE", "yes")
    encrypt = os.getenv("SQLSERVER_ENCRYPT", "yes")
    if trusted:
        auth = "Trusted_Connection=yes;"
    else:
        user = "sa"
        password = "masterkey"
        if not user or not password:
            print("Não contém usuario ou senha", user, password)
            raise HTTPException(status_code=500, detail="Defina SQLSERVER_USER e SQLSERVER_PASSWORD ou use SQLSERVER_TRUSTED_CONNECTION=yes.")
        auth = f"UID={user};PWD={password};"
    return f"DRIVER={driver};SERVER={server};DATABASE={database};UID=sa;PWD=masterkey;"


def get_db():
    db_type = os.getenv("DB_TYPE", "sqlite").lower()

    conn_str = build_sqlserver_conn_str()
    print("Conectando ao SQL Server com:", conn_str)
    import pyodbc
    print(pyodbc.drivers())
    return pyodbc.connect(conn_str)