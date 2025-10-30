from fastapi import FastAPI, HTTPException
import os
import sqlite3
from app.routes import routes_tela_relatorio

app = FastAPI()

# inclui as rotas do módulo empresas
app.include_router(routes_tela_relatorio.router)


