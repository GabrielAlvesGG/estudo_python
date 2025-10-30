from fastapi import APIRouter, Query, HTTPException
from datetime import date
from typing import Optional

from app.services.pegar_os_arquivos import fetch_all_dicts, fetch_empresas_relatorio

router = APIRouter(
    prefix="/empresas",
    tags=["Empresas"]
)

def _agrupar_empresas(rows):
    # Agrupa por empresa e aninha estabelecimentos
    empresas = {}
    for r in rows:
        e_id = r["e_Id"]
        if e_id not in empresas:
            empresas[e_id] = {
                "id": r["e_Id"],
                "razaoSocial": r["e_RazaoSocial"],
                "nomeFantasia": r["e_NomeFantasia"],
                "cnpjRaiz": r.get("e_CnpjRaiz"),
                "emailContato": r.get("e_EmailContato"),
                "telefoneContato": r.get("e_TelefoneContato"),
                "estabelecimentos": []
            }
        empresas[e_id]["estabelecimentos"].append({
            "id": r["es_Id"],
            "empresaId": r["es_EmpresaId"],
            "cnpj": r["es_Cnpj"],
            "nomeUnidade": r.get("es_NomeUnidade"),
            "isMatriz": r.get("es_IsMatriz")
        })
    return list(empresas.values())

@router.get("/empresas")
def listar_empresas():
    """
    Retorna empresas com seus estabelecimentos em formato aninhado (sem filtros).
    """
    rows = fetch_all_dicts()
    return _agrupar_empresas(rows)

@router.get("/relatorio")
def relatorio_empresas(
    data_entrada: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_saida: date = Query(..., description="Data final (YYYY-MM-DD)"),
    empresa_id: int = Query(..., description="Id da empresa"),
    filial_id: Optional[int] = Query(None, description="Id da filial (Estabelecimento)")
):
    """
    Retorna empresas e estabelecimentos filtrados por período e ids.
    Os filtros são aplicados via WHERE no service.
    """
    if data_entrada > data_saida:
        raise HTTPException(status_code=400, detail="data_entrada não pode ser maior que data_saida")

    rows = fetch_empresas_relatorio(
        data_inicio=data_entrada,
        data_fim=data_saida,
        empresa_id=empresa_id,
        filial_id=filial_id
    )
    return _agrupar_empresas(rows)