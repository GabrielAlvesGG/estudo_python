from fastapi import APIRouter, Query, HTTPException
from datetime import date
from typing import Optional

from app.services.pegar_os_arquivos import fetch_all_dicts, fetch_empresas_relatorio, processar_relatorio_validacao

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
    - Se já existir um filtro igual com status EM_ANDAMENTO/CONCLUIDO, retorna o status.
    - Caso contrário, gera o relatório e salva o filtro com status EM_ANDAMENTO.
    """
    if data_entrada > data_saida:
        raise HTTPException(status_code=400, detail="data_entrada não pode ser maior que data_saida")

    result = processar_relatorio_validacao(
        data_inicio=data_entrada,
        data_fim=data_saida,
        empresa_id=empresa_id,
        filial_id=filial_id
    )

    if "rows" not in result:
        # Já havia um registro na tabela de filtros
        return {
            "status": result["status"],
            "filtro_id": result["filtro_id"],
            "message": result.get("message", "")
        }

    # Nova geração: agrega os dados e retorna junto com o status EM_ANDAMENTO
    agregados = _agrupar_empresas(result["rows"])
    return {
        "status": "em_andamento",
        "filtro_id": result["filtro_id"],
        "resultado": agregados
    }