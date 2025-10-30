from app.config.connection import get_db   
from typing import Optional, List, Dict, Any
from datetime import date

def fetch_all_dicts():

    conn = get_db()
    sql = """
            select
                e.Id              as e_Id,
                e.RazaoSocial     as e_RazaoSocial,
                e.NomeFantasia    as e_NomeFantasia,
                e.CnpjRaiz        as e_CnpjRaiz,
                e.EmailContato    as e_EmailContato,
                e.TelefoneContato as e_TelefoneContato,
                es.Id             as es_Id,
                es.EmpresaId      as es_EmpresaId,
                es.Cnpj           as es_Cnpj,
                es.NomeUnidade    as es_NomeUnidade,
                es.IsMatriz       as es_IsMatriz
            from dbo.Empresa e
            inner join dbo.Estabelecimento es
                on e.Id = es.EmpresaId
        """
    
    if conn.__class__.__module__.startswith("sqlite3"):
        rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def fetch_empresas_relatorio(
    data_inicio: date,
    data_fim: date,
    empresa_id: int,
    filial_id: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Retorna empresas e estabelecimentos aplicando filtros.
    Ajuste a coluna de data conforme sua regra (ex.: es.CriadaEm ou es.AtualizadaEm).
    """
    conn = get_db()

    base_sql = """
        select
            e.Id              as e_Id,
            e.RazaoSocial     as e_RazaoSocial,
            e.NomeFantasia    as e_NomeFantasia,
            e.CnpjRaiz        as e_CnpjRaiz,
            e.EmailContato    as e_EmailContato,
            e.TelefoneContato as e_TelefoneContato,
            es.Id             as es_Id,
            es.EmpresaId      as es_EmpresaId,
            es.Cnpj           as es_Cnpj,
            es.NomeUnidade    as es_NomeUnidade,
            es.IsMatriz       as es_IsMatriz
        from dbo.Empresa e
        inner join dbo.Estabelecimento es
            on e.Id = es.EmpresaId
    """

    where_clauses = [
        "e.Id = ?",
        # TODO: ajuste a coluna de data se necessário (ex.: es.AtualizadaEm)
        "es.CriadaEm between ? and ?"
    ]
    params: List[Any] = [empresa_id, data_inicio, data_fim]

    if filial_id is not None:
        where_clauses.append("es.Id = ?")
        params.append(filial_id)

    sql = f"{base_sql}\nwhere " + " and ".join(where_clauses)

    # Execução compatível com sqlite e pyodbc
    if conn.__class__.__module__.startswith("sqlite3"):
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]