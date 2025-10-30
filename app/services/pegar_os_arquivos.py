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

def _is_sqlite(conn) -> bool:
    return conn.__class__.__module__.startswith("sqlite3")

def _table_name(conn) -> str:
    # SQL Server usa schema dbo; SQLite não
    return "FiltroRelatorio" if _is_sqlite(conn) else "dbo.FiltroRelatorio"

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
        # Ajuste para a coluna de data correta do seu modelo (ex.: es.AtualizadaEm)
        "es.CriadaEm between ? and ?"
    ]
    params: List[Any] = [empresa_id, data_inicio, data_fim]
    if filial_id is not None:
        where_clauses.append("es.Id = ?")
        params.append(filial_id)
    sql = f"{base_sql}\nwhere " + " and ".join(where_clauses)

    if _is_sqlite(conn):
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def _buscar_filtro_relatorio(
    conn,
    data_inicio: date,
    data_fim: date,
    empresa_id: int,
    filial_id: Optional[int]
) -> Optional[Dict[str, Any]]:
    """
    Busca o filtro mais recente que combine exatamente com os parâmetros.
    """
    tbl = _table_name(conn)
    params: List[Any] = [empresa_id, data_inicio, data_fim]
    where = ["EmpresaId = ?", "DataInicio = ?", "DataFim = ?"]
    if filial_id is None:
        where.append("FilialId IS NULL")
    else:
        where.append("FilialId = ?")
        params.append(filial_id)

    if _is_sqlite(conn):
        sql = f"""
            select Id, EmpresaId, FilialId, DataInicio, DataFim, Status
            from {tbl}
            where {" and ".join(where)}
            order by AtualizadoEm desc nulls last, Id desc
            limit 1
        """
        # SQLite não suporta 'nulls last'; a ordenação por Id desc já atende
        sql = sql.replace(" nulls last", "")
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None
    else:
        cur = conn.cursor()
        sql = f"""
            select top 1 Id, EmpresaId, FilialId, DataInicio, DataFim, Status
            from {tbl}
            where {" and ".join(where)}
            order by isnull(AtualizadoEm, CriadoEm) desc, Id desc
        """
        cur.execute(sql, params)
        rec = cur.fetchone()
        if not rec:
            return None
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, rec))

def _inserir_filtro_relatorio(
    conn,
    data_inicio: date,
    data_fim: date,
    empresa_id: int,
    filial_id: Optional[int],
    status: str
) -> int:
    """
    Insere um novo registro do filtro e retorna o Id.
    """
    tbl = _table_name(conn)
    if _is_sqlite(conn):
        sql = f"""
            insert into {tbl} (EmpresaId, FilialId, DataInicio, DataFim, Status, CriadoEm)
            values (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        cur = conn.cursor()
        cur.execute(sql, (empresa_id, filial_id, data_inicio, data_fim, status))
        conn.commit()
        return int(cur.lastrowid)
    else:
        cur = conn.cursor()
        sql = f"""
            insert into {tbl} (EmpresaId, FilialId, DataInicio, DataFim, Status, CriadoEm)
            output inserted.Id
            values (?, ?, ?, ?, ?, SYSUTCDATETIME())
        """
        cur.execute(sql, (empresa_id, filial_id, data_inicio, data_fim, status))
        new_id = cur.fetchone()[0]
        conn.commit()
        return int(new_id)

def processar_relatorio_validacao(
    data_inicio: date,
    data_fim: date,
    empresa_id: int,
    filial_id: Optional[int]
) -> Dict[str, Any]:
    """
    - Se já houver filtro com Status EM_ANDAMENTO ou CONCLUIDO, retorna apenas o status.
    - Caso contrário, gera o relatório (consulta) e registra o filtro com Status EM_ANDAMENTO.
    """
    conn = get_db()
    existente = _buscar_filtro_relatorio(conn, data_inicio, data_fim, empresa_id, filial_id)
    if existente:
        status = str(existente.get("Status", "")).upper()
        if status in ("EM_ANDAMENTO", "CONCLUIDO"):
            return {
                "status": status.lower(),
                "filtro_id": existente["Id"],
                "message": "Relatório já está em andamento." if status == "EM_ANDAMENTO" else "Relatório já foi concluído."
            }

    # Não existe (ou não tem status relevante): gera dados e cria o registro EM_ANDAMENTO
    rows = fetch_empresas_relatorio(data_inicio, data_fim, empresa_id, filial_id)
    filtro_id = _inserir_filtro_relatorio(conn, data_inicio, data_fim, empresa_id, filial_id, status="EM_ANDAMENTO")
    return {
        "status": "em_andamento",
        "filtro_id": filtro_id,
        "rows": rows  # o router agregará
    }