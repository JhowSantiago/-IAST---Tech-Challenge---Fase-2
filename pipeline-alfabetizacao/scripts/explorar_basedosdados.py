#!/usr/bin/env python3
"""
Exploração das tabelas da Base dos Dados utilizadas na pipeline ICA.

Coleta schema, volume, anos disponíveis e amostras para validar o
dicionário de dados antes da implementação dos jobs de ingestão.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import basedosdados as bd
import pandas as pd
import pydata_google_auth
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import FONTES_BIGQUERY, get_settings  # noqa: E402

SCOPES = ["https://www.googleapis.com/auth/bigquery"]
SAMPLE_LIMIT = 5
OUTPUT_DIR = ROOT / "tests" / "validation" / "samples"

# Papel de cada coluna na modelagem da pipeline
COLUNA_PAPEL: dict[str, str] = {
    "ano": "chave",
    "sigla_uf": "chave",
    "id_municipio": "chave",
    "id_aluno": "chave",
    "id_escola": "chave",
    "rede": "dimensao",
    "serie": "dimensao",
    "taxa_alfabetizacao": "resultado",
    "media_portugues": "resultado",
    "proficiencia": "resultado",
    "alfabetizado": "resultado",
    "peso_aluno": "resultado",
    "percentual_participacao": "resultado",
    "nivel_alfabetizacao": "resultado",
    "presenca": "dimensao",
    "preenchimento_caderno": "dimensao",
    "caderno": "dimensao",
    "nome": "dimensao",
    "nome_uf": "dimensao",
    "nome_municipio": "dimensao",
}

META_PREFIX = "meta_alfabetizacao_"
PROPORCAO_PREFIX = "proporcao_aluno_nivel_"


def _papel_coluna(nome: str) -> str:
    if nome in COLUNA_PAPEL:
        return COLUNA_PAPEL[nome]
    if nome.startswith(META_PREFIX):
        return "meta"
    if nome.startswith(PROPORCAO_PREFIX):
        return "indicador"
    return "atributo"


def _dtype_to_tipo(dtype: Any) -> str:
    nome = str(dtype)
    if "int" in nome.lower():
        return "INTEGER"
    if "float" in nome.lower():
        return "FLOAT"
    if "bool" in nome.lower():
        return "BOOLEAN"
    if "datetime" in nome.lower():
        return "TIMESTAMP"
    return "STRING"


def _bigquery_client() -> bigquery.Client:
    settings = get_settings()
    creds = pydata_google_auth.get_user_credentials(SCOPES)
    return bigquery.Client(project=settings.billing_project_id, credentials=creds)


def _consultar_agregados(client: bigquery.Client, dataset: str, table: str) -> dict[str, Any]:
    tabela = f"`basedosdados.{dataset}.{table}`"
    sql = f"""
    SELECT
        COUNT(*) AS total_linhas,
        COUNT(DISTINCT ano) AS total_anos
    FROM {tabela}
    """
    try:
        row = client.query(sql).to_dataframe().iloc[0]
        total_linhas = int(row["total_linhas"])
        tem_ano = int(row["total_anos"]) > 0
    except Exception:
        sql_sem_ano = f"SELECT COUNT(*) AS total_linhas FROM {tabela}"
        row = client.query(sql_sem_ano).to_dataframe().iloc[0]
        total_linhas = int(row["total_linhas"])
        tem_ano = False

    anos: list[int] = []
    if tem_ano:
        sql_anos = f"SELECT DISTINCT ano FROM {tabela} ORDER BY ano"
        df_anos = client.query(sql_anos).to_dataframe()
        anos = [int(v) for v in df_anos["ano"].tolist()]

    return {"total_linhas": total_linhas, "anos": anos}


def _schema_from_sample(df: pd.DataFrame) -> list[dict[str, Any]]:
    schema: list[dict[str, Any]] = []
    for coluna in df.columns:
        schema.append(
            {
                "nome": coluna,
                "tipo": _dtype_to_tipo(df[coluna].dtype),
                "papel": _papel_coluna(coluna),
                "nulos_amostra": int(df[coluna].isna().sum()),
            }
        )
    return schema


def _duplicatas_chave(df: pd.DataFrame, chaves: list[str]) -> int | None:
    chaves_presentes = [c for c in chaves if c in df.columns]
    if not chaves_presentes:
        return None
    return int(df.duplicated(subset=chaves_presentes).sum())


def explorar_tabela(
    entidade: str,
    dataset: str,
    table: str,
    client: bigquery.Client,
    billing_project_id: str,
) -> dict[str, Any]:
    print(f"Explorando {entidade} ({dataset}.{table})...")

    amostra = bd.read_table(
        dataset,
        table,
        billing_project_id=billing_project_id,
        limit=SAMPLE_LIMIT,
    )
    agregados = _consultar_agregados(client, dataset, table)

    chaves_candidatas = ["id_municipio", "sigla_uf", "ano", "rede", "serie", "id_aluno"]
    duplicatas = _duplicatas_chave(amostra, chaves_candidatas)

    return {
        "entidade": entidade,
        "dataset": dataset,
        "table": table,
        "bigquery_id": f"basedosdados.{dataset}.{table}",
        "total_linhas": agregados["total_linhas"],
        "anos": agregados["anos"],
        "schema": _schema_from_sample(amostra),
        "amostra": amostra.head(SAMPLE_LIMIT).astype(str).to_dict(orient="records"),
        "duplicatas_amostra": duplicatas,
        "nulos_amostra": {col: int(amostra[col].isna().sum()) for col in amostra.columns},
    }


def main() -> None:
    settings = get_settings()
    if not settings.billing_project_id:
        raise SystemExit("BILLING_PROJECT_ID não configurado no .env")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    client = _bigquery_client()

    resultados: list[dict[str, Any]] = []
    for entidade, (dataset, table) in FONTES_BIGQUERY.items():
        resultado = explorar_tabela(
            entidade,
            dataset,
            table,
            client,
            settings.billing_project_id,
        )
        resultados.append(resultado)

    relatorio = {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "billing_project_id": settings.billing_project_id,
        "tabelas": resultados,
    }

    output_path = OUTPUT_DIR / "exploracao_basedosdados.json"
    output_path.write_text(
        json.dumps(relatorio, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nRelatório salvo em {output_path}")
    print(f"Tabelas exploradas: {len(resultados)}")


if __name__ == "__main__":
    main()
