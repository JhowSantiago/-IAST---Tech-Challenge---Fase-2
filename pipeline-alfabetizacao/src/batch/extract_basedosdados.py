"""Extração batch de entidades da Base dos Dados (BigQuery)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import basedosdados as bd
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import ENTIDADES_BATCH, get_settings  # noqa: E402
from src.common.logging_config import get_logger  # noqa: E402

logger = get_logger(__name__)
QUERIES_DIR = Path(__file__).resolve().parent / "queries"
STAGING_DIR = ROOT / "data" / "staging"

COLUNAS_STRING = {
    "id_municipio": 7,
    "id_uf": None,
    "id_escola": None,
    "id_aluno": None,
}


def _normalizar_dataframe(df: pd.DataFrame, entidade: str) -> pd.DataFrame:
    """Garante tipos compatíveis com o schema bronze."""
    for coluna, largura in COLUNAS_STRING.items():
        if coluna in df.columns:
            serie = df[coluna].astype("string")
            if largura:
                serie = serie.str.zfill(largura)
            df[coluna] = serie
    return df


def _ler_query(entidade: str) -> str:
    query_path = QUERIES_DIR / f"{entidade}.sql"
    if not query_path.exists():
        raise FileNotFoundError(f"Query não encontrada: {query_path}")
    return query_path.read_text(encoding="utf-8").strip()


def extrair(entidade: str, destino: Path | None = None) -> pd.DataFrame:
    """Executa a query SQL da entidade e persiste Parquet em staging."""
    settings = get_settings()
    if not settings.billing_project_id:
        raise ValueError("BILLING_PROJECT_ID não configurado no .env")

    query = _ler_query(entidade)
    logger.info("Extraindo entidade '%s' via BigQuery", entidade)

    df = bd.read_sql(query, billing_project_id=settings.billing_project_id)
    df = _normalizar_dataframe(df, entidade)

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    output = destino or (STAGING_DIR / f"{entidade}.parquet")
    df.to_parquet(output, index=False, compression="snappy")

    logger.info(
        "Entidade '%s' extraída: %s linhas, %s colunas → %s",
        entidade,
        len(df),
        len(df.columns),
        output,
    )
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Extração batch — Base dos Dados")
    parser.add_argument("--entidade", choices=ENTIDADES_BATCH, help="Entidade a extrair")
    parser.add_argument("--todas", action="store_true", help="Extrai as 6 entidades batch")
    args = parser.parse_args()

    if not args.entidade and not args.todas:
        parser.error("Informe --entidade ou --todas")

    entidades = ENTIDADES_BATCH if args.todas else [args.entidade]
    for entidade in entidades:
        extrair(entidade)


if __name__ == "__main__":
    main()
