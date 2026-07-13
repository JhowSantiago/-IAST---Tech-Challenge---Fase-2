"""Transformações da camada Silver (pandas)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Callable

import pandas as pd

PONTO_CORTE_ALFABETIZACAO = 743.0


def _agora() -> datetime:
    return datetime.now(timezone.utc)


def _initcap_serie(serie: pd.Series) -> pd.Series:
    return serie.astype(str).str.strip().str.title()


def _meta_vigente(linha: pd.Series) -> float | None:
    ano = int(linha["ano"])
    coluna = f"meta_alfabetizacao_{ano}"
    if coluna in linha.index and pd.notna(linha[coluna]):
        return float(linha[coluna])
    if pd.notna(linha.get("meta_alfabetizacao_2030")):
        return float(linha["meta_alfabetizacao_2030"])
    return None


def _calcular_gap_meta(df: pd.DataFrame) -> pd.DataFrame:
    resultado = df.copy()
    metas = resultado.apply(_meta_vigente, axis=1)
    resultado["meta_vigente"] = metas
    resultado["gap_meta"] = resultado["taxa_alfabetizacao"] - resultado["meta_vigente"]
    resultado["atingiu_meta"] = resultado["taxa_alfabetizacao"] >= resultado["meta_vigente"]
    return resultado


def transformar_uf(df: pd.DataFrame) -> pd.DataFrame:
    resultado = df.copy()
    resultado["sigla_uf"] = resultado["sigla"].astype(str).str.strip().str.upper()
    resultado["nome_uf"] = _initcap_serie(resultado["nome"])
    resultado["regiao"] = resultado.get("regiao", pd.Series(dtype=str)).astype(str).str.strip()
    resultado = resultado.drop_duplicates(subset=["sigla_uf"])
    return resultado[["id_uf", "sigla_uf", "nome_uf", "regiao"]]


def transformar_municipio(df: pd.DataFrame) -> pd.DataFrame:
    resultado = df.copy()
    resultado["id_municipio"] = resultado["id_municipio"].astype(str).str.zfill(7)
    resultado["sigla_uf"] = resultado["sigla_uf"].astype(str).str.strip().str.upper()
    resultado["nome_municipio"] = resultado["nome"].astype(str).str.strip().str.title()
    resultado["nome_uf"] = resultado.get("nome_uf", pd.Series(dtype=str)).astype(str).str.strip()
    resultado = resultado.drop_duplicates(subset=["id_municipio"])
    return resultado[["id_municipio", "sigla_uf", "nome_municipio", "nome_uf"]]


def transformar_meta_brasil(df: pd.DataFrame) -> pd.DataFrame:
    resultado = df.copy()
    resultado["ano"] = resultado["ano"].astype(int)
    return resultado.drop_duplicates(subset=["ano", "rede"])


def transformar_meta_uf(df: pd.DataFrame) -> pd.DataFrame:
    resultado = df.copy()
    resultado["sigla_uf"] = resultado["sigla_uf"].astype(str).str.strip().str.upper()
    resultado["ano"] = resultado["ano"].astype(int)
    return resultado.drop_duplicates(subset=["sigla_uf", "ano", "rede"])


def transformar_meta_municipio(
    df: pd.DataFrame,
    municipios: pd.DataFrame | None = None,
) -> pd.DataFrame:
    resultado = df.copy()
    resultado["id_municipio"] = resultado["id_municipio"].astype(str).str.zfill(7)
    resultado["ano"] = resultado["ano"].astype(int)
    if municipios is not None and "sigla_uf" not in resultado.columns:
        lookup = municipios[["id_municipio", "sigla_uf"]].drop_duplicates()
        resultado = resultado.merge(lookup, on="id_municipio", how="left")
    elif "sigla_uf" in resultado.columns:
        resultado["sigla_uf"] = resultado["sigla_uf"].astype(str).str.strip().str.upper()
    resultado = resultado.drop_duplicates(subset=["id_municipio", "ano", "rede"])
    if "nivel_alfabetizacao" in resultado.columns:
        resultado["nivel_alfabetizacao"] = resultado["nivel_alfabetizacao"].astype("string")
    resultado = _calcular_gap_meta(resultado)
    if "meta_vigente" in resultado.columns:
        resultado = resultado.drop(columns=["meta_vigente"])
    return resultado


def transformar_alunos(df: pd.DataFrame) -> pd.DataFrame:
    resultado = df.copy()
    resultado["id_municipio"] = resultado["id_municipio"].astype(str).str.zfill(7)
    resultado["id_aluno"] = resultado["id_aluno"].astype(str).str.strip()
    resultado["id_escola"] = resultado.get("id_escola", pd.Series(dtype=str)).astype(str)
    resultado["proficiencia"] = pd.to_numeric(resultado["proficiencia"], errors="coerce")
    alf = resultado.get("alfabetizado")
    if alf is not None:
        resultado["alfabetizado"] = alf.astype(str).str.strip()
    else:
        resultado["alfabetizado"] = None
    derivado = resultado["proficiencia"] >= PONTO_CORTE_ALFABETIZACAO
    resultado.loc[resultado["alfabetizado"].isna(), "alfabetizado"] = derivado.map(
        lambda x: "1" if x else "0"
    )
    resultado["peso_aluno"] = pd.to_numeric(resultado.get("peso_aluno"), errors="coerce")
    resultado = resultado.drop_duplicates(subset=["id_aluno", "ano"])
    colunas = [
        "ano",
        "id_municipio",
        "id_escola",
        "id_aluno",
        "serie",
        "rede",
        "alfabetizado",
        "proficiencia",
        "peso_aluno",
    ]
    return resultado[[c for c in colunas if c in resultado.columns]]


def transformar_indicador_streaming(df: pd.DataFrame) -> pd.DataFrame:
    resultado = df.copy()
    resultado["id_municipio"] = resultado["id_municipio"].astype(str).str.zfill(7)
    resultado["ano"] = resultado["ano"].astype(int)
    resultado = resultado.sort_values("timestamp").drop_duplicates(
        subset=["event_id"], keep="last"
    )
    return resultado


TRANSFORMACOES_PANDAS: dict[str, Callable[..., pd.DataFrame]] = {
    "uf": transformar_uf,
    "municipio": transformar_municipio,
    "meta_brasil": transformar_meta_brasil,
    "meta_uf": transformar_meta_uf,
    "meta_municipio": transformar_meta_municipio,
    "alunos": transformar_alunos,
    "indicador_alfabetizacao": transformar_indicador_streaming,
}

ENTIDADES_SILVER_BATCH = [
    "uf",
    "municipio",
    "meta_brasil",
    "meta_uf",
    "meta_municipio",
    "alunos",
]
