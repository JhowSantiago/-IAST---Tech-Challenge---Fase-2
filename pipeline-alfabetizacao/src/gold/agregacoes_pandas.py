"""Agregações analíticas da camada Gold (pandas)."""

from __future__ import annotations

import pandas as pd

VISAO_INDICADOR_MUNICIPIO = "indicador_municipio"
VISAO_META_VS_RESULTADO = "meta_vs_resultado"
VISAO_EVOLUCAO_TEMPORAL = "evolucao_temporal"

VISOES_GOLD = [
    VISAO_INDICADOR_MUNICIPIO,
    VISAO_META_VS_RESULTADO,
    VISAO_EVOLUCAO_TEMPORAL,
]


def _meta_vigente(linha: pd.Series) -> float | None:
    ano = int(linha["ano"])
    coluna = f"meta_alfabetizacao_{ano}"
    if coluna in linha.index and pd.notna(linha[coluna]):
        return float(linha[coluna])
    if pd.notna(linha.get("meta_alfabetizacao_2030")):
        return float(linha["meta_alfabetizacao_2030"])
    return None


def _base_integrada(integrado: pd.DataFrame) -> pd.DataFrame:
    """Usa registros batch; dedup por município/ano/rede."""
    base = integrado.loc[integrado.get("_source_type", "batch").eq("batch")].copy()
    if base.empty:
        base = integrado.drop_duplicates(subset=["id_municipio", "ano", "rede"], keep="first").copy()
    else:
        base = base.drop_duplicates(subset=["id_municipio", "ano", "rede"], keep="first")
    base["meta_vigente"] = base.apply(_meta_vigente, axis=1)
    base["gap_meta"] = (base["taxa_alfabetizacao"] - base["meta_vigente"]).round(2)
    base["atingiu_meta"] = pd.NA
    com_taxa = base["taxa_alfabetizacao"].notna() & base["meta_vigente"].notna()
    base.loc[com_taxa, "atingiu_meta"] = (
        base.loc[com_taxa, "taxa_alfabetizacao"] >= base.loc[com_taxa, "meta_vigente"]
    )
    return base


def agregar_indicador_municipio(
    integrado: pd.DataFrame,
    alunos: pd.DataFrame | None = None,
) -> pd.DataFrame:
    base = _base_integrada(integrado)
    gold = base.rename(
        columns={
            "nome_municipio": "nome",
            "taxa_alfabetizacao": "pct_alfabetizados",
            "meta_vigente": "meta_pct",
        }
    )
    colunas = [
        "id_municipio",
        "nome",
        "sigla_uf",
        "nome_uf",
        "ano",
        "rede",
        "pct_alfabetizados",
        "meta_pct",
        "gap_meta",
        "atingiu_meta",
        "indicador_uf",
        "nivel_alfabetizacao",
        "percentual_participacao",
    ]
    gold = gold[[c for c in colunas if c in gold.columns]]

    if alunos is not None and not alunos.empty:
        totais = (
            alunos.groupby(["id_municipio", "ano"], as_index=False)
            .agg(total_alunos_avaliados=("id_aluno", "count"))
        )
        totais["id_municipio"] = totais["id_municipio"].astype("string")
        gold["id_municipio"] = gold["id_municipio"].astype("string")
        gold = gold.merge(totais, on=["id_municipio", "ano"], how="left")

    return gold.sort_values(["ano", "sigla_uf", "nome"]).reset_index(drop=True)


def agregar_meta_vs_resultado(
    integrado: pd.DataFrame,
    uf: pd.DataFrame | None = None,
) -> pd.DataFrame:
    base = _base_integrada(integrado)
    por_uf = (
        base.groupby(["sigla_uf", "ano"], as_index=False)
        .agg(
            taxa_media=("taxa_alfabetizacao", "mean"),
            meta_media=("meta_vigente", "mean"),
            gap_medio=("gap_meta", "mean"),
            municipios_total=("id_municipio", "nunique"),
            municipios_acima_meta=("atingiu_meta", lambda s: int(s.sum())),
            municipios_abaixo_meta=("atingiu_meta", lambda s: int((~s).sum())),
        )
    )
    por_uf["taxa_media"] = por_uf["taxa_media"].round(2)
    por_uf["meta_media"] = por_uf["meta_media"].round(2)
    por_uf["gap_medio"] = por_uf["gap_medio"].round(2)
    por_uf["ranking_taxa"] = (
        por_uf.groupby("ano")["taxa_media"].rank(ascending=False, method="dense").astype("Int64")
    )

    por_uf = por_uf.sort_values(["sigla_uf", "ano"])
    por_uf["taxa_anterior"] = por_uf.groupby("sigla_uf")["taxa_media"].shift(1)
    por_uf["delta_percentual"] = (por_uf["taxa_media"] - por_uf["taxa_anterior"]).round(2)

    if uf is not None and "regiao" in uf.columns:
        lookup = uf[["sigla_uf", "regiao", "nome_uf"]].drop_duplicates()
        por_uf = por_uf.merge(lookup, on="sigla_uf", how="left")

    return por_uf.reset_index(drop=True)


def agregar_evolucao_temporal(integrado: pd.DataFrame) -> pd.DataFrame:
    base = _base_integrada(integrado)
    evolucao = base.rename(
        columns={
            "nome_municipio": "nome",
            "taxa_alfabetizacao": "pct_alfabetizados",
            "meta_vigente": "meta_pct",
        }
    )
    evolucao = evolucao.sort_values(["id_municipio", "rede", "ano"])
    evolucao["pct_anterior"] = evolucao.groupby(["id_municipio", "rede"])["pct_alfabetizados"].shift(1)
    evolucao["delta_percentual"] = (evolucao["pct_alfabetizados"] - evolucao["pct_anterior"]).round(2)
    evolucao["delta_anual"] = evolucao["delta_percentual"]

    colunas = [
        "id_municipio",
        "nome",
        "sigla_uf",
        "nome_uf",
        "rede",
        "ano",
        "pct_alfabetizados",
        "pct_anterior",
        "delta_percentual",
        "delta_anual",
        "meta_pct",
        "gap_meta",
        "atingiu_meta",
    ]
    return evolucao[[c for c in colunas if c in evolucao.columns]].reset_index(drop=True)


AGREGACOES_PANDAS = {
    VISAO_INDICADOR_MUNICIPIO: agregar_indicador_municipio,
    VISAO_META_VS_RESULTADO: agregar_meta_vs_resultado,
    VISAO_EVOLUCAO_TEMPORAL: agregar_evolucao_temporal,
}
