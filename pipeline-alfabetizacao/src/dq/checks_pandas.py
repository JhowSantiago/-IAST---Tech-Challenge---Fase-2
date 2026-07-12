"""Verificações de qualidade para DataFrames pandas (execução local)."""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

from src.dq.checks import get_checks

logger = logging.getLogger(__name__)


def checar_qualidade_pandas(df: pd.DataFrame, checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    resultados: list[dict[str, Any]] = []
    falhas_criticas: list[str] = []

    for check in checks:
        tipo = check["tipo"]
        critico = check.get("critico", True)
        nome = f"{tipo}:{check['coluna']}" if "coluna" in check else tipo
        aprovado = True
        detalhe = ""

        if tipo == "not_null":
            coluna = check["coluna"]
            nulos = int(df[coluna].isna().sum())
            aprovado = nulos == 0
            detalhe = f"{nulos} nulos em '{coluna}'"

        elif tipo == "unique":
            coluna = check["coluna"]
            total = len(df)
            distintos = df[coluna].nunique(dropna=False)
            aprovado = total == distintos
            detalhe = f"{total - distintos} duplicatas em '{coluna}'"

        elif tipo == "regex":
            coluna = check["coluna"]
            padrao = check["valor"]
            serie = df[coluna].dropna().astype(str)
            invalidos = int((~serie.map(lambda v: bool(re.fullmatch(padrao, v)))).sum())
            aprovado = invalidos == 0
            detalhe = f"{invalidos} valores fora do padrão {padrao}"

        elif tipo == "range":
            coluna = check["coluna"]
            minimo = check.get("minimo")
            maximo = check.get("maximo")
            serie = df[coluna].dropna()
            fora = 0
            if minimo is not None:
                fora += int((serie < minimo).sum())
            if maximo is not None:
                fora += int((serie > maximo).sum())
            aprovado = fora == 0
            detalhe = f"{fora} valores fora de [{minimo}, {maximo}]"

        elif tipo == "min_count":
            minimo = check["valor"]
            total = len(df)
            aprovado = total >= minimo
            detalhe = f"contagem={total}, mínimo={minimo}"

        else:
            aprovado = False
            detalhe = f"tipo de check desconhecido: {tipo}"
            critico = True

        resultados.append(
            {
                "check": nome,
                "tipo": tipo,
                "critico": critico,
                "aprovado": aprovado,
                "detalhe": detalhe,
            }
        )
        if not aprovado and critico:
            falhas_criticas.append(f"{nome}: {detalhe}")

    if falhas_criticas:
        raise ValueError(
            "Verificações críticas de qualidade falharam:\n- " + "\n- ".join(falhas_criticas)
        )

    return resultados


def checar_entidade_pandas(df: pd.DataFrame, entidade: str, camada: str = "bronze") -> list[dict[str, Any]]:
    return checar_qualidade_pandas(df, get_checks(entidade, camada))
