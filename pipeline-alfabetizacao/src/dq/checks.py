"""Regras de qualidade de dados — padrão CHECKS do curso ETL."""

from __future__ import annotations

import logging
import re
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

CheckDict = dict[str, Any]

CHECKS: dict[str, dict[str, list[CheckDict]]] = {
    "uf": {
        "bronze": [
            {"tipo": "not_null", "coluna": "sigla", "critico": True},
            {"tipo": "unique", "coluna": "sigla", "critico": True},
            {"tipo": "regex", "coluna": "sigla", "valor": r"^[A-Z]{2}$", "critico": True},
            {"tipo": "min_count", "valor": 27, "critico": True},
        ],
        "silver": [
            {"tipo": "not_null", "coluna": "sigla_uf", "critico": True},
            {"tipo": "unique", "coluna": "sigla_uf", "critico": True},
            {"tipo": "regex", "coluna": "sigla_uf", "valor": r"^[A-Z]{2}$", "critico": True},
        ],
        "gold": [
            {"tipo": "min_count", "valor": 1, "critico": True},
            {"tipo": "not_null", "coluna": "sigla_uf", "critico": True},
        ],
    },
    "municipio": {
        "bronze": [
            {"tipo": "not_null", "coluna": "id_municipio", "critico": True},
            {"tipo": "regex", "coluna": "id_municipio", "valor": r"^\d{7}$", "critico": True},
            {"tipo": "not_null", "coluna": "sigla_uf", "critico": True},
            {"tipo": "min_count", "valor": 1000, "critico": True},
        ],
        "silver": [
            {"tipo": "not_null", "coluna": "id_municipio", "critico": True},
            {"tipo": "unique", "coluna": "id_municipio", "critico": True},
            {"tipo": "regex", "coluna": "id_municipio", "valor": r"^\d{7}$", "critico": True},
            {"tipo": "not_null", "coluna": "sigla_uf", "critico": True},
        ],
        "gold": [
            {"tipo": "min_count", "valor": 1, "critico": True},
            {"tipo": "not_null", "coluna": "id_municipio", "critico": True},
        ],
    },
    "meta_brasil": {
        "bronze": [
            {"tipo": "not_null", "coluna": "ano", "critico": True},
            {"tipo": "range", "coluna": "ano", "minimo": 2023, "maximo": 2030, "critico": True},
            {"tipo": "not_null", "coluna": "taxa_alfabetizacao", "critico": True},
            {"tipo": "range", "coluna": "taxa_alfabetizacao", "minimo": 0, "maximo": 100, "critico": True},
        ],
        "silver": [
            {"tipo": "not_null", "coluna": "ano", "critico": True},
            {"tipo": "not_null", "coluna": "meta_alfabetizacao_2030", "critico": True},
            {"tipo": "range", "coluna": "meta_alfabetizacao_2030", "minimo": 0, "maximo": 100, "critico": True},
        ],
        "gold": [
            {"tipo": "min_count", "valor": 1, "critico": True},
            {"tipo": "not_null", "coluna": "taxa_alfabetizacao", "critico": True},
            {"tipo": "not_null", "coluna": "meta_vigente", "critico": True},
        ],
    },
    "meta_uf": {
        "bronze": [
            {"tipo": "not_null", "coluna": "sigla_uf", "critico": True},
            {"tipo": "not_null", "coluna": "ano", "critico": True},
            {"tipo": "range", "coluna": "taxa_alfabetizacao", "minimo": 0, "maximo": 100, "critico": True},
        ],
        "silver": [
            {"tipo": "not_null", "coluna": "sigla_uf", "critico": True},
            {"tipo": "not_null", "coluna": "ano", "critico": True},
            {"tipo": "not_null", "coluna": "taxa_alfabetizacao", "critico": False},
            {"tipo": "range", "coluna": "taxa_alfabetizacao", "minimo": 0, "maximo": 100, "critico": True},
        ],
        "gold": [
            {"tipo": "min_count", "valor": 1, "critico": True},
            {"tipo": "not_null", "coluna": "sigla_uf", "critico": True},
            {"tipo": "not_null", "coluna": "gap_meta", "critico": False},
        ],
    },
    "meta_municipio": {
        "bronze": [
            {"tipo": "not_null", "coluna": "id_municipio", "critico": True},
            {"tipo": "regex", "coluna": "id_municipio", "valor": r"^\d{7}$", "critico": True},
            {"tipo": "not_null", "coluna": "ano", "critico": True},
            {"tipo": "range", "coluna": "taxa_alfabetizacao", "minimo": 0, "maximo": 100, "critico": True},
        ],
        "silver": [
            {"tipo": "not_null", "coluna": "id_municipio", "critico": True},
            {"tipo": "not_null", "coluna": "ano", "critico": True},
            {"tipo": "not_null", "coluna": "taxa_alfabetizacao", "critico": False},
            {"tipo": "range", "coluna": "taxa_alfabetizacao", "minimo": 0, "maximo": 100, "critico": True},
            {"tipo": "not_null", "coluna": "gap_meta", "critico": False},
        ],
        "gold": [
            {"tipo": "min_count", "valor": 1, "critico": True},
            {"tipo": "not_null", "coluna": "id_municipio", "critico": True},
            {"tipo": "not_null", "coluna": "taxa_alfabetizacao", "critico": True},
            {"tipo": "not_null", "coluna": "meta_vigente", "critico": True},
        ],
    },
    "alunos": {
        "bronze": [
            {"tipo": "not_null", "coluna": "id_aluno", "critico": True},
            {"tipo": "not_null", "coluna": "id_municipio", "critico": True},
            {"tipo": "regex", "coluna": "id_municipio", "valor": r"^\d{7}$", "critico": True},
            {"tipo": "min_count", "valor": 1, "critico": True},
        ],
        "silver": [
            {"tipo": "not_null", "coluna": "id_aluno", "critico": True},
            {"tipo": "not_null", "coluna": "id_municipio", "critico": True},
            {"tipo": "range", "coluna": "proficiencia", "minimo": 0, "maximo": 1000, "critico": False},
        ],
        "gold": [
            {"tipo": "min_count", "valor": 1, "critico": True},
            {"tipo": "not_null", "coluna": "total_alunos", "critico": True},
            {"tipo": "not_null", "coluna": "taxa_alfabetizacao_calc", "critico": True},
        ],
    },
}


def get_checks(entidade: str, camada: str) -> list[CheckDict]:
    """Retorna as regras CHECKS de uma entidade e camada."""
    return CHECKS[entidade][camada.lower()]


def checar_qualidade(df: DataFrame, checks: list[CheckDict]) -> list[dict[str, Any]]:
    """
    Executa verificações de qualidade sobre um DataFrame Spark.

    Retorna lista de resultados. Levanta exceção se alguma regra crítica falhar.
    """
    resultados: list[dict[str, Any]] = []
    falhas_criticas: list[str] = []

    for check in checks:
        tipo = check["tipo"]
        critico = check.get("critico", True)
        nome = f"{tipo}"
        if "coluna" in check:
            nome = f"{tipo}:{check['coluna']}"

        aprovado = True
        detalhe = ""

        if tipo == "not_null":
            coluna = check["coluna"]
            nulos = df.filter(F.col(coluna).isNull()).count()
            aprovado = nulos == 0
            detalhe = f"{nulos} nulos em '{coluna}'"

        elif tipo == "unique":
            coluna = check["coluna"]
            total = df.count()
            distintos = df.select(coluna).distinct().count()
            aprovado = total == distintos
            detalhe = f"{total - distintos} duplicatas em '{coluna}'"

        elif tipo == "regex":
            coluna = check["coluna"]
            padrao = check["valor"]
            invalidos = df.filter(
                F.col(coluna).isNotNull() & ~F.col(coluna).rlike(padrao)
            ).count()
            aprovado = invalidos == 0
            detalhe = f"{invalidos} valores fora do padrão {padrao}"

        elif tipo == "range":
            coluna = check["coluna"]
            minimo = check.get("minimo")
            maximo = check.get("maximo")
            condicao_invalida = F.col(coluna).isNotNull()
            if minimo is not None:
                condicao_invalida = condicao_invalida & (F.col(coluna) < F.lit(minimo))
            if maximo is not None:
                condicao_invalida = condicao_invalida | (
                    F.col(coluna).isNotNull() & (F.col(coluna) > F.lit(maximo))
                )
            fora = df.filter(condicao_invalida).count()
            aprovado = fora == 0
            detalhe = f"{fora} valores fora de [{minimo}, {maximo}]"

        elif tipo == "min_count":
            minimo = check["valor"]
            total = df.count()
            aprovado = total >= minimo
            detalhe = f"contagem={total}, mínimo={minimo}"

        else:
            aprovado = False
            detalhe = f"tipo de check desconhecido: {tipo}"
            critico = True

        resultado = {
            "check": nome,
            "tipo": tipo,
            "critico": critico,
            "aprovado": aprovado,
            "detalhe": detalhe,
        }
        resultados.append(resultado)

        nivel = logging.INFO if aprovado else logging.WARNING
        logger.log(nivel, "DQ %s — %s (%s)", "OK" if aprovado else "FALHA", nome, detalhe)

        if not aprovado and critico:
            falhas_criticas.append(f"{nome}: {detalhe}")

    if falhas_criticas:
        raise ValueError(
            "Verificações críticas de qualidade falharam:\n- "
            + "\n- ".join(falhas_criticas)
        )

    return resultados


def validar_regex_amostra(valor: str, padrao: str) -> bool:
    """Utilitário para testes locais fora do Spark."""
    return bool(re.fullmatch(padrao, valor))
