"""Constantes e geração de eventos educacionais simulados."""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd

TOPICO_INDICADOR = "educacao.indicador_alfabetizacao"
TOPICO_META = "educacao.meta_atualizada"
ENTIDADE_STREAMING = "indicador_alfabetizacao"
CONSUMER_GROUP = "bronze-ingestao"
JOB_NAME_STREAMING = "etl-bronze-streaming"

TIPOS_EVENTO = (
    "indicador_atualizado",
    "meta_revisada",
    "medicao_nova",
)
PESOS_EVENTO = (0.7, 0.2, 0.1)


def escolher_tipo_evento() -> str:
    return random.choices(TIPOS_EVENTO, weights=PESOS_EVENTO, k=1)[0]


def meta_vigente(linha: pd.Series) -> float:
    ano = int(linha["ano"])
    coluna = f"meta_alfabetizacao_{ano}"
    if coluna in linha.index and pd.notna(linha[coluna]):
        return float(linha[coluna])
    for fallback in ("meta_alfabetizacao_2024", "meta_alfabetizacao_2030"):
        if fallback in linha.index and pd.notna(linha[fallback]):
            return float(linha[fallback])
    return float(linha.get("taxa_alfabetizacao", 0.0))


def gerar_evento(linha: pd.Series) -> dict[str, Any]:
    tipo = escolher_tipo_evento()
    meta = meta_vigente(linha)
    taxa_raw = linha.get("taxa_alfabetizacao")
    taxa_base = float(taxa_raw) if pd.notna(taxa_raw) else meta * 0.85

    if tipo == "indicador_atualizado":
        taxa = round(max(0.0, min(100.0, taxa_base * random.uniform(0.98, 1.02))), 2)
    elif tipo == "meta_revisada":
        taxa = taxa_base
        meta = round(max(0.0, min(100.0, meta * random.uniform(0.99, 1.01))), 2)
    else:
        taxa = round(max(0.0, min(100.0, taxa_base * random.uniform(0.97, 1.03))), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": tipo,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "id_municipio": str(linha["id_municipio"]).zfill(7),
            "sigla_uf": str(linha.get("sigla_uf", ""))[:2].upper() or None,
            "ano": int(linha["ano"]),
            "taxa_alfabetizacao": taxa,
            "meta": meta,
        },
    }


def evento_para_json(evento: dict[str, Any]) -> bytes:
    return json.dumps(evento, ensure_ascii=False).encode("utf-8")


def eventos_para_dataframe(eventos: list[dict[str, Any]]) -> pd.DataFrame:
    linhas = []
    for evento in eventos:
        payload = evento["payload"]
        linhas.append(
            {
                "event_id": evento["event_id"],
                "event_type": evento["event_type"],
                "timestamp": evento["timestamp"],
                "id_municipio": payload["id_municipio"],
                "sigla_uf": payload.get("sigla_uf"),
                "ano": payload["ano"],
                "taxa_alfabetizacao": payload["taxa_alfabetizacao"],
                "meta": payload["meta"],
            }
        )
    return pd.DataFrame(linhas)
