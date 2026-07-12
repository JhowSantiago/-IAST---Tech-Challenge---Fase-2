# Testes de verificação da Fase 05 — ingestão streaming.

from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import boto3
import pandas as pd
import pytest
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bronze.load_streaming_pandas import construir_bronze_streaming_pandas  # noqa: E402
from src.common.config import get_settings  # noqa: E402
from src.dq.checks_pandas import checar_entidade_pandas  # noqa: E402
from src.streaming.eventos import (  # noqa: E402
    ENTIDADE_STREAMING,
    TOPICO_INDICADOR,
    evento_para_json,
    eventos_para_dataframe,
    gerar_evento,
)

ARTEFATOS_OBRIGATORIOS = [
    "docker/docker-compose.kafka.yml",
    "docs/streaming.md",
    "scripts/kafka_setup_topics.sh",
    "scripts/kafka_setup_topics.py",
    "scripts/carregar_bronze_streaming.py",
    "scripts/executar_pipeline_streaming.py",
    "src/streaming/producer_simulador.py",
    "src/streaming/consumer_bronze.py",
    "src/streaming/eventos.py",
    "src/bronze/etl-bronze-streaming.py",
    "src/bronze/load_streaming.py",
    "src/bronze/load_streaming_pandas.py",
]


def kafka_disponivel() -> bool:
    try:
        from kafka import KafkaAdminClient

        load_dotenv(ROOT / ".env")
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            client_id="ica-test-probe",
            request_timeout_ms=3000,
        )
        admin.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def requer_kafka():
    if not kafka_disponivel():
        pytest.skip("Kafka indisponível em localhost:9092")


def test_artefatos_existem() -> None:
    for rel in ARTEFATOS_OBRIGATORIOS:
        assert (ROOT / rel).exists(), rel


def test_contrato_evento() -> None:
    df = pd.read_parquet(ROOT / "data" / "staging" / "meta_municipio.parquet")
    evento = gerar_evento(df.iloc[0])
    assert uuid.UUID(evento["event_id"])
    assert evento["event_type"] in {
        "indicador_atualizado",
        "meta_revisada",
        "medicao_nova",
    }
    payload = evento["payload"]
    assert len(payload["id_municipio"]) == 7
    assert payload["id_municipio"].isdigit()
    assert 2023 <= payload["ano"] <= 2024
    json.loads(evento_para_json(evento).decode("utf-8"))


def test_dq_bronze_streaming_local() -> None:
    eventos = []
    df = pd.read_parquet(ROOT / "data" / "staging" / "meta_municipio.parquet").head(10)
    for _, linha in df.iterrows():
        eventos.append(gerar_evento(linha))
    df_eventos = eventos_para_dataframe(eventos)
    df_bronze = construir_bronze_streaming_pandas(df_eventos)
    assert (df_bronze["_source_type"] == "streaming").all()
    checar_entidade_pandas(df_bronze, ENTIDADE_STREAMING, "bronze")


def test_pipeline_kafka_e2e(requer_kafka) -> None:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    load_dotenv(ROOT / ".env")
    settings = get_settings()
    bootstrap = settings.kafka_bootstrap_servers
    group_id = f"test-streaming-{uuid.uuid4().hex[:8]}"

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="ica-test-admin")
    try:
        admin.create_topics(
            [NewTopic(TOPICO_INDICADOR, num_partitions=3, replication_factor=1)],
            validate_only=False,
        )
    except TopicAlreadyExistsError:
        pass
    admin.close()

    df = pd.read_parquet(ROOT / "data" / "staging" / "meta_municipio.parquet").head(20)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v,
        key_serializer=lambda k: k.encode("utf-8"),
    )
    for _, linha in df.iterrows():
        evento = gerar_evento(linha)
        producer.send(
            TOPICO_INDICADOR,
            key=evento["payload"]["id_municipio"],
            value=evento_para_json(evento),
        ).get(timeout=10)
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        TOPICO_INDICADOR,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    eventos: list[dict] = []
    try:
        while len(eventos) < 10:
            lote = consumer.poll(timeout_ms=2000, max_records=10 - len(eventos))
            for _tp, registros in lote.items():
                for registro in registros:
                    eventos.append(registro.value)
                    if len(eventos) >= 10:
                        break
    finally:
        consumer.close()
    assert len(eventos) >= 10

    buffer_dir = ROOT / "data" / "staging" / "streaming"
    buffer_dir.mkdir(parents=True, exist_ok=True)
    buffer = buffer_dir / "events_buffer_test.parquet"
    eventos_para_dataframe(eventos[:10]).to_parquet(buffer, index=False)

    df_bronze = construir_bronze_streaming_pandas(pd.read_parquet(buffer))
    checar_entidade_pandas(df_bronze, ENTIDADE_STREAMING, "bronze")

    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    from src.bronze.load_streaming_pandas import salvar_bronze_streaming_s3

    salvar_bronze_streaming_s3(df_bronze, settings.bucket_bronze, s3)

    prefix = f"bronze/streaming/{ENTIDADE_STREAMING}/"
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.bucket_bronze, Prefix=prefix):
        keys.extend(o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet"))
    assert keys

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        path = tmp.name
    s3.download_file(settings.bucket_bronze, keys[-1], path)
    amostra = pd.read_parquet(path)
    os.remove(path)
    assert (amostra["_source_type"] == "streaming").any()
    assert amostra["event_id"].notna().all()


def test_batch_e_streaming_coexistem_s3() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    batch = s3.list_objects_v2(
        Bucket=settings.bucket_bronze, Prefix="bronze/batch/uf/", MaxKeys=1
    )
    streaming = s3.list_objects_v2(
        Bucket=settings.bucket_bronze,
        Prefix=f"bronze/streaming/{ENTIDADE_STREAMING}/",
        MaxKeys=1,
    )
    assert batch.get("KeyCount", 0) > 0
    assert streaming.get("KeyCount", 0) > 0
