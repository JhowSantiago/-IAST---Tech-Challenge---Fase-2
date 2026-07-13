# Streaming — Kafka e contrato de eventos

## 1. Visão geral

A ingestão streaming simula atualizações educacionais em tempo quase real. O fluxo é:

```
Produtor (simulador) → Kafka → Consumidor → Bronze S3 (bronze/streaming/)
```

A camada batch (`bronze/batch/`) e a streaming coexistem na Bronze; a Silver fará a integração e deduplicação por `event_id`.

## 2. Infraestrutura local

### Docker Compose (recomendado)

```bash
docker compose -f docker/docker-compose.kafka.yml up -d
python scripts/kafka_setup_topics.py
```

### Variáveis de ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers Kafka |

## 3. Tópicos

Padrão de nomenclatura: `dominio.entidade` (Aula 2.1).

| Tópico | Partições | Uso |
|--------|-----------|-----|
| `educacao.indicador_alfabetizacao` | 3 | Atualizações de indicador, metas e medições |
| `educacao.meta_atualizada` | 3 | Reservado para eventos exclusivos de meta |

O produtor principal publica em `educacao.indicador_alfabetizacao`. Três partições permitem demonstrar consumer groups paralelos mantendo ordenação por município via **partition key** (`id_municipio`).

## 4. Contrato de evento

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "indicador_atualizado",
  "timestamp": "2026-07-12T20:00:00+00:00",
  "payload": {
    "id_municipio": "3550308",
    "sigla_uf": "SP",
    "ano": 2024,
    "taxa_alfabetizacao": 62.5,
    "meta": 70.0
  }
}
```

### Tipos de evento

| `event_type` | Descrição | Campos principais |
|--------------|-----------|-------------------|
| `indicador_atualizado` | Variação na taxa observada | `taxa_alfabetizacao` |
| `meta_revisada` | Revisão de meta municipal | `meta` |
| `medicao_nova` | Nova medição de proficiência | `taxa_alfabetizacao` |

### Contrato adotado

- `event_id`: UUID v4, único por evento (deduplicação na Silver)
- `timestamp`: ISO 8601 em UTC
- `payload.id_municipio`: STRING com 7 dígitos (IBGE)
- Partition key Kafka: `id_municipio` (mantém a ordenação por município)

## 5. Scripts operacionais

```bash
# Publicar eventos simulados (base: meta_municipio staging)
python -m src.streaming.producer_simulador --limite 100

# Consumir e gravar buffer local
python -m src.streaming.consumer_bronze --limite 100 --timeout 30

# Publicar buffer na Bronze S3
python scripts/carregar_bronze_streaming.py

# Pipeline completo (produtor → consumidor → bronze)
python scripts/executar_pipeline_streaming.py --limite 50
```

## 6. Path Bronze streaming

```
s3://{BUCKET_BRONZE}/bronze/streaming/indicador_alfabetizacao/ano={ano}/mes={mes}/dia={dia}/
```

Metadados adicionais em cada registro:

- `_source_type`: `streaming`
- `_source_entity`: `indicador_alfabetizacao`
- `_job_name`: `etl-bronze-streaming`

## 7. Consumer group

| Atributo | Valor |
|----------|-------|
| Group ID | `bronze-ingestao` |
| Flush | A cada 50 eventos ou timeout de 30s |
| Destino intermediário | `data/staging/streaming/events_buffer.parquet` |
