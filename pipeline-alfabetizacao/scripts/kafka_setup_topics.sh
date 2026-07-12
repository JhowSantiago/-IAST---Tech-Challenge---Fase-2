#!/usr/bin/env bash
# Cria tópicos Kafka da pipeline ICA (domínio.entidade, 3 partições).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

create_topic() {
  local topic="$1"
  if kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$topic"; then
    echo "Tópico ${topic} já existe."
    return
  fi
  kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
  echo "Tópico ${topic} criado."
}

create_topic "educacao.indicador_alfabetizacao"
create_topic "educacao.meta_atualizada"

kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic educacao.indicador_alfabetizacao
kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic educacao.meta_atualizada

echo "Tópicos Kafka configurados."
