#!/bin/bash
# =============================================================================
# init-topics.sh — Inicializa los topics de Apache Kafka
# Se ejecuta como un contenedor auxiliar (kafka-init) en docker-compose
# =============================================================================

set -e

BOOTSTRAP="kafka:29092"
echo "============================================"
echo "  ELDEN RING — Inicializando Kafka Topics  "
echo "============================================"

# Esperar a que Kafka esté disponible
until kafka-topics --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
  echo "⏳ Esperando que Kafka esté listo..."
  sleep 3
done

echo "✅ Kafka listo. Creando topics..."

create_topic() {
  local name=$1
  local partitions=${2:-3}
  local retention_ms=${3:-86400000}  # 24h por defecto

  if kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$name" > /dev/null 2>&1; then
    echo "  ℹ️  Topic '$name' ya existe"
  else
    kafka-topics --bootstrap-server "$BOOTSTRAP" \
      --create \
      --topic "$name" \
      --partitions "$partitions" \
      --replication-factor 1 \
      --config "retention.ms=$retention_ms" \
      --config "compression.type=gzip"
    echo "  ✅ Topic '$name' creado ($partitions particiones, retención ${retention_ms}ms)"
  fi
}

# Topics del proyecto
create_topic "player-events"    3   86400000   # 24h de retención, 3 particiones
create_topic "meta-shift-alerts" 1  604800000  # 7 días de retención, 1 partición
create_topic "weapon-stats"     1   86400000   # 24h, 1 partición

echo ""
echo "📋 Topics activos:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --list | while read topic; do
  echo "  - $topic"
done

echo ""
echo "✅ Inicialización de Kafka completada"
