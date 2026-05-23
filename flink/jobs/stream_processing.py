"""
Flink Stream Job: Procesamiento en tiempo real de eventos de jugadores
- Ventanas temporales tumbling de 5 minutos
- Cálculo de tasas de victoria (win rate) por arma/jefe
- Detección de Meta-Shift (>15% variación entre ventanas)
- Escritura a PostgreSQL y alerta a Kafka

NOTA: Usa la nueva API KafkaSource (FlinkKafkaConsumer está deprecado desde 1.15)
Requiere: flink-sql-connector-kafka JAR en /opt/flink/usrlib/
"""

import json
import logging
import os
import pickle
import time
from datetime import datetime

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("EldenRingStream")

# ── Configuración ─────────────────────────────────────────────────────────────
KAFKA_BROKER     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
POSTGRES_HOST    = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB      = os.getenv("POSTGRES_DB", "elden_ring")
POSTGRES_USER    = os.getenv("POSTGRES_USER", "elden")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ring123")
WINDOW_MINUTES   = 5
META_SHIFT_THRESHOLD = 15.0   # % de cambio en win rate para disparar alerta


# ── Helpers PostgreSQL ─────────────────────────────────────────────────────────
def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def upsert_ranking(data: dict):
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO real_time_rankings
                    (weapon_id, boss_id, total_attempts, total_victories,
                     total_deaths, win_rate, avg_time_to_kill,
                     window_start, window_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s,
                        NOW() - INTERVAL '5 minutes', NOW())
                ON CONFLICT (weapon_id, boss_id, window_start)
                DO UPDATE SET
                    total_attempts   = EXCLUDED.total_attempts,
                    total_victories  = EXCLUDED.total_victories,
                    total_deaths     = EXCLUDED.total_deaths,
                    win_rate         = EXCLUDED.win_rate,
                    avg_time_to_kill = EXCLUDED.avg_time_to_kill,
                    updated_at       = NOW()
            """, (
                data["weapon_id"], data["boss_id"],
                data["total_attempts"], data["victories"],
                data["deaths"], data["win_rate"],
                data["avg_time_to_kill"]
            ))
        conn.commit()


def insert_meta_shift(alert: dict):
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO meta_shifts
                    (weapon_id, boss_id, shift_type,
                     previous_popularity, current_popularity, change_percentage)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                alert["weapon_id"], alert["boss_id"], alert["shift_type"],
                alert["previous_win_rate"], alert["current_win_rate"],
                alert["change_percentage"]
            ))
        conn.commit()


def publish_alert_to_kafka(alert: dict):
    """Publica alerta de meta-shift al topic Kafka usando kafka-python."""
    from kafka import KafkaProducer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
            max_block_ms=5000
        )
        producer.send("meta-shift-alerts", value=alert)
        producer.flush(timeout=5)
        producer.close()
        log.info(f"[KAFKA] Alerta publicada: {alert['shift_type']} {alert['weapon_id']} vs {alert['boss_id']}")
    except Exception as e:
        log.warning(f"[KAFKA] No se pudo publicar alerta: {e}")


# ── Motor de Ventanas (Python puro con kafka-python) ───────────────────────────
# PyFlink tiene limitaciones importantes cuando se usa como script Python directo
# sin un Job Server. Esta implementación usa kafka-python para hacer el polling
# y agrega eventos manualmente en ventanas de tiempo, siendo equivalente a lo que
# haría el pipeline de Flink pero ejecutable de forma fiable en Docker.
#
# Para producción real con un cluster Flink completo, ver los comentarios al final.

class TumblingWindowAggregator:
    """Agrega eventos en ventanas de WINDOW_MINUTES minutos."""

    def __init__(self, window_minutes: int = 5):
        self.window_seconds = window_minutes * 60
        self.windows: dict[str, list] = {}   # key=(weapon_id,boss_id), val=lista de eventos
        self.last_flush = time.time()
        self.prev_stats: dict[str, dict] = {}  # Estado para detección de meta-shift

    def add_event(self, event: dict):
        wid = event.get("weapon_id", "")
        bid = event.get("boss_id", "")
        if not wid or not bid:
            return
        key = f"{wid}|{bid}"
        self.windows.setdefault(key, []).append(event)

    def should_flush(self) -> bool:
        return (time.time() - self.last_flush) >= self.window_seconds

    def flush(self) -> list[dict]:
        """Calcula agregados por ventana y devuelve lista de resultados."""
        results = []
        for key, events in self.windows.items():
            weapon_id, boss_id = key.split("|", 1)
            total = len(events)
            victories = sum(1 for e in events if e.get("success") is True)
            deaths = total - victories
            win_rate = round((victories / total * 100), 2) if total > 0 else 0.0
            ttk_vals = [e["time_to_kill"] for e in events
                        if e.get("time_to_kill") is not None and e["time_to_kill"] > 0]
            avg_ttk = round(sum(ttk_vals) / len(ttk_vals), 2) if ttk_vals else 0.0

            stats = {
                "weapon_id": weapon_id,
                "boss_id": boss_id,
                "total_attempts": total,
                "victories": victories,
                "deaths": deaths,
                "win_rate": win_rate,
                "avg_time_to_kill": avg_ttk,
            }

            # Detección de Meta-Shift
            if key in self.prev_stats:
                prev_wr = self.prev_stats[key].get("win_rate", 0.0)
                delta = abs(win_rate - prev_wr)
                if delta >= META_SHIFT_THRESHOLD:
                    alert = {
                        "weapon_id": weapon_id,
                        "boss_id": boss_id,
                        "shift_type": "emerging" if win_rate > prev_wr else "declining",
                        "previous_win_rate": prev_wr,
                        "current_win_rate": win_rate,
                        "change_percentage": round(delta, 2),
                        "detected_at": datetime.now().isoformat()
                    }
                    stats["_alert"] = alert

            self.prev_stats[key] = stats
            results.append(stats)

        self.windows = {}
        self.last_flush = time.time()
        return results


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  ELDEN RING - FLINK STREAM PROCESSING (via kafka-python)")
    log.info("=" * 60)
    log.info(f"  Kafka: {KAFKA_BROKER} | Postgres: {POSTGRES_HOST}")
    log.info(f"  Ventana: {WINDOW_MINUTES} min | Meta-Shift threshold: {META_SHIFT_THRESHOLD}%")
    log.info("=" * 60)

    # Importar aquí para facilitar el manejo de errores de inicio
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    consumer = None
    for attempt in range(40):
        try:
            consumer = KafkaConsumer(
                "player-events",
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="flink-meta-build-processor",
                consumer_timeout_ms=2000,    # No bloquear indefinidamente
                enable_auto_commit=True,
            )
            log.info(f"✅ Conectado a Kafka en intento {attempt + 1}")
            break
        except NoBrokersAvailable:
            log.info(f"⏳ Esperando Kafka... ({attempt + 1}/40)")
            time.sleep(3)

    if consumer is None:
        raise RuntimeError("No se pudo conectar a Kafka después de 40 intentos")

    aggregator = TumblingWindowAggregator(WINDOW_MINUTES)
    events_total = 0

    log.info("🔄 Comenzando consumo de eventos del topic 'player-events'...")

    try:
        while True:
            # Poll con timeout para poder revisar el flush periódico
            raw_records = consumer.poll(timeout_ms=1000, max_records=500)

            for tp, records in raw_records.items():
                for record in records:
                    event = record.value
                    if event:
                        aggregator.add_event(event)
                        events_total += 1

            # Flush de la ventana si corresponde
            if aggregator.should_flush():
                results = aggregator.flush()
                log.info(f"📊 Flush de ventana: {len(results)} pares weapon/boss procesados | Total eventos: {events_total:,}")

                for stats in results:
                    alert = stats.pop("_alert", None)
                    try:
                        upsert_ranking(stats)
                    except Exception as e:
                        log.error(f"[DB] Error al escribir ranking: {e}")

                    if alert:
                        log.warning(
                            f"🚨 META-SHIFT: {alert['shift_type'].upper()} "
                            f"{alert['weapon_id']} vs {alert['boss_id']} "
                            f"({alert['previous_win_rate']}% → {alert['current_win_rate']}%)"
                        )
                        try:
                            insert_meta_shift(alert)
                            publish_alert_to_kafka(alert)
                        except Exception as e:
                            log.error(f"[ALERT] Error procesando alerta: {e}")

    except KeyboardInterrupt:
        log.info("⛔ Stream processing detenido manualmente.")
    finally:
        if consumer:
            consumer.close()
        log.info("Conexiones cerradas correctamente.")


if __name__ == "__main__":
    main()
