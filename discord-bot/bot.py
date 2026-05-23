"""
Discord Alert Bot for Elden Ring Meta-Shift
Consumes Flink-generated alerts from Kafka topic 'meta-shift-alerts'
and notifies via Discord Webhook with rich embeds.
"""

import json
import logging
import os
import time

import psycopg2
import requests
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("DiscordAlertBot")

# ── Configuración ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DISCORD_WEBHOOK_URL  = os.getenv("DISCORD_WEBHOOK_URL", "")
DB_HOST              = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT              = os.getenv("POSTGRES_PORT", "5432")
DB_NAME              = os.getenv("POSTGRES_DB", "elden_ring")
DB_USER              = os.getenv("POSTGRES_USER", "elden")
DB_PASS              = os.getenv("POSTGRES_PASSWORD", "ring123")


class DiscordAlertBot:
    def __init__(self):
        self.consumer = None
        self._connect_kafka()

    def _connect_kafka(self):
        for i in range(40):
            try:
                self.consumer = KafkaConsumer(
                    "meta-shift-alerts",
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    # BUG FIX: era value_serializer (no existe en Consumer), debe ser value_deserializer
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
                    auto_offset_reset="latest",
                    group_id="discord-bot-alerts",
                    enable_auto_commit=True,
                    consumer_timeout_ms=5000,
                )
                log.info(f"✅ Discord Bot conectado a Kafka: {KAFKA_BOOTSTRAP}")
                return
            except NoBrokersAvailable:
                log.info(f"⏳ Esperando Kafka... ({i + 1}/40)")
                time.sleep(3)
        raise RuntimeError("Discord Bot: No se pudo conectar a Kafka")

    def _get_names(self, weapon_id: str, boss_id: str) -> tuple[str, str]:
        weapon_name = weapon_id
        boss_name   = boss_id
        try:
            with psycopg2.connect(
                host=DB_HOST, port=DB_PORT, database=DB_NAME,
                user=DB_USER, password=DB_PASS, cursor_factory=RealDictCursor
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT name FROM weapons WHERE weapon_id = %s", (weapon_id,))
                    row = cur.fetchone()
                    if row:
                        weapon_name = row["name"]

                    cur.execute("SELECT name FROM bosses WHERE boss_id = %s", (boss_id,))
                    row = cur.fetchone()
                    if row:
                        boss_name = row["name"]
        except Exception as e:
            log.warning(f"Error enriqueciendo nombres desde DB: {e}")
        return weapon_name, boss_name

    def _send_discord_alert(self, alert: dict):
        weapon_id  = alert.get("weapon_id", "?")
        boss_id    = alert.get("boss_id", "?")
        shift_type = alert.get("shift_type", "unknown")
        prev_wr    = alert.get("previous_win_rate", 0.0)
        curr_wr    = alert.get("current_win_rate", 0.0)
        change_pct = alert.get("change_percentage", 0.0)

        weapon_name, boss_name = self._get_names(weapon_id, boss_id)

        # Discord embed colors (decimal)
        color_map = {"emerging": 3066993, "declining": 15158332, "dominant": 16753920}
        color     = color_map.get(shift_type, 3447003)
        emoji     = {"emerging": "📈", "declining": "📉", "dominant": "👑"}.get(shift_type, "⚡")

        embed = {
            "title":       f"🚨 META-SHIFT: {shift_type.upper()} {emoji}",
            "description": (
                f"El rendimiento de **{weapon_name}** contra **{boss_name}** ha cambiado drásticamente.\n\n"
                f"**Win Rate Anterior:** `{prev_wr}%`\n"
                f"**Win Rate Actual:** `{curr_wr}%`\n"
                f"**Variación:** `{change_pct:+.1f}%`\n\n"
                f"⚔️ _Visita la app Streamlit para actualizar tu estrategia._"
            ),
            "color":  color,
            "footer": {"text": "Elden Ring Meta-Build Intelligence"},
            "timestamp": alert.get("detected_at"),
        }

        payload = {
            "username":   "Elden Ring Meta Bot",
            "avatar_url": "https://upload.wikimedia.org/wikipedia/en/b/b9/Elden_Ring_Box_art.jpg",
            "embeds":     [embed],
        }

        log.warning(
            f"🔔 META-SHIFT [{shift_type}] {weapon_name} vs {boss_name}: "
            f"{prev_wr}% → {curr_wr}% (Δ{change_pct:+.1f}%)"
        )

        if DISCORD_WEBHOOK_URL and "mock" not in DISCORD_WEBHOOK_URL:
            try:
                resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
                if resp.status_code == 204:
                    log.info("✅ Alerta enviada a Discord correctamente.")
                else:
                    log.error(f"❌ Discord webhook error: HTTP {resp.status_code} - {resp.text}")
            except Exception as e:
                log.error(f"❌ Error HTTP Discord: {e}")
        else:
            log.info("ℹ️  DISCORD_WEBHOOK_URL no configurado. Alerta solo en logs.")

    def run(self):
        log.info("=" * 50)
        log.info("  ELDEN RING - DISCORD BOT ACTIVO")
        log.info("=" * 50)

        try:
            while True:
                records = self.consumer.poll(timeout_ms=3000, max_records=10)
                for tp, messages in records.items():
                    for msg in messages:
                        alert = msg.value
                        if alert and isinstance(alert, dict):
                            self._send_discord_alert(alert)
        except KeyboardInterrupt:
            log.info("⛔ Discord Bot detenido.")
        finally:
            if self.consumer:
                self.consumer.close()


if __name__ == "__main__":
    bot = DiscordAlertBot()
    bot.run()
