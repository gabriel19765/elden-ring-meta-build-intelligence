"""
Elden Ring Meta-Build REST API — FastAPI 0.115.x
Endpoints: weapons, bosses, effectiveness, rankings, meta-shifts, events
"""

import datetime
import logging
import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("EldenRingAPI")

app = FastAPI(
    title="Elden Ring Meta-Build Intelligence API",
    description="REST backend for static weapon/boss data, effectiveness scores, real-time rankings and meta-shift events.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Configuración ──────────────────────────────────────────────────────────────
DB_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    database=os.getenv("POSTGRES_DB", "elden_ring"),
    user=os.getenv("POSTGRES_USER", "elden"),
    password=os.getenv("POSTGRES_PASSWORD", "ring123"),
    cursor_factory=RealDictCursor,
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

_kafka_producer: Optional[KafkaProducer] = None


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def get_kafka() -> Optional[KafkaProducer]:
    global _kafka_producer
    if _kafka_producer is not None:
        return _kafka_producer
    try:
        _kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
            request_timeout_ms=5000,
            max_block_ms=5000,
        )
        log.info("Kafka producer listo")
    except NoBrokersAvailable:
        log.warning("Kafka no disponible — modo solo DB")
    return _kafka_producer


# ── Models ─────────────────────────────────────────────────────────────────────
class PlayerEventIn(BaseModel):
    player_id:    str
    event_type:   str   # 'death' | 'victory' | 'equip_weapon' | 'enter_boss'
    weapon_id:    str
    boss_id:      str
    location:     Optional[str] = "Unknown"
    success:      Optional[bool] = None
    time_to_kill: Optional[float] = None


# ── Health ─────────────────────────────────────────────────────────────────────
@app.get("/", tags=["health"])
def root():
    return {"status": "online", "service": "Elden Ring Meta-Build API", "version": "2.0.0"}


@app.get("/health", tags=["health"])
def health():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"db": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {e}")


# ── Weapons ────────────────────────────────────────────────────────────────────
@app.get("/weapons", tags=["static"])
def get_weapons(category: Optional[str] = None):
    with get_db() as conn:
        with conn.cursor() as cur:
            if category:
                cur.execute("SELECT * FROM weapons WHERE category = %s ORDER BY name", (category,))
            else:
                cur.execute("SELECT * FROM weapons ORDER BY name")
            return list(cur.fetchall())


# ── Bosses ─────────────────────────────────────────────────────────────────────
@app.get("/bosses", tags=["static"])
def get_bosses(location: Optional[str] = None):
    with get_db() as conn:
        with conn.cursor() as cur:
            if location:
                cur.execute("SELECT * FROM bosses WHERE location ILIKE %s ORDER BY name", (f"%{location}%",))
            else:
                cur.execute("SELECT * FROM bosses ORDER BY name")
            return list(cur.fetchall())


# ── Effectiveness ──────────────────────────────────────────────────────────────
@app.get("/effectiveness/{boss_id}", tags=["analytics"])
def get_effectiveness(boss_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT w.name AS weapon_name, w.category,
                       ROUND(e.effectiveness_score::numeric, 2) AS effectiveness_score
                FROM weapon_boss_effectiveness e
                JOIN weapons w ON e.weapon_id = w.weapon_id
                WHERE e.boss_id = %s
                ORDER BY e.effectiveness_score DESC
            """, (boss_id,))
            rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for boss '{boss_id}'")
    return list(rows)


# ── Rankings ───────────────────────────────────────────────────────────────────
@app.get("/rankings", tags=["realtime"])
def get_rankings(boss_id: Optional[str] = None, limit: int = Query(50, le=200)):
    with get_db() as conn:
        with conn.cursor() as cur:
            if boss_id:
                cur.execute("""
                    SELECT w.name AS weapon_name, b.name AS boss_name, r.*
                    FROM real_time_rankings r
                    JOIN weapons w ON r.weapon_id = w.weapon_id
                    JOIN bosses  b ON r.boss_id   = b.boss_id
                    WHERE r.boss_id = %s
                    ORDER BY r.win_rate DESC
                    LIMIT %s
                """, (boss_id, limit))
            else:
                cur.execute("""
                    SELECT w.name AS weapon_name, b.name AS boss_name, r.*
                    FROM real_time_rankings r
                    JOIN weapons w ON r.weapon_id = w.weapon_id
                    JOIN bosses  b ON r.boss_id   = b.boss_id
                    ORDER BY r.win_rate DESC
                    LIMIT %s
                """, (limit,))
            return list(cur.fetchall())


# ── Meta-Shifts ────────────────────────────────────────────────────────────────
@app.get("/meta-shifts", tags=["realtime"])
def get_meta_shifts(limit: int = Query(20, le=100)):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.*, w.name AS weapon_name,
                       COALESCE(b.name, m.boss_id) AS boss_name
                FROM meta_shifts m
                JOIN weapons w ON m.weapon_id = w.weapon_id
                LEFT JOIN bosses b ON m.boss_id = b.boss_id
                ORDER BY m.detected_at DESC
                LIMIT %s
            """, (limit,))
            return list(cur.fetchall())


# ── Events ─────────────────────────────────────────────────────────────────────
@app.post("/events", status_code=201, tags=["ingest"])
def create_event(event: PlayerEventIn):
    payload = {
        "event_id":   f"api_{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{os.urandom(3).hex()}",
        "player_id":  event.player_id,
        "event_type": event.event_type,
        "weapon_id":  event.weapon_id,
        "boss_id":    event.boss_id,
        "location":   event.location,
        "timestamp":  datetime.datetime.utcnow().isoformat(),
        "success":    event.success,
        "time_to_kill": event.time_to_kill,
    }

    # Kafka (fire-and-forget)
    kafka_ok = False
    prod = get_kafka()
    if prod:
        try:
            prod.send("player-events", key=payload["player_id"], value=payload)
            prod.flush(timeout=3)
            kafka_ok = True
        except Exception as e:
            log.warning(f"Kafka publish error: {e}")

    # PostgreSQL (siempre)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO player_events
                    (event_id, player_id, event_type, weapon_id, boss_id, location, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, (
                payload["event_id"], payload["player_id"], payload["event_type"],
                payload["weapon_id"], payload["boss_id"], payload["location"],
                payload["timestamp"],
            ))
        conn.commit()

    return {
        "message":         "Evento registrado",
        "event_id":        payload["event_id"],
        "kafka_published": kafka_ok,
        "db_persisted":    True,
    }
