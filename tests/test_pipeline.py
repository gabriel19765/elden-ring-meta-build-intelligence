"""
Tests de Integración — Elden Ring Meta-Build Intelligence Pipeline
Valida el comportamiento end-to-end de todos los servicios.

Uso:
    # Con Docker running:
    pytest tests/ -v

    # Solo tests unitarios (sin Docker):
    pytest tests/test_unit.py -v
"""

import json
import time
from typing import Generator

import pytest
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# ─── Configuración ─────────────────────────────────────────────────────────────
API_BASE   = "http://localhost:8000"
DB_CONFIG  = dict(host="localhost", port=5432, database="elden_ring", user="elden", password="ring123")
TIMEOUT    = 10   # segundos por request
TEST_PLAYER = "pytest_integration_player"


# ─── Fixtures ──────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def api_available():
    """Verifica que la API esté accesible antes de correr los tests."""
    try:
        r = requests.get(f"{API_BASE}/health", timeout=TIMEOUT)
        if r.status_code != 200:
            pytest.skip(f"API no disponible (HTTP {r.status_code}). Ejecuta 'make up' primero.")
    except requests.ConnectionError:
        pytest.skip("API no disponible. Ejecuta 'make up' primero.")


@pytest.fixture(scope="session")
def db_conn():
    """Conexión a PostgreSQL directa para verificar estado de la BD."""
    try:
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        yield conn
        conn.close()
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL no accesible. Ejecuta 'make up' primero.")


@pytest.fixture(autouse=True, scope="session")
def cleanup_test_events(db_conn):
    """Limpia los eventos de test al final de la sesión."""
    yield
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM player_events WHERE player_id = %s", (TEST_PLAYER,))
    db_conn.commit()


# ─── Tests: API Health ─────────────────────────────────────────────────────────
class TestAPIHealth:
    def test_root_returns_online(self, api_available):
        r = requests.get(f"{API_BASE}/", timeout=TIMEOUT)
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "online"
        assert "version" in data

    def test_health_endpoint_db_ok(self, api_available):
        r = requests.get(f"{API_BASE}/health", timeout=TIMEOUT)
        assert r.status_code == 200
        data = r.json()
        assert data["db"] == "ok"

    def test_docs_available(self, api_available):
        r = requests.get(f"{API_BASE}/docs", timeout=TIMEOUT)
        assert r.status_code == 200


# ─── Tests: Weapons ────────────────────────────────────────────────────────────
class TestWeapons:
    def test_get_all_weapons(self, api_available):
        r = requests.get(f"{API_BASE}/weapons", timeout=TIMEOUT)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) >= 10, "Debe haber al menos 10 armas (seed data)"

    def test_weapon_has_required_fields(self, api_available):
        r = requests.get(f"{API_BASE}/weapons", timeout=TIMEOUT)
        weapons = r.json()
        assert len(weapons) > 0
        first = weapons[0]
        for field in ["weapon_id", "name", "category"]:
            assert field in first, f"Falta campo '{field}' en weapon"

    def test_filter_by_category(self, api_available):
        r = requests.get(f"{API_BASE}/weapons?category=Katana", timeout=TIMEOUT)
        assert r.status_code == 200
        data = r.json()
        for w in data:
            assert w["category"] == "Katana"

    def test_weapons_sorted_by_name(self, api_available):
        r = requests.get(f"{API_BASE}/weapons", timeout=TIMEOUT)
        names = [w["name"] for w in r.json()]
        assert names == sorted(names), "Las armas deben venir ordenadas por nombre"


# ─── Tests: Bosses ─────────────────────────────────────────────────────────────
class TestBosses:
    def test_get_all_bosses(self, api_available):
        r = requests.get(f"{API_BASE}/bosses", timeout=TIMEOUT)
        assert r.status_code == 200
        data = r.json()
        assert len(data) >= 8, "Debe haber al menos 8 jefes (seed data)"

    def test_boss_has_required_fields(self, api_available):
        r = requests.get(f"{API_BASE}/bosses", timeout=TIMEOUT)
        first = r.json()[0]
        for field in ["boss_id", "name", "location", "health_points"]:
            assert field in first, f"Falta campo '{field}' en boss"

    def test_boss_health_points_positive(self, api_available):
        r = requests.get(f"{API_BASE}/bosses", timeout=TIMEOUT)
        for boss in r.json():
            assert boss["health_points"] > 0, f"Boss {boss['name']} tiene HP <= 0"

    def test_filter_by_location(self, api_available):
        r = requests.get(f"{API_BASE}/bosses?location=Stormveil", timeout=TIMEOUT)
        assert r.status_code == 200


# ─── Tests: Effectiveness ──────────────────────────────────────────────────────
class TestEffectiveness:
    def test_effectiveness_for_known_boss(self, api_available):
        r = requests.get(f"{API_BASE}/effectiveness/b001", timeout=TIMEOUT)
        assert r.status_code == 200
        data = r.json()
        assert len(data) > 0

    def test_effectiveness_score_in_range(self, api_available):
        r = requests.get(f"{API_BASE}/effectiveness/b001", timeout=TIMEOUT)
        for item in r.json():
            score = item["effectiveness_score"]
            assert 0 <= score <= 100, f"Score {score} fuera de rango [0,100]"

    def test_effectiveness_sorted_desc(self, api_available):
        r = requests.get(f"{API_BASE}/effectiveness/b001", timeout=TIMEOUT)
        scores = [item["effectiveness_score"] for item in r.json()]
        assert scores == sorted(scores, reverse=True), "Efectividad debe venir ordenada DESC"

    def test_effectiveness_unknown_boss_404(self, api_available):
        r = requests.get(f"{API_BASE}/effectiveness/boss_nonexistent_xyz", timeout=TIMEOUT)
        assert r.status_code == 404


# ─── Tests: Events (Ingesta) ───────────────────────────────────────────────────
class TestEvents:
    def _post_event(self, **kwargs):
        payload = {
            "player_id": TEST_PLAYER,
            "event_type": "victory",
            "weapon_id": "w001",
            "boss_id": "b001",
            "success": True,
            "time_to_kill": 120.0,
            **kwargs
        }
        return requests.post(f"{API_BASE}/events", json=payload, timeout=TIMEOUT)

    def test_post_event_success(self, api_available):
        r = self._post_event()
        assert r.status_code == 201
        data = r.json()
        assert data["db_persisted"] is True
        assert "event_id" in data

    def test_post_event_persisted_in_db(self, api_available, db_conn):
        self._post_event(player_id=TEST_PLAYER)
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM player_events WHERE player_id = %s", (TEST_PLAYER,))
            count = cur.fetchone()["n"]
        assert count >= 1, "El evento debe haberse persistido en PostgreSQL"

    def test_post_death_event(self, api_available):
        r = self._post_event(event_type="death", success=False, time_to_kill=None)
        assert r.status_code == 201

    def test_post_event_invalid_missing_fields(self, api_available):
        r = requests.post(f"{API_BASE}/events", json={"player_id": "x"}, timeout=TIMEOUT)
        assert r.status_code == 422  # Validation error

    def test_multiple_events_different_weapons(self, api_available, db_conn):
        for wid in ["w001", "w002", "w003"]:
            self._post_event(weapon_id=wid)
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(DISTINCT weapon_id) AS n FROM player_events WHERE player_id = %s",
                (TEST_PLAYER,)
            )
            count = cur.fetchone()["n"]
        assert count >= 3


# ─── Tests: Rankings ───────────────────────────────────────────────────────────
class TestRankings:
    def test_get_rankings(self, api_available):
        r = requests.get(f"{API_BASE}/rankings", timeout=TIMEOUT)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_rankings_limit_param(self, api_available):
        r = requests.get(f"{API_BASE}/rankings?limit=5", timeout=TIMEOUT)
        assert r.status_code == 200
        assert len(r.json()) <= 5

    def test_rankings_filter_by_boss(self, api_available):
        r = requests.get(f"{API_BASE}/rankings?boss_id=b001", timeout=TIMEOUT)
        assert r.status_code == 200
        for rank in r.json():
            assert rank.get("boss_id") == "b001" or "boss_id" not in rank


# ─── Tests: Meta-Shifts ────────────────────────────────────────────────────────
class TestMetaShifts:
    def test_get_meta_shifts(self, api_available):
        r = requests.get(f"{API_BASE}/meta-shifts", timeout=TIMEOUT)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_meta_shifts_limit(self, api_available):
        r = requests.get(f"{API_BASE}/meta-shifts?limit=5", timeout=TIMEOUT)
        assert r.status_code == 200
        assert len(r.json()) <= 5


# ─── Tests: PostgreSQL — Directo ──────────────────────────────────────────────
class TestDatabase:
    def test_weapons_table_has_data(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM weapons")
            assert cur.fetchone()["n"] >= 10

    def test_bosses_table_has_data(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM bosses")
            assert cur.fetchone()["n"] >= 8

    def test_effectiveness_table_populated(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM weapon_boss_effectiveness")
            assert cur.fetchone()["n"] >= 80, "Debe haber al menos 10 armas × 8 jefes"

    def test_effectiveness_scores_valid(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS n FROM weapon_boss_effectiveness
                WHERE effectiveness_score < 0 OR effectiveness_score > 100
            """)
            assert cur.fetchone()["n"] == 0, "Hay scores de efectividad fuera de [0,100]"

    def test_all_tables_exist(self, db_conn):
        expected_tables = {
            "weapons", "bosses", "weapon_boss_effectiveness",
            "player_events", "real_time_rankings", "meta_shifts"
        }
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
            """)
            actual = {row["tablename"] for row in cur.fetchall()}
        missing = expected_tables - actual
        assert not missing, f"Faltan tablas en la BD: {missing}"

    def test_foreign_key_consistency(self, db_conn):
        """weapon_boss_effectiveness debe referenciar armas y jefes válidos."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS n FROM weapon_boss_effectiveness e
                LEFT JOIN weapons w ON e.weapon_id = w.weapon_id
                WHERE w.weapon_id IS NULL
            """)
            orphans = cur.fetchone()["n"]
        assert orphans == 0, f"Hay {orphans} registros huérfanos en effectiveness"
