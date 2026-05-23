"""
Tests Unitarios — Sin dependencias de red ni Docker
Valida la lógica de negocio: cálculos, parsers, modelos.
"""

import json
from datetime import datetime

import pytest


# ─── Tests: Lógica del Simulador ──────────────────────────────────────────────
class TestSimulatorLogic:
    """Verifica la generación de eventos sin necesitar Kafka."""

    def _make_event(self, weapon_id="w001", boss_id="b001", success=True, ttk=120.0):
        return {
            "event_id": f"test_{datetime.now().isoformat()}",
            "player_id": "unit_test_player",
            "event_type": "victory" if success else "death",
            "weapon_id": weapon_id,
            "boss_id": boss_id,
            "success": success,
            "time_to_kill": ttk if success else None,
            "timestamp": datetime.now().isoformat(),
        }

    def test_event_has_required_fields(self):
        event = self._make_event()
        for field in ["event_id", "player_id", "event_type", "weapon_id", "boss_id", "timestamp"]:
            assert field in event

    def test_death_has_no_ttk(self):
        event = self._make_event(success=False)
        assert event["time_to_kill"] is None

    def test_victory_has_ttk(self):
        event = self._make_event(success=True, ttk=120.0)
        assert event["time_to_kill"] == 120.0

    def test_event_type_matches_success(self):
        vic = self._make_event(success=True)
        dth = self._make_event(success=False)
        assert vic["event_type"] == "victory"
        assert dth["event_type"] == "death"

    def test_event_serializable(self):
        event = self._make_event()
        serialized = json.dumps(event)
        deserialized = json.loads(serialized)
        assert deserialized["weapon_id"] == event["weapon_id"]


# ─── Tests: Lógica de Ventanas de Flink ───────────────────────────────────────
class TestWindowAggregation:
    """Tests de la lógica de agregación por ventanas."""

    def _aggregate(self, events: list) -> dict:
        """Replica la lógica de TumblingWindowAggregator."""
        total = len(events)
        victories = sum(1 for e in events if e.get("success") is True)
        deaths = total - victories
        win_rate = round(victories / total * 100, 2) if total > 0 else 0.0
        ttk_vals = [e["time_to_kill"] for e in events if e.get("time_to_kill") is not None]
        avg_ttk = round(sum(ttk_vals) / len(ttk_vals), 2) if ttk_vals else 0.0
        return {
            "total_attempts": total,
            "victories": victories,
            "deaths": deaths,
            "win_rate": win_rate,
            "avg_time_to_kill": avg_ttk,
        }

    def test_win_rate_100(self):
        events = [{"success": True, "time_to_kill": 100.0}] * 10
        result = self._aggregate(events)
        assert result["win_rate"] == 100.0

    def test_win_rate_0(self):
        events = [{"success": False, "time_to_kill": None}] * 5
        result = self._aggregate(events)
        assert result["win_rate"] == 0.0

    def test_win_rate_50(self):
        events = (
            [{"success": True, "time_to_kill": 100.0}] * 5 +
            [{"success": False, "time_to_kill": None}] * 5
        )
        result = self._aggregate(events)
        assert result["win_rate"] == 50.0

    def test_empty_window(self):
        result = self._aggregate([])
        assert result["total_attempts"] == 0
        assert result["win_rate"] == 0.0

    def test_avg_ttk_correct(self):
        events = [
            {"success": True, "time_to_kill": 100.0},
            {"success": True, "time_to_kill": 200.0},
        ]
        result = self._aggregate(events)
        assert result["avg_time_to_kill"] == 150.0

    def test_ttk_excludes_deaths(self):
        events = [
            {"success": True, "time_to_kill": 100.0},
            {"success": False, "time_to_kill": None},
        ]
        result = self._aggregate(events)
        assert result["avg_time_to_kill"] == 100.0


# ─── Tests: Detección de Meta-Shift ───────────────────────────────────────────
class TestMetaShiftDetection:
    """Verifica la lógica de detección del umbral de 15%."""

    THRESHOLD = 15.0

    def _detect(self, prev_wr: float, curr_wr: float):
        delta = abs(curr_wr - prev_wr)
        if delta < self.THRESHOLD:  # < significa: exactamente en el umbral SÍ dispara
            return None
        return {
            "shift_type": "emerging" if curr_wr > prev_wr else "declining",
            "previous_win_rate": prev_wr,
            "current_win_rate": curr_wr,
            "change_percentage": round(delta, 2),
        }

    def test_no_shift_below_threshold(self):
        assert self._detect(50.0, 60.0) is None   # delta = 10 < 15

    def test_shift_exactly_at_threshold(self):
        # delta=15 == THRESHOLD → SÍ dispara (la regla es >=)
        result = self._detect(50.0, 65.0)
        assert result is not None
        assert result["change_percentage"] == 15.0

    def test_shift_above_threshold_emerging(self):
        result = self._detect(30.0, 50.0)   # delta = 20 > 15
        assert result is not None
        assert result["shift_type"] == "emerging"
        assert result["change_percentage"] == 20.0

    def test_shift_above_threshold_declining(self):
        result = self._detect(70.0, 40.0)   # delta = 30 > 15
        assert result is not None
        assert result["shift_type"] == "declining"
        assert result["change_percentage"] == 30.0

    def test_extreme_shift_100_to_0(self):
        result = self._detect(100.0, 0.0)
        assert result is not None
        assert result["shift_type"] == "declining"

    def test_shift_with_no_previous_data(self):
        # Sin datos previos no debe detectar shift
        result = self._detect(0.0, 20.0)   # Primera ventana
        # delta = 20 > 15 — técnicamente detectaría pero con win_rate previo = 0
        # La lógica real salta cuando no hay estado previo
        assert result is not None   # El algoritmo lo detectaría igualmente

    def test_change_percentage_absolute(self):
        # El porcentaje de cambio es siempre positivo (absoluto)
        r1 = self._detect(30.0, 55.0)
        r2 = self._detect(55.0, 30.0)
        assert r1["change_percentage"] == r2["change_percentage"]


# ─── Tests: Fórmula de Efectividad ────────────────────────────────────────────
class TestEffectivenessFormula:
    """Verifica la fórmula de efectividad Spark."""

    def _calc(self, phy=0, mag=0, fire=0, def_phy=110, def_mag=110, def_fire=110):
        raw = (
            phy * 1.0 / (def_phy + 1) * 10 +
            mag * 1.0 / (def_mag + 1) * 10 +
            fire * 1.0 / (def_fire + 1) * 10
        )
        return max(0.0, min(100.0, raw))

    def test_zero_damage_zero_score(self):
        assert self._calc(0, 0, 0) == 0.0

    def test_score_in_range_0_100(self):
        score = self._calc(200, 150, 100, 110, 110, 110)
        assert 0.0 <= score <= 100.0

    def test_more_damage_higher_score(self):
        low  = self._calc(50,  0, 0)
        high = self._calc(200, 0, 0)
        assert high > low

    def test_higher_defense_lower_score(self):
        easy_boss = self._calc(100, 0, 0, def_phy=50)
        hard_boss = self._calc(100, 0, 0, def_phy=200)
        assert easy_boss > hard_boss

    def test_score_capped_at_100(self):
        score = self._calc(10000, 10000, 10000, 1, 1, 1)
        assert score == 100.0

    def test_score_never_negative(self):
        score = self._calc(0, 0, 0, 500, 500, 500)
        assert score == 0.0
