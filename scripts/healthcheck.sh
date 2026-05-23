#!/bin/bash
# =============================================================================
# healthcheck.sh — Verifica el estado completo de la pipeline
# Uso: bash scripts/healthcheck.sh
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
PASS=0; FAIL=0

ok()   { echo -e "  ${GREEN}✅ PASS${NC} — $1"; ((PASS++)); }
fail() { echo -e "  ${RED}❌ FAIL${NC} — $1"; ((FAIL++)); }
info() { echo -e "  ${YELLOW}ℹ️  INFO${NC} — $1"; }
section() { echo -e "\n${CYAN}═══ $1 ═══${NC}"; }

# ─────────────────────────────────────────────────────────────────────────────
section "1. Servicios Docker"
for svc in er_postgres er_kafka er_zookeeper er_streamlit er_grafana er_api er_simulator; do
    status=$(docker inspect --format='{{.State.Status}}' "$svc" 2>/dev/null || echo "not found")
    if [[ "$status" == "running" ]]; then
        ok "Contenedor $svc está running"
    else
        fail "Contenedor $svc está $status"
    fi
done

# ─────────────────────────────────────────────────────────────────────────────
section "2. PostgreSQL — Conexión y Tablas"
PG_CMD="docker exec er_postgres psql -U elden -d elden_ring -t -c"

for tabla in weapons bosses weapon_boss_effectiveness player_events real_time_rankings meta_shifts; do
    count=$($PG_CMD "SELECT COUNT(*) FROM $tabla;" 2>/dev/null | tr -d ' ')
    if [[ -n "$count" && "$count" -ge 0 ]]; then
        ok "Tabla '$tabla' accesible — $count filas"
    else
        fail "Tabla '$tabla' no accesible"
    fi
done

# ─────────────────────────────────────────────────────────────────────────────
section "3. Kafka — Topics"
topics=$(docker exec er_kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)
for topic in "player-events" "meta-shift-alerts" "weapon-stats"; do
    if echo "$topics" | grep -q "^$topic$"; then
        ok "Topic Kafka '$topic' existe"
    else
        fail "Topic Kafka '$topic' NO existe"
    fi
done

# ─────────────────────────────────────────────────────────────────────────────
section "4. FastAPI REST — Endpoints"
API="http://localhost:8000"

if curl -sf "$API/" > /dev/null 2>&1; then
    ok "GET /  responde"
else
    fail "GET / no responde"
fi

if curl -sf "$API/health" > /dev/null 2>&1; then
    ok "GET /health responde"
else
    fail "GET /health no responde"
fi

weapons_count=$(curl -sf "$API/weapons" 2>/dev/null | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [[ "$weapons_count" -gt 0 ]]; then
    ok "GET /weapons devuelve $weapons_count armas"
else
    fail "GET /weapons devuelve 0 resultados"
fi

bosses_count=$(curl -sf "$API/bosses" 2>/dev/null | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [[ "$bosses_count" -gt 0 ]]; then
    ok "GET /bosses devuelve $bosses_count jefes"
else
    fail "GET /bosses devuelve 0 resultados"
fi

effectiveness_count=$(curl -sf "$API/effectiveness/b001" 2>/dev/null | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [[ "$effectiveness_count" -gt 0 ]]; then
    ok "GET /effectiveness/b001 devuelve $effectiveness_count scores"
else
    fail "GET /effectiveness/b001 sin datos"
fi

# Test POST /events
post_result=$(curl -sf -X POST "$API/events" \
    -H "Content-Type: application/json" \
    -d '{"player_id":"hc_test","event_type":"victory","weapon_id":"w001","boss_id":"b001","success":true,"time_to_kill":90.0}' \
    2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('db_persisted',''))" 2>/dev/null || echo "")
if [[ "$post_result" == "True" ]]; then
    ok "POST /events persiste en PostgreSQL"
else
    fail "POST /events no persiste"
fi

# ─────────────────────────────────────────────────────────────────────────────
section "5. Streamlit — Disponibilidad"
if curl -sf "http://localhost:8501" > /dev/null 2>&1; then
    ok "Streamlit responde en :8501"
else
    fail "Streamlit no responde"
fi

# ─────────────────────────────────────────────────────────────────────────────
section "6. Grafana — Disponibilidad"
grafana_status=$(curl -sf -u admin:admin "http://localhost:3000/api/health" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('database','?'))" 2>/dev/null || echo "?")
if [[ "$grafana_status" == "ok" ]]; then
    ok "Grafana API healthcheck: database=$grafana_status"
else
    fail "Grafana no responde correctamente (database=$grafana_status)"
fi

datasource_count=$(curl -sf -u admin:admin "http://localhost:3000/api/datasources" 2>/dev/null | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [[ "$datasource_count" -gt 0 ]]; then
    ok "Grafana tiene $datasource_count datasource(s) provisionado(s)"
else
    fail "Grafana sin datasources"
fi

# ─────────────────────────────────────────────────────────────────────────────
section "7. Flujo End-to-End — Simulador → DB"
events_before=$($PG_CMD "SELECT COUNT(*) FROM player_events;" 2>/dev/null | tr -d ' ')
sleep 3
events_after=$($PG_CMD "SELECT COUNT(*) FROM player_events;" 2>/dev/null | tr -d ' ')
new_events=$((events_after - events_before))
if [[ "$new_events" -gt 0 ]]; then
    ok "El simulador insertó $new_events nuevos eventos en 3 segundos"
else
    info "No se detectaron nuevos eventos (el simulador puede estar iniciando)"
fi

# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}═════════════════════════════════════════${NC}"
echo -e "  RESULTADO: ${GREEN}${PASS} passed${NC}  |  ${RED}${FAIL} failed${NC}"
echo -e "${CYAN}═════════════════════════════════════════${NC}"

if [[ "$FAIL" -gt 0 ]]; then
    echo -e "  ${YELLOW}💡 Tip: ejecuta 'make logs' para ver errores detallados${NC}"
    exit 1
else
    echo -e "  ${GREEN}🎉 Pipeline completamente operativa!${NC}"
fi
