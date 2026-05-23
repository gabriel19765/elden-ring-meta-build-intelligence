#!/bin/bash
# =============================================================================
# demo.sh — Demo interactivo del pipeline completo
# Inyecta eventos de escenarios reales y muestra resultados en tiempo real
# Uso: bash scripts/demo.sh
# =============================================================================

API="http://localhost:8000"
CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

banner() {
    echo -e "\n${CYAN}══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  ⚔️  $1${NC}"
    echo -e "${CYAN}══════════════════════════════════════════════════${NC}\n"
}

wait_bar() {
    local secs=$1 msg=$2
    echo -ne "  ${YELLOW}⏳ $msg${NC}"
    for i in $(seq 1 $secs); do
        sleep 1
        echo -n "."
    done
    echo ""
}

# ─── Verificar que la API esté lista ─────────────────────────────────────────
if ! curl -sf "$API/health" > /dev/null 2>&1; then
    echo -e "${RED}❌ API no disponible en $API. Ejecuta 'make up' primero.${NC}"
    exit 1
fi

banner "DEMO: Elden Ring Meta-Build Intelligence Pipeline"
echo "  Este script simula una sesión intensa de juego y muestra cómo"
echo "  la pipeline Big Data detecta el metagame en tiempo real."
echo ""

# ─── ESCENARIO 1: Moonveil domina contra Margit ──────────────────────────────
banner "ESCENARIO 1: Moonveil (w001) es el arma meta vs Margit (b001)"
echo "  Simulando 60 victorias con Moonveil vs Margit..."

for i in $(seq 1 60); do
    curl -sf -X POST "$API/events" \
        -H "Content-Type: application/json" \
        -d "{\"player_id\":\"demo_$i\",\"event_type\":\"victory\",\"weapon_id\":\"w001\",\"boss_id\":\"b001\",\"success\":true,\"time_to_kill\":$(python3 -c "import random; print(round(random.uniform(80,200),1))")}" \
        > /dev/null 2>&1
done

for i in $(seq 1 20); do
    curl -sf -X POST "$API/events" \
        -H "Content-Type: application/json" \
        -d "{\"player_id\":\"demo_d$i\",\"event_type\":\"death\",\"weapon_id\":\"w001\",\"boss_id\":\"b001\",\"success\":false}" \
        > /dev/null 2>&1
done
echo -e "  ${GREEN}✅ 80 eventos Moonveil vs Margit inyectados${NC}"

# ─── ESCENARIO 2: Rivers of Blood domina vs Malenia ──────────────────────────
banner "ESCENARIO 2: Rivers of Blood (w002) vs Malenia (b007)"
echo "  Simulando sesión de Rivers of Blood contra Malenia..."

for i in $(seq 1 45); do
    curl -sf -X POST "$API/events" \
        -H "Content-Type: application/json" \
        -d "{\"player_id\":\"rivers_$i\",\"event_type\":\"victory\",\"weapon_id\":\"w002\",\"boss_id\":\"b007\",\"success\":true,\"time_to_kill\":$(python3 -c "import random; print(round(random.uniform(120,400),1))")}" \
        > /dev/null 2>&1
done
for i in $(seq 1 30); do
    curl -sf -X POST "$API/events" \
        -H "Content-Type: application/json" \
        -d "{\"player_id\":\"rivers_d$i\",\"event_type\":\"death\",\"weapon_id\":\"w002\",\"boss_id\":\"b007\",\"success\":false}" \
        > /dev/null 2>&1
done
echo -e "  ${GREEN}✅ 75 eventos Rivers vs Malenia inyectados${NC}"

# ─── ESCENARIO 3: Dark Moon Greatsword vs Rennala ────────────────────────────
banner "ESCENARIO 3: Dark Moon Greatsword (w005) vs Rennala (b003)"
echo "  Simulando el build perfecto de mago..."

for i in $(seq 1 50); do
    curl -sf -X POST "$API/events" \
        -H "Content-Type: application/json" \
        -d "{\"player_id\":\"mage_$i\",\"event_type\":\"victory\",\"weapon_id\":\"w005\",\"boss_id\":\"b003\",\"success\":true,\"time_to_kill\":$(python3 -c "import random; print(round(random.uniform(60,150),1))")}" \
        > /dev/null 2>&1
done
for i in $(seq 1 5); do
    curl -sf -X POST "$API/events" \
        -H "Content-Type: application/json" \
        -d "{\"player_id\":\"mage_d$i\",\"event_type\":\"death\",\"weapon_id\":\"w005\",\"boss_id\":\"b003\",\"success\":false}" \
        > /dev/null 2>&1
done
echo -e "  ${GREEN}✅ 55 eventos Dark Moon vs Rennala inyectados (win rate ~91%)${NC}"

# ─── Estadísticas finales ─────────────────────────────────────────────────────
banner "RESULTADOS DEL DEMO"

total=$(docker exec er_postgres psql -U elden -d elden_ring -t -c \
    "SELECT COUNT(*) FROM player_events WHERE player_id LIKE 'demo_%' OR player_id LIKE 'rivers_%' OR player_id LIKE 'mage_%';" 2>/dev/null | tr -d ' ')

echo -e "  ${GREEN}📊 Eventos inyectados en esta sesión: $total${NC}"
echo ""

echo -e "  ${CYAN}Top Rankings actuales (desde la API):${NC}"
curl -sf "$API/rankings" 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
if not data:
    print('  (Flink aún está procesando los datos...)')
else:
    print(f'  {'Arma':<30} {'Jefe':<35} {'WR%':>6} {'Intentos':>8}')
    print('  ' + '-'*82)
    for r in data[:8]:
        print(f\"  {r.get('weapon_name','?'):<30} {r.get('boss_name','?'):<35} {r.get('win_rate',0):>6.1f}% {r.get('total_attempts',0):>8}\")
" 2>/dev/null || echo "  (Rankings se generarán tras la primera ventana de Flink de 5 min)"

echo ""
echo -e "  ${YELLOW}💡 Visita http://localhost:8501 para ver los resultados en Streamlit${NC}"
echo -e "  ${YELLOW}💡 Visita http://localhost:3000 para ver los dashboards en Grafana${NC}"
echo ""
