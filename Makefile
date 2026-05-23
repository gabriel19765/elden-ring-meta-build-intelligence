# ╔══════════════════════════════════════════════════════════════════════════╗
# ║          ELDEN RING META-BUILD INTELLIGENCE — Makefile                 ║
# ║  Uso: make <target>    |    make help (para ver todos los comandos)    ║
# ╚══════════════════════════════════════════════════════════════════════════╝

COMPOSE   = docker compose
PROJECT   = elden-ring-meta
PYTHON    = python3
API_URL   = http://localhost:8000
ST_URL    = http://localhost:8501

# Colores ANSI
GREEN  = \033[0;32m
YELLOW = \033[1;33m
CYAN   = \033[0;36m
RED    = \033[0;31m
RESET  = \033[0m

.DEFAULT_GOAL := help

# ─── AYUDA ──────────────────────────────────────────────────────────────────
.PHONY: help
help: ## Muestra esta ayuda
	@echo ""
	@echo "$(CYAN)⚔️  Elden Ring Meta-Build Intelligence$(RESET)"
	@echo "$(CYAN)══════════════════════════════════════$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-18s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ─── CICLO DE VIDA ──────────────────────────────────────────────────────────
.PHONY: up
up: ## Levanta toda la infraestructura (build si es necesario)
	@echo "$(YELLOW)🚀 Iniciando Elden Ring Meta-Build Intelligence...$(RESET)"
	$(COMPOSE) up -d --build
	@echo "$(GREEN)✅ Stack levantado. Espera ~60s para que los servicios estén listos.$(RESET)"
	@echo "   Streamlit : $(CYAN)$(ST_URL)$(RESET)"
	@echo "   Grafana   : $(CYAN)http://localhost:3000$(RESET)  (admin/admin)"
	@echo "   FastAPI   : $(CYAN)$(API_URL)/docs$(RESET)"
	@echo "   Flink UI  : $(CYAN)http://localhost:8082$(RESET)"
	@echo "   Spark UI  : $(CYAN)http://localhost:8081$(RESET)"

.PHONY: down
down: ## Para y elimina todos los contenedores (preserva volúmenes)
	@echo "$(YELLOW)🛑 Deteniendo servicios...$(RESET)"
	$(COMPOSE) down
	@echo "$(GREEN)✅ Servicios detenidos.$(RESET)"

.PHONY: reset
reset: ## Para servicios, elimina volúmenes y datos (¡reset completo!)
	@echo "$(RED)⚠️  Reset completo — se borrarán TODOS los datos...$(RESET)"
	$(COMPOSE) down -v --remove-orphans
	@echo "$(GREEN)✅ Reset completo.$(RESET)"

.PHONY: restart
restart: down up ## Reinicia todos los servicios

.PHONY: build
build: ## Reconstruye todas las imágenes sin levantar
	$(COMPOSE) build --parallel

.PHONY: logs
logs: ## Muestra logs de todos los servicios en tiempo real
	$(COMPOSE) logs -f --tail=50

.PHONY: status
status: ## Estado de todos los contenedores
	$(COMPOSE) ps

# ─── LOGS POR SERVICIO ───────────────────────────────────────────────────────
.PHONY: logs-sim logs-flink logs-spark logs-api logs-bot
logs-sim:   ## Logs del simulador de eventos
	$(COMPOSE) logs -f --tail=100 simulator

logs-flink: ## Logs del job de Flink (stream processing)
	$(COMPOSE) logs -f --tail=100 flink-stream-job

logs-spark: ## Logs del job batch de Spark
	$(COMPOSE) logs --tail=100 spark-batch-job

logs-api:   ## Logs de la API REST (FastAPI)
	$(COMPOSE) logs -f --tail=100 api

logs-bot:   ## Logs del Discord Bot
	$(COMPOSE) logs -f --tail=100 discord-bot

# ─── DEMO Y PRUEBAS ─────────────────────────────────────────────────────────
.PHONY: demo
demo: ## Inyecta 200 eventos de demo y muestra resultados en consola
	@echo "$(YELLOW)⚔️  Ejecutando demo del pipeline...$(RESET)"
	@bash scripts/demo.sh

.PHONY: healthcheck
healthcheck: ## Verifica que toda la pipeline esté funcionando
	@echo "$(YELLOW)🔍 Verificando estado del pipeline...$(RESET)"
	@bash scripts/healthcheck.sh

.PHONY: test
test: ## Ejecuta los tests de integración del pipeline
	@echo "$(YELLOW)🧪 Ejecutando tests de integración...$(RESET)"
	$(PYTHON) -m pytest tests/ -v --tb=short 2>&1 | head -80
	@echo "$(GREEN)✅ Tests completados.$(RESET)"

.PHONY: test-unit
test-unit: ## Ejecuta solo los tests unitarios (sin Docker)
	$(PYTHON) -m pytest tests/test_unit.py -v --tb=short

# ─── BASE DE DATOS ───────────────────────────────────────────────────────────
.PHONY: db-shell
db-shell: ## Abre una sesión psql interactiva en PostgreSQL
	$(COMPOSE) exec postgres psql -U elden -d elden_ring

.PHONY: db-stats
db-stats: ## Muestra estadísticas rápidas de las tablas
	@$(COMPOSE) exec postgres psql -U elden -d elden_ring -c "\
		SELECT 'weapons' AS tabla, COUNT(*) AS registros FROM weapons \
		UNION ALL SELECT 'bosses', COUNT(*) FROM bosses \
		UNION ALL SELECT 'effectiveness', COUNT(*) FROM weapon_boss_effectiveness \
		UNION ALL SELECT 'player_events', COUNT(*) FROM player_events \
		UNION ALL SELECT 'real_time_rankings', COUNT(*) FROM real_time_rankings \
		UNION ALL SELECT 'meta_shifts', COUNT(*) FROM meta_shifts \
		ORDER BY tabla;"

.PHONY: db-reset-rankings
db-reset-rankings: ## Limpia los rankings en tiempo real (sin borrar datos históricos)
	@$(COMPOSE) exec postgres psql -U elden -d elden_ring \
		-c "TRUNCATE real_time_rankings, meta_shifts, player_events;"
	@echo "$(GREEN)✅ Rankings reseteados.$(RESET)"

# ─── KAFKA ───────────────────────────────────────────────────────────────────
.PHONY: kafka-topics
kafka-topics: ## Lista los topics de Kafka con número de mensajes
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

.PHONY: kafka-monitor
kafka-monitor: ## Monitorea el topic player-events en tiempo real
	$(COMPOSE) exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic player-events \
		--from-beginning \
		--max-messages 10

# ─── DEVELOPMENT ─────────────────────────────────────────────────────────────
.PHONY: lint
lint: ## Ejecuta ruff (linter) sobre todo el código Python
	@$(PYTHON) -m ruff check . --select E,W,F 2>/dev/null || \
		echo "$(YELLOW)Instala ruff: pip install ruff$(RESET)"

.PHONY: format
format: ## Formatea el código con black
	@$(PYTHON) -m black . 2>/dev/null || \
		echo "$(YELLOW)Instala black: pip install black$(RESET)"

# ─── INFORMACIÓN ─────────────────────────────────────────────────────────────
.PHONY: urls
urls: ## Muestra todas las URLs del proyecto
	@echo ""
	@echo "$(CYAN)🌐 URLs del Proyecto$(RESET)"
	@echo "$(CYAN)══════════════════$(RESET)"
	@echo "  Streamlit (Frontend)    : $(GREEN)http://localhost:8501$(RESET)"
	@echo "  Grafana (Dashboard)     : $(GREEN)http://localhost:3000$(RESET) — admin/admin"
	@echo "  FastAPI (REST + Swagger): $(GREEN)http://localhost:8000/docs$(RESET)"
	@echo "  Flink Web Dashboard     : $(GREEN)http://localhost:8082$(RESET)"
	@echo "  Spark Master UI         : $(GREEN)http://localhost:8081$(RESET)"
	@echo "  PostgreSQL              : $(GREEN)localhost:5432$(RESET) — elden/ring123"
	@echo ""
