# ⚔️ Elden Ring Meta-Build Intelligence

> **Proyecto Big Data Aplicado 2026** — Arquitectura End-to-End con Apache Kafka, Spark, Flink, PostgreSQL, Streamlit, Grafana y FastAPI.

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ELDEN RING META-BUILD INTELLIGENCE                        │
├─────────────────────────────────────────────────────────────────────────────┤
│  INGESTA            MENSAJERÍA       PROCESAMIENTO      ALMACENAMIENTO      │
│  ┌──────────┐      ┌──────────────┐  ┌─────────────┐   ┌─────────────────┐ │
│  │Elden Ring│      │Apache Kafka  │  │Apache Spark │   │PostgreSQL 17    │ │
│  │Fan API   │─────▶│player-events │─▶│Batch Job    │──▶│weapons / bosses │ │
│  │(REST)    │      │              │  │(efectividad)│   │effectiveness    │ │
│  └──────────┘      │meta-shift    │  └─────────────┘   └─────────────────┘ │
│  ┌──────────┐      │alerts        │  ┌─────────────┐   ┌─────────────────┐ │
│  │Simulador │─────▶│weapon-stats  │─▶│Apache Flink │──▶│real_time_rank.  │ │
│  │Python    │      │(3 topics)    │  │Stream Job   │   │meta_shifts      │ │
│  └──────────┘      └──────────────┘  │(ventanas 5m)│   │player_events    │ │
│                                      └─────────────┘   └─────────────────┘ │
├─────────────────────────────────────────────────────────────────────────────┤
│  SALIDA: Grafana 12 │ Streamlit 1.45 │ Discord Alerts │ FastAPI REST       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Stack de Tecnologías (Versiones Actualizadas 2026)

| Componente | Imagen / Versión | Puerto |
|---|---|---|
| **PostgreSQL** | `postgres:17-alpine` | 5432 |
| **Apache Kafka** | `confluentinc/cp-kafka:7.9.2` | 9092 |
| **Apache ZooKeeper** | `confluentinc/cp-zookeeper:7.9.2` | 2181 |
| **Apache Spark** | `bitnami/spark:3.5.6` | 8081, 7077 |
| **Apache Flink** | `flink:1.20.1-scala_2.12-java11` (custom) | 8082 |
| **Apache NiFi** | `apache/nifi:2.4.0` (perfil opcional) | 8443 (HTTPS) |
| **Streamlit** | `streamlit==1.45.1` | 8501 |
| **Grafana** | `grafana/grafana:12.0.1` | 3000 |
| **FastAPI** | `fastapi==0.115.12` | 8000 |
| **Discord Bot** | `kafka-python==2.1.5` | — |
| **Simulador** | `kafka-python==2.1.5` | — |

---

## 📁 Estructura del Proyecto

```
elden-ring-meta/
├── docker-compose.yml
├── .env
├── README.md
├── kafka/
│   └── init-topics.sh          # Crea topics: player-events, meta-shift-alerts, weapon-stats
├── nifi/
│   └── flows/elden_ring_ingest.xml
├── postgres/
│   └── init/
│       ├── 01-init.sql         # Esquema: weapons, bosses, rankings, meta_shifts, events
│       └── 02-seed-data.sql    # 10 armas + 8 jefes populares + efectividad inicial
├── spark/
│   ├── Dockerfile              # bitnami/spark:3.5.6 + requests + psycopg2 + JDBC
│   ├── jars/
│   │   ├── postgresql-42.7.1.jar
│   │   └── postgresql-42.7.4.jar  ← versión activa
│   └── jobs/
│       ├── batch_processing.py # Ingesta API → limpieza → cross-join efectividad
│       └── utils.py
├── flink/
│   ├── Dockerfile              # flink:1.20.1 + PyFlink + kafka-python + psycopg2
│   └── jobs/
│       └── stream_processing.py  # Ventanas 5 min + Meta-Shift detection > 15%
├── simulator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── player_events.py        # 100 jugadores, meta dinámica, shift cada 500 eventos
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                 # FastAPI: /weapons /bosses /effectiveness /rankings /events
├── discord-bot/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── bot.py                  # Consume meta-shift-alerts → Discord Webhook embed
├── streamlit/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                  # Dashboard | Recomendador | Rankings | Alertas
│   └── utils.py
└── grafana/
    ├── dashboards/
    │   ├── dashboard.yml
    │   └── elden_ring_dashboard.json
    └── datasources/
        └── datasource.yml      # PostgreSQL auto-provisionado con uid
```

---

## 🚀 Despliegue

### Prerrequisitos
- Docker >= 24 y Docker Compose v2 (plugin, no `docker-compose` separado)
- 6 GB de RAM mínimo disponibles para Docker
- Conexión a Internet (descarga de imágenes y la API de Elden Ring)

### 1. Configuración de Variables de Entorno

Edita el archivo `.env` en la raíz del proyecto:

```env
POSTGRES_USER=elden
POSTGRES_PASSWORD=ring123
POSTGRES_DB=elden_ring
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/TU_ID/TU_TOKEN
SIMULATION_SPEED=2.0
```

> `DISCORD_WEBHOOK_URL` es opcional. Sin él, las alertas aparecen solo en los logs del bot.

### 2. Levantar la Infraestructura

```bash
# Desde la carpeta del proyecto
docker compose up -d --build
```

> **Sin NiFi** (ahorra ~1 GB RAM): NiFi está en el perfil `nifi` y no arranca por defecto.  
> Para incluirlo: `docker compose --profile nifi up -d --build`

### 3. Verificar el Estado

```bash
docker compose ps
```

Todos los servicios deben aparecer como `healthy` o `running` tras ~60 segundos.

---

## 🌐 Puertos y Accesos

| Servicio | URL | Credenciales |
|---|---|---|
| 🎮 **Streamlit** (Build Recommender) | http://localhost:8501 | — |
| 📊 **Grafana** (Dashboard) | http://localhost:3000 | admin / admin |
| 🔌 **FastAPI** (REST + Swagger) | http://localhost:8000/docs | — |
| 🌊 **Flink** (Web Dashboard) | http://localhost:8082 | — |
| 🔥 **Spark** (Web UI) | http://localhost:8081 | — |
| 📦 **NiFi** (perfil nifi) | https://localhost:8443/nifi | admin / admin123456789 |

---

## 🔍 Verificación del Pipeline

### Monitorear el flujo de datos en tiempo real

```bash
# Eventos del simulador (Kafka producer)
docker logs -f er_simulator

# Ventanas de Flink procesando events
docker logs -f er_flink_stream

# Resultados del job batch de Spark
docker logs er_spark_batch

# Alertas del bot de Discord
docker logs -f er_discord_bot
```

### Inyectar un evento manual vía API REST

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "player_id": "player_test_01",
    "event_type": "victory",
    "weapon_id": "w001",
    "boss_id": "b007",
    "location": "Elphael",
    "success": true,
    "time_to_kill": 120.5
  }'
```

### Consultar rankings actuales

```bash
curl http://localhost:8000/rankings?boss_id=b007 | python3 -m json.tool
```

---

## 🧠 Flujo de Datos Detallado

### Procesamiento Batch (Apache Spark)
1. Spark extrae datos de la **Elden Ring Fan API** (armas y jefes)
2. Limpia y normaliza los datos usando DataFrames con schemas explícitos
3. Calcula un **cross-join enriquecido** entre todas las armas y todos los jefes
4. Aplica la fórmula de efectividad: `score = Σ(daño_tipo / (defensa_tipo + 1) × 10)`
5. Escribe el resultado en la tabla `weapon_boss_effectiveness` (datos seed ya incluidos)

### Procesamiento Streaming (Apache Flink via kafka-python)
1. El **simulador** genera eventos realistas y los publica en el topic Kafka `player-events`
2. Flink abre **ventanas tumbling de 5 minutos** agrupadas por `(weapon_id, boss_id)`
3. Calcula `win_rate`, `total_attempts`, `victories`, `deaths` y `avg_time_to_kill`
4. Compara con la ventana anterior: si `|Δwin_rate| > 15%` → **Meta-Shift Alert**
5. Escribe en `real_time_rankings` (upsert) y `meta_shifts`
6. Publica la alerta al topic `meta-shift-alerts` de Kafka
7. El **Discord Bot** consume ese topic y envía un embed visual al webhook configurado

---

## 📋 Tablas de la Base de Datos

| Tabla | Fuente | Descripción |
|---|---|---|
| `weapons` | Spark + seed | Armas del juego con stats y scaling |
| `bosses` | Spark + seed | Jefes con HP, defensas y debilidades |
| `weapon_boss_effectiveness` | Spark | Score teórico 0-100 por par arma/jefe |
| `player_events` | Simulador + API | Log de eventos individuales de combate |
| `real_time_rankings` | Flink | Win rate agregado por ventana de 5 min |
| `meta_shifts` | Flink | Histórico de alertas de cambio de meta |
