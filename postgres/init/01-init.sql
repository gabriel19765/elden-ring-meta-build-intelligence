-- ============================================================================
-- SCHEMA: Elden Ring Meta-Build Intelligence
-- PostgreSQL 17 | Big Data Aplicado 2026
-- ============================================================================

-- ─── WEAPONS ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weapons (
    weapon_id         VARCHAR(50)  PRIMARY KEY,
    name              VARCHAR(100) NOT NULL,
    category          VARCHAR(50)  NOT NULL,         -- Katana, Greatsword, Dagger…
    weapon_type       VARCHAR(30)  DEFAULT 'Melee',  -- Melee | Magic | Ranged
    damage_physical   INTEGER      DEFAULT 0,
    damage_magic      INTEGER      DEFAULT 0,
    damage_fire       INTEGER      DEFAULT 0,
    damage_lightning  INTEGER      DEFAULT 0,
    damage_holy       INTEGER      DEFAULT 0,
    weight            DECIMAL(5,2) DEFAULT 0.0,
    requirements_str  INTEGER      DEFAULT 0,
    requirements_dex  INTEGER      DEFAULT 0,
    requirements_int  INTEGER      DEFAULT 0,
    requirements_fai  INTEGER      DEFAULT 0,
    requirements_arc  INTEGER      DEFAULT 0,
    scaling_str       VARCHAR(2)   DEFAULT '',
    scaling_dex       VARCHAR(2)   DEFAULT '',
    scaling_int       VARCHAR(2)   DEFAULT '',
    scaling_fai       VARCHAR(2)   DEFAULT '',
    scaling_arc       VARCHAR(2)   DEFAULT '',
    source            VARCHAR(20)  DEFAULT 'seed',   -- seed | api | manual
    created_at        TIMESTAMP    DEFAULT NOW()
);

-- ─── BOSSES ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bosses (
    boss_id           VARCHAR(50)  PRIMARY KEY,
    name              VARCHAR(100) NOT NULL,
    location          VARCHAR(500) DEFAULT 'Unknown',
    health_points     INTEGER      DEFAULT 0,
    defense_physical  INTEGER      DEFAULT 100,
    defense_magic     INTEGER      DEFAULT 100,
    defense_fire      INTEGER      DEFAULT 100,
    weaknesses        TEXT[]       DEFAULT '{}',
    resistances       TEXT[]       DEFAULT '{}',
    source            VARCHAR(20)  DEFAULT 'seed',
    created_at        TIMESTAMP    DEFAULT NOW()
);

-- ─── EFFECTIVENESS MATRIX (Batch — Apache Spark) ──────────────────────────────
CREATE TABLE IF NOT EXISTS weapon_boss_effectiveness (
    id                SERIAL       PRIMARY KEY,
    weapon_id         VARCHAR(50)  NOT NULL REFERENCES weapons(weapon_id) ON DELETE CASCADE,
    boss_id           VARCHAR(50)  NOT NULL REFERENCES bosses(boss_id)   ON DELETE CASCADE,
    effectiveness_score DECIMAL(5,2) NOT NULL CHECK (effectiveness_score BETWEEN 0 AND 100),
    calculated_at     TIMESTAMP    DEFAULT NOW(),
    UNIQUE (weapon_id, boss_id)
);

-- ─── PLAYER EVENTS (Raw ingestion) ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS player_events (
    event_id          VARCHAR(80)  PRIMARY KEY,
    player_id         VARCHAR(50)  NOT NULL,
    event_type        VARCHAR(30)  NOT NULL CHECK (event_type IN ('death','victory','equip_weapon','enter_boss')),
    weapon_id         VARCHAR(50)  NOT NULL,
    boss_id           VARCHAR(50)  NOT NULL,
    location          VARCHAR(500) DEFAULT 'Unknown',
    timestamp         TIMESTAMP    NOT NULL DEFAULT NOW(),
    is_meta_weapon    BOOLEAN      DEFAULT FALSE,
    created_at        TIMESTAMP    DEFAULT NOW()
);

-- ─── REAL-TIME RANKINGS (Streaming — Apache Flink, ventanas de 5 min) ─────────
CREATE TABLE IF NOT EXISTS real_time_rankings (
    id                SERIAL       PRIMARY KEY,
    weapon_id         VARCHAR(50)  NOT NULL,
    boss_id           VARCHAR(50)  NOT NULL,
    total_attempts    INTEGER      DEFAULT 0,
    total_victories   INTEGER      DEFAULT 0,
    total_deaths      INTEGER      DEFAULT 0,
    win_rate          DECIMAL(5,2) DEFAULT 0.0 CHECK (win_rate BETWEEN 0 AND 100),
    avg_time_to_kill  DECIMAL(8,2) DEFAULT 0.0,
    window_start      TIMESTAMP    NOT NULL DEFAULT (NOW() - INTERVAL '5 minutes'),
    window_end        TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP    DEFAULT NOW(),
    UNIQUE (weapon_id, boss_id, window_start)
);

-- ─── META SHIFTS (Detección de cambios de tendencia >15%) ─────────────────────
CREATE TABLE IF NOT EXISTS meta_shifts (
    id                  SERIAL       PRIMARY KEY,
    weapon_id           VARCHAR(50)  NOT NULL,
    boss_id             VARCHAR(50)  NOT NULL,
    shift_type          VARCHAR(20)  NOT NULL CHECK (shift_type IN ('emerging','declining','dominant')),
    previous_popularity DECIMAL(5,2) DEFAULT 0.0,
    current_popularity  DECIMAL(5,2) DEFAULT 0.0,
    change_percentage   DECIMAL(5,2) DEFAULT 0.0,
    detected_at         TIMESTAMP    DEFAULT NOW()
);

-- ─── ÍNDICES ──────────────────────────────────────────────────────────────────
-- Weapons
CREATE INDEX IF NOT EXISTS idx_weapons_category   ON weapons(category);
CREATE INDEX IF NOT EXISTS idx_weapons_weapon_type ON weapons(weapon_type);

-- Bosses
CREATE INDEX IF NOT EXISTS idx_bosses_location ON bosses(location);

-- Effectiveness
CREATE INDEX IF NOT EXISTS idx_eff_boss_id   ON weapon_boss_effectiveness(boss_id);
CREATE INDEX IF NOT EXISTS idx_eff_weapon_id ON weapon_boss_effectiveness(weapon_id);
CREATE INDEX IF NOT EXISTS idx_eff_score     ON weapon_boss_effectiveness(effectiveness_score DESC);

-- Player events
CREATE INDEX IF NOT EXISTS idx_events_player_id  ON player_events(player_id);
CREATE INDEX IF NOT EXISTS idx_events_weapon_id  ON player_events(weapon_id);
CREATE INDEX IF NOT EXISTS idx_events_boss_id    ON player_events(boss_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp  ON player_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON player_events(event_type);

-- Real-time rankings
CREATE INDEX IF NOT EXISTS idx_rankings_boss_id    ON real_time_rankings(boss_id);
CREATE INDEX IF NOT EXISTS idx_rankings_weapon_id  ON real_time_rankings(weapon_id);
CREATE INDEX IF NOT EXISTS idx_rankings_win_rate   ON real_time_rankings(win_rate DESC);
CREATE INDEX IF NOT EXISTS idx_rankings_updated_at ON real_time_rankings(updated_at DESC);

-- Meta shifts
CREATE INDEX IF NOT EXISTS idx_shifts_detected_at ON meta_shifts(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_shifts_shift_type  ON meta_shifts(shift_type);
CREATE INDEX IF NOT EXISTS idx_shifts_weapon_id   ON meta_shifts(weapon_id);

-- ─── VISTAS ANALÍTICAS ────────────────────────────────────────────────────────

-- Vista: Top 10 armas por efectividad promedio (resultado del Batch)
CREATE OR REPLACE VIEW v_top_weapons_batch AS
SELECT
    w.weapon_id,
    w.name          AS weapon_name,
    w.category,
    ROUND(AVG(e.effectiveness_score)::numeric, 2) AS avg_effectiveness,
    MAX(e.effectiveness_score)                     AS max_effectiveness,
    COUNT(e.boss_id)                               AS bosses_covered
FROM weapons w
JOIN weapon_boss_effectiveness e ON w.weapon_id = e.weapon_id
GROUP BY w.weapon_id, w.name, w.category
ORDER BY avg_effectiveness DESC;

-- Vista: Comparativa batch vs stream por arma
CREATE OR REPLACE VIEW v_batch_vs_stream AS
SELECT
    w.name          AS weapon_name,
    b.name          AS boss_name,
    e.effectiveness_score AS theoretical_score,
    r.win_rate            AS actual_win_rate,
    ROUND((r.win_rate - e.effectiveness_score)::numeric, 2) AS delta,
    r.total_attempts,
    r.updated_at    AS last_stream_update
FROM weapon_boss_effectiveness e
JOIN weapons w ON e.weapon_id = w.weapon_id
JOIN bosses  b ON e.boss_id   = b.boss_id
LEFT JOIN (
    SELECT DISTINCT ON (weapon_id, boss_id) *
    FROM real_time_rankings
    ORDER BY weapon_id, boss_id, updated_at DESC
) r ON r.weapon_id = e.weapon_id AND r.boss_id = e.boss_id
ORDER BY ABS(COALESCE(r.win_rate, 0) - e.effectiveness_score) DESC;

-- Vista: Actividad reciente del simulador (últimos 5 minutos)
CREATE OR REPLACE VIEW v_recent_activity AS
SELECT
    event_type,
    weapon_id,
    boss_id,
    COUNT(*)                                                      AS event_count,
    COUNT(*) FILTER (WHERE event_type = 'victory')               AS victories,
    ROUND(
        COUNT(*) FILTER (WHERE event_type = 'victory') * 100.0
        / NULLIF(COUNT(*), 0), 1
    )                                                             AS win_rate_pct
FROM player_events
WHERE timestamp >= NOW() - INTERVAL '5 minutes'
GROUP BY event_type, weapon_id, boss_id
ORDER BY event_count DESC;
