"""
Streamlit App: Elden Ring Meta-Build Intelligence
- Dashboard con métricas generales (Spark batch)
- Recomendador de builds por jefe (batch + stream)
- Rankings en tiempo real (Flink stream)
- Alertas de Meta-Shift
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

# ── Configuración de página ───────────────────────────────────────────────────
st.set_page_config(
    page_title="Elden Ring Meta-Build Intelligence",
    page_icon="⚔️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS Premium ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap');

html, body, [class*="css"] { font-family: 'Outfit', sans-serif; }

.main-header {
    font-size: 3rem; font-weight: 800;
    background: linear-gradient(135deg, #FFE082 0%, #D4AF37 50%, #8D6E63 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    text-align: center; margin-bottom: 0.2rem;
    filter: drop-shadow(0px 4px 8px rgba(0,0,0,0.5));
}
.sub-header {
    font-size: 1.2rem; color: #B0BEC5; text-align: center;
    margin-bottom: 2rem; font-weight: 300; letter-spacing: 1px;
}
.metric-container { display: flex; justify-content: space-around; gap: 1rem; margin-bottom: 2rem; }
.metric-card {
    flex: 1; background: linear-gradient(135deg, rgba(26,26,46,.9) 0%, rgba(22,33,62,.9) 100%);
    border: 1px solid rgba(255,255,255,.05); border-left: 5px solid #D4AF37;
    border-radius: 12px; padding: 1.25rem; box-shadow: 0 4px 20px rgba(0,0,0,.25);
    text-align: center;
}
.metric-value { font-size: 2.2rem; font-weight: 800; color: #FFFFFF; margin: 0.2rem 0; }
.metric-label { font-size: .9rem; color: #B0BEC5; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }
.premium-card {
    background: rgba(22,33,62,.7); backdrop-filter: blur(10px);
    border: 1px solid rgba(212,175,55,.25); border-radius: 16px; padding: 1.5rem;
    margin-bottom: 1rem; box-shadow: 0 8px 32px rgba(0,0,0,.37);
    transition: transform .3s ease, border .3s ease;
}
.premium-card:hover { transform: translateY(-4px); border: 1px solid rgba(212,175,55,.6); }
.badge-weak {
    background-color: rgba(76,175,80,.2); color: #81C784; border: 1px solid #4CAF50;
    padding: 4px 10px; border-radius: 20px; font-size: .85rem; margin-right: 5px;
    display: inline-block; font-weight: 600;
}
.badge-res {
    background-color: rgba(244,67,54,.2); color: #E57373; border: 1px solid #F44336;
    padding: 4px 10px; border-radius: 20px; font-size: .85rem; margin-right: 5px;
    display: inline-block; font-weight: 600;
}
</style>
""", unsafe_allow_html=True)


# ── Conexión PostgreSQL ───────────────────────────────────────────────────────
@st.cache_resource(ttl=30)
def _get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "elden_ring"),
        user=os.getenv("POSTGRES_USER", "elden"),
        password=os.getenv("POSTGRES_PASSWORD", "ring123"),
    )


def run_query(query: str, params=None) -> pd.DataFrame:
    try:
        conn = _get_connection()
        return pd.read_sql(query, conn, params=params)
    except Exception:
        # Conexión perdida → limpiar caché y reintentar una sola vez
        st.cache_resource.clear()
        try:
            conn = _get_connection()
            return pd.read_sql(query, conn, params=params)
        except Exception as e:
            st.error(f"Error de base de datos: {e}")
            return pd.DataFrame()


# ── Cabecera ─────────────────────────────────────────────────────────────────
st.markdown('<p class="main-header">⚔️ Elden Ring Meta-Build Intelligence</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Plataforma Big Data en Tiempo Real · Batch (Spark) + Stream (Flink) · PostgreSQL</p>', unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    # Streamlit >= 1.28: use_container_width reemplaza use_column_width
    st.image(
        "https://upload.wikimedia.org/wikipedia/en/b/b9/Elden_Ring_Box_art.jpg",
        use_container_width=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.title("Navegación del Reino")
    page = st.radio("", [
        "🏠 Dashboard Principal",
        "🔍 Recomendador de Builds",
        "📊 Rankings en Tiempo Real",
        "🚨 Alertas de Meta-Shift",
    ])
    st.divider()
    st.info(
        "🎓 **Proyecto Big Data Aplicado**\n\n"
        "Arquitectura end-to-end:\n"
        "- **Batch**: Apache Spark\n"
        "- **Stream**: Apache Flink + Kafka\n"
        "- **DB**: PostgreSQL\n"
        "- **Viz**: Grafana + Streamlit"
    )

# ═══════════════════════════════════════════════════════════════
# DASHBOARD PRINCIPAL
# ═══════════════════════════════════════════════════════════════
if page == "🏠 Dashboard Principal":
    st.header("📈 Estado General de las Tierras Intermedias")

    # Métricas
    def _count(table):
        df = run_query(f"SELECT COUNT(*) AS n FROM {table}")
        return int(df["n"].iloc[0]) if not df.empty else 0

    n_weapons = _count("weapons")
    n_bosses  = _count("bosses")
    n_events  = _count("player_events")
    n_shifts  = _count("meta_shifts")

    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-card">
            <div class="metric-label">Armas Analizadas</div>
            <div class="metric-value">{n_weapons}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Jefes Catalogados</div>
            <div class="metric-value">{n_bosses}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Eventos Procesados</div>
            <div class="metric-value">{n_events:,}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Meta-Shifts</div>
            <div class="metric-value">{n_shifts}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()
    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown('<div class="premium-card">', unsafe_allow_html=True)
        st.subheader("🏆 Top 10 Armas Más Efectivas (Cálculo Batch)")
        top_w = run_query("""
            SELECT w.name AS weapon,
                   ROUND(AVG(e.effectiveness_score)::numeric, 1) AS avg_score
            FROM weapons w
            JOIN weapon_boss_effectiveness e ON w.weapon_id = e.weapon_id
            GROUP BY w.name
            ORDER BY avg_score DESC
            LIMIT 10
        """)
        if not top_w.empty:
            fig = px.bar(
                top_w, x="avg_score", y="weapon", orientation="h",
                color="avg_score", color_continuous_scale="YlOrRd",
                labels={"avg_score": "Score Promedio", "weapon": "Arma"},
                template="plotly_dark",
            )
            fig.update_layout(
                yaxis={"categoryorder": "total ascending"},
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=10, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos de efectividad todavía. Espera al job de Spark.")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_r:
        st.markdown('<div class="premium-card">', unsafe_allow_html=True)
        st.subheader("👹 Dificultad de Jefes (HP vs Efectividad de Armas)")
        hard = run_query("""
            SELECT b.name AS boss,
                   ROUND(AVG(e.effectiveness_score)::numeric, 1) AS avg_effectiveness,
                   b.health_points
            FROM bosses b
            LEFT JOIN weapon_boss_effectiveness e ON b.boss_id = e.boss_id
            GROUP BY b.name, b.health_points
            ORDER BY avg_effectiveness ASC
        """)
        if not hard.empty:
            fig2 = px.scatter(
                hard, x="health_points", y="avg_effectiveness",
                text="boss", size="health_points", color="avg_effectiveness",
                color_continuous_scale="RdYlGn",
                labels={"health_points": "HP del Jefe", "avg_effectiveness": "Efectividad Media de Armas"},
                template="plotly_dark",
            )
            fig2.update_traces(textposition="top center")
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=10, b=10),
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Sin datos de jefes todavía.")
        st.markdown("</div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# RECOMENDADOR DE BUILDS
# ═══════════════════════════════════════════════════════════════
elif page == "🔍 Recomendador de Builds":
    st.header("🔍 Asistente de Combate Inteligente")

    bosses_df = run_query(
        "SELECT boss_id, name, location, health_points, "
        "defense_physical, defense_magic, defense_fire, weaknesses, resistances "
        "FROM bosses ORDER BY name"
    )

    if bosses_df.empty:
        st.warning("No hay datos de jefes en la base de datos.")
        st.stop()

    boss_map = dict(zip(bosses_df["name"], bosses_df["boss_id"]))
    selected_name = st.selectbox("Selecciona el enemigo a enfrentar:", list(boss_map.keys()))
    selected_id   = boss_map[selected_name]
    info          = bosses_df[bosses_df["boss_id"] == selected_id].iloc[0]

    st.divider()

    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    col_meta, col_radar = st.columns([1, 1])

    with col_meta:
        st.markdown(f"### 👿 {info['name']}")
        st.markdown(f"**📍 Ubicación:** {info['location']}")
        st.markdown(f"**❤️ HP:** {info['health_points']:,}")
        st.write("**🔥 Debilidades:**")
        for w in (info["weaknesses"] or []):
            st.markdown(f'<span class="badge-weak">{w}</span>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        st.write("**🛡️ Resistencias:**")
        for r in (info["resistances"] or []):
            st.markdown(f'<span class="badge-res">{r}</span>', unsafe_allow_html=True)

    with col_radar:
        st.markdown("### 📊 Perfil Defensivo")
        def_df = pd.DataFrame({
            "Atributo": ["Físico", "Magia", "Fuego"],
            "Defensa":  [info["defense_physical"], info["defense_magic"], info["defense_fire"]],
        })
        fig_r = px.line_polar(
            def_df, r="Defensa", theta="Atributo", line_close=True,
            template="plotly_dark", color_discrete_sequence=["#D4AF37"],
        )
        fig_r.update_traces(fill="toself", opacity=0.6)
        fig_r.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 150])),
            paper_bgcolor="rgba(0,0,0,0)", height=220,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_r, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    col_batch, col_stream = st.columns(2)

    with col_batch:
        st.markdown('<div class="premium-card">', unsafe_allow_html=True)
        st.subheader("📚 Recomendación Teórica (Spark Batch)")
        batch = run_query("""
            SELECT w.name, w.category,
                   ROUND(e.effectiveness_score::numeric, 1) AS score
            FROM weapon_boss_effectiveness e
            JOIN weapons w ON e.weapon_id = w.weapon_id
            WHERE e.boss_id = %s
            ORDER BY score DESC LIMIT 3
        """, (selected_id,))
        if not batch.empty:
            for _, row in batch.iterrows():
                st.markdown(f"""
                <div style="padding:10px;background:rgba(0,0,0,.2);border-radius:8px;
                            margin-bottom:10px;border-left:3px solid #D4AF37">
                    <span style="font-size:1.1rem;font-weight:600;color:#FFE082;">{row['name']}</span><br/>
                    <span style="font-size:.85rem;color:#B0BEC5;">{row['category']}</span> |
                    <span style="font-size:.9rem;font-weight:600;color:#4CAF50;">Score: {row['score']}/100</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("Sin datos de efectividad para este jefe.")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_stream:
        st.markdown('<div class="premium-card">', unsafe_allow_html=True)
        st.subheader("⚡ Rendimiento Real-Time (Flink Stream)")
        live = run_query("""
            SELECT w.name, r.win_rate, r.total_attempts, r.avg_time_to_kill
            FROM real_time_rankings r
            JOIN weapons w ON r.weapon_id = w.weapon_id
            WHERE r.boss_id = %s
            ORDER BY r.win_rate DESC LIMIT 3
        """, (selected_id,))
        if not live.empty:
            for _, row in live.iterrows():
                st.markdown(f"""
                <div style="padding:10px;background:rgba(0,0,0,.2);border-radius:8px;
                            margin-bottom:10px;border-left:3px solid #64B5F6">
                    <span style="font-size:1.1rem;font-weight:600;color:#90CAF9;">{row['name']}</span><br/>
                    <span style="font-size:.85rem;color:#B0BEC5;">
                        Intentos: {row['total_attempts']} | TTK: {row['avg_time_to_kill']}s
                    </span> |
                    <span style="font-size:.9rem;font-weight:600;color:#81C784;">
                        Win Rate: {row['win_rate']}%
                    </span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.warning("Sin datos en tiempo real para este jefe aún. El simulador necesita generar suficientes eventos.")
        st.markdown("</div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# RANKINGS EN TIEMPO REAL
# ═══════════════════════════════════════════════════════════════
elif page == "📊 Rankings en Tiempo Real":
    st.header("📊 Tabla de Clasificación en Tiempo Real")
    st.caption("Actualizada por Apache Flink a partir de las ventanas de 5 minutos sobre el topic Kafka player-events.")

    bosses_df = run_query("SELECT boss_id, name FROM bosses ORDER BY name")
    filter_opts = {"Todos los Jefes": "ALL"}
    if not bosses_df.empty:
        filter_opts.update(dict(zip(bosses_df["name"], bosses_df["boss_id"])))

    selected_filter = st.selectbox("Filtrar por jefe:", list(filter_opts.keys()))
    selected_val    = filter_opts[selected_filter]

    st.markdown('<div class="premium-card">', unsafe_allow_html=True)

    if selected_val == "ALL":
        q, params = ("""
            SELECT w.name AS "Arma", b.name AS "Jefe",
                   r.total_attempts AS "Intentos", r.total_victories AS "Victorias",
                   r.total_deaths AS "Muertes",
                   r.win_rate AS "Win Rate (%)", r.avg_time_to_kill AS "TTK (s)",
                   r.updated_at AS "Actualizado"
            FROM real_time_rankings r
            JOIN weapons w ON r.weapon_id = w.weapon_id
            JOIN bosses  b ON r.boss_id   = b.boss_id
            ORDER BY r.win_rate DESC, r.total_attempts DESC
            LIMIT 50
        """, None)
    else:
        q, params = ("""
            SELECT w.name AS "Arma", b.name AS "Jefe",
                   r.total_attempts AS "Intentos", r.total_victories AS "Victorias",
                   r.total_deaths AS "Muertes",
                   r.win_rate AS "Win Rate (%)", r.avg_time_to_kill AS "TTK (s)",
                   r.updated_at AS "Actualizado"
            FROM real_time_rankings r
            JOIN weapons w ON r.weapon_id = w.weapon_id
            JOIN bosses  b ON r.boss_id   = b.boss_id
            WHERE r.boss_id = %s
            ORDER BY r.win_rate DESC, r.total_attempts DESC
        """, (selected_val,))

    rank_df = run_query(q, params)

    if not rank_df.empty:
        if "Actualizado" in rank_df.columns:
            rank_df["Actualizado"] = pd.to_datetime(rank_df["Actualizado"]).dt.strftime("%H:%M:%S")
        st.dataframe(rank_df, use_container_width=True, hide_index=True)

        st.subheader("📈 Popularidad vs Efectividad Real")
        fig_sc = px.scatter(
            rank_df, x="Intentos", y="Win Rate (%)",
            size="Victorias", color="Arma",
            hover_name="Arma", text="Arma", size_max=30,
            template="plotly_dark",
        )
        fig_sc.update_traces(textposition="top center")
        fig_sc.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_sc, use_container_width=True)
    else:
        st.warning(
            "Sin datos en tiempo real todavía. "
            "El simulador necesita enviar eventos y Flink procesar al menos una ventana (5 min)."
        )
    st.markdown("</div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# ALERTAS DE META-SHIFT
# ═══════════════════════════════════════════════════════════════
elif page == "🚨 Alertas de Meta-Shift":
    st.header("🚨 Sistema de Detección de Cambios de Metagame")
    st.caption(
        "Flink detecta automáticamente variaciones >15% en el win rate entre ventanas consecutivas de 5 min "
        "y registra una alerta en PostgreSQL (y la envía a Discord si hay webhook configurado)."
    )

    shifts = run_query("""
        SELECT m.id, w.name AS weapon_name, b.name AS boss_name,
               m.shift_type, m.previous_popularity AS prev_wr,
               m.current_popularity AS curr_wr,
               m.change_percentage, m.detected_at
        FROM meta_shifts m
        JOIN weapons w ON m.weapon_id = w.weapon_id
        LEFT JOIN bosses b ON m.boss_id = b.boss_id
        ORDER BY m.detected_at DESC
        LIMIT 50
    """)

    if not shifts.empty:
        color_map = {
            "emerging":  ("border-left:5px solid #4CAF50;background:rgba(76,175,80,.1);", "📈 EMERGING", "#4CAF50"),
            "declining": ("border-left:5px solid #F44336;background:rgba(244,67,54,.1);", "📉 DECLINING", "#F44336"),
            "dominant":  ("border-left:5px solid #FF9800;background:rgba(255,152,0,.1);",  "👑 DOMINANT",  "#FF9800"),
        }

        for _, row in shifts.iterrows():
            stype = row["shift_type"]
            style, label, clr = color_map.get(stype, ("", stype.upper(), "#78909C"))
            ts = pd.to_datetime(row["detected_at"]).strftime("%d/%m/%Y %H:%M:%S")
            delta_sign = "+" if row["curr_wr"] > row["prev_wr"] else ""
            st.markdown(f"""
            <div style="padding:1.2rem;border-radius:12px;margin-bottom:12px;{style}
                        box-shadow:0 4px 15px rgba(0,0,0,.15);">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                    <div>
                        <span style="background:{clr};color:#fff;padding:3px 8px;
                                     border-radius:4px;font-weight:600;font-size:.8rem;">{label}</span>
                        <span style="font-weight:600;font-size:1.1rem;color:#FFF;margin-left:10px;">
                            {row['weapon_name']} vs {row['boss_name']}
                        </span>
                    </div>
                    <span style="font-size:.85rem;color:#B0BEC5;">{ts}</span>
                </div>
                <div style="margin-top:8px;font-size:.9rem;color:#B0BEC5;">
                    Win Rate anterior: <strong style="color:#FFF">{row['prev_wr']}%</strong> →
                    Win Rate actual: <strong style="color:#FFF">{row['curr_wr']}%</strong> |
                    Variación: <strong style="color:{clr}">{delta_sign}{row['change_percentage']}%</strong>
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.divider()
        st.subheader("📊 Historial de Fluctuaciones")
        fig_b = px.bar(
            shifts, x="weapon_name", y="change_percentage",
            color="shift_type", barmode="group",
            color_discrete_map={"emerging": "#4CAF50", "declining": "#F44336", "dominant": "#FF9800"},
            labels={"weapon_name": "Arma", "change_percentage": "Variación Win Rate (%)", "shift_type": "Tipo"},
            template="plotly_dark",
        )
        fig_b.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_b, use_container_width=True)
    else:
        st.markdown("""
        <div style="text-align:center;padding:3rem;background:rgba(22,33,62,.5);
                    border-radius:16px;border:1px dashed rgba(212,175,55,.3);">
            <h3 style="color:#D4AF37;">⚔️ Paz en el Reino</h3>
            <p style="color:#B0BEC5;margin-bottom:0;">No se han detectado Meta-Shifts todavía.</p>
            <p style="color:#78909C;font-size:.85rem;">
                Se disparan cuando el win rate cambia &gt;15% entre dos ventanas consecutivas de 5 minutos.
            </p>
        </div>
        """, unsafe_allow_html=True)
