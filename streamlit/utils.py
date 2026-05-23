"""
Streamlit Utilities for Elden Ring Meta-Build Intelligence
"""
import streamlit as st

def add_combat_banner():
    st.markdown("""
    <div style="background: linear-gradient(90deg, #1a1a2e 0%, #16213e 100%); padding: 10px; border-radius: 8px; border-left: 4px solid #D4AF37; text-align: center;">
        <span style="color: #D4AF37; font-weight: bold;">⚠️ AVISO DEL CAMPO DE BATALLA:</span> 
        <span style="color: #E2E8F0;">¡Los boss fights están activos! Usa el recomendador para optimizar tu build.</span>
    </div>
    """, unsafe_allow_html=True)
