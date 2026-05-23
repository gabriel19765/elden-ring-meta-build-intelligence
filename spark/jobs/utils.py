"""
utils.py — Spark Batch Job helpers
"""


def scaling_to_multiplier(scaling: str) -> float:
    """Convierte el grade de escalado (S, A, B, C, D, E) a multiplicador numérico."""
    return {"S": 1.5, "A": 1.3, "B": 1.15, "C": 1.0, "D": 0.85, "E": 0.6}.get(
        scaling.strip().upper() if scaling else "", 0.0
    )


def clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    """Limita un valor entre lo y hi."""
    return max(lo, min(hi, value))


def normalize_weapon_id(raw_id: str) -> str:
    """Normaliza un ID de arma eliminando espacios y bajando a minúsculas."""
    return raw_id.strip().lower().replace(" ", "_") if raw_id else ""


def effective_damage(
    base: float, scaling_grade: str, attribute_level: int
) -> float:
    """Calcula daño efectivo con scaling.
    
    Args:
        base: Daño base del arma
        scaling_grade: Grade de escalado ('S', 'A', etc.)
        attribute_level: Nivel del atributo correspondiente del jugador

    Returns:
        Daño total estimado
    """
    mult = scaling_to_multiplier(scaling_grade)
    bonus = base * mult * (attribute_level / 60.0)
    return clamp(base + bonus, 0, 9999)
