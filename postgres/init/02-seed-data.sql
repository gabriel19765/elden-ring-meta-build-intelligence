-- ============================================================================
-- SEED DATA — Elden Ring Meta-Build Intelligence
-- 10 armas × 8 jefes = 80 pares de efectividad con datos reales del juego
-- ============================================================================

-- ─── ARMAS ──────────────────────────────────────────────────────────────────
INSERT INTO weapons (weapon_id, name, category, weapon_type,
    damage_physical, damage_magic, damage_fire, damage_lightning, damage_holy,
    weight, requirements_str, requirements_dex, requirements_int,
    scaling_str, scaling_dex, scaling_int, scaling_fai, scaling_arc, source)
VALUES
-- Katanas (meta del juego, alto bleed)
('w001', 'Moonveil',                'Katana',           'Melee',  73,  87,   0,  0,  0,  6.5, 12, 18, 23, 'E', 'C', 'S', '',  '',  'seed'),
('w002', 'Rivers of Blood',         'Katana',           'Melee',  76,   0,  76,  0,  0,  6.5, 12, 18,  0, 'E', 'D', '',  '',  'E',  'seed'),
-- Espadas a dos manos (Faith/STR builds)
('w003', 'Blasphemous Blade',       'Greatsword',       'Melee', 121,   0,  78,  0,  0, 13.5, 22, 15,  0, 'D', 'D', '',  'D', '',   'seed'),
('w004', 'Starscourge Greatsword',  'Colossal Sword',   'Melee', 129,   0,  83,  0,  0, 20.0, 38, 12,  0, 'C', 'E', '',  '',  '',   'seed'),
-- Magia
('w005', 'Dark Moon Greatsword',    'Greatsword',       'Melee',  82,  98,   0,  0,  0, 10.0, 16, 11, 38, 'D', 'D', 'S', '',  '',   'seed'),
('w007', 'Meteorite Staff',         'Glintstone Staff', 'Magic',  60,   0,   0,  0,  0,  4.5,  6, 18, 18, 'E', 'S', '',  '',  '',   'seed'),
('w008', 'Carian Regal Scepter',    'Glintstone Staff', 'Magic',  74,   0,   0,  0,  0,  3.0,  8, 10, 60, 'E', 'B', '',  '',  '',   'seed'),
-- Otras builds
('w006', 'Bloodhound''s Fang',      'Curved Greatsword','Melee', 141,   0,   0,  0,  0, 11.5, 18, 17,  0, 'D', 'C', '',  '',  '',   'seed'),
('w009', 'Reduvia',                 'Dagger',           'Melee',  79,   0,   0,  0,  0,  2.5,  5, 13,  0, 'E', 'C', '',  '',  'D',  'seed'),
('w010', 'Morgott''s Cursed Sword', 'Curved Greatsword','Melee', 112,   0,   0,  0, 88,  9.0, 14, 35,  0, 'D', 'D', '',  'D', '',   'seed'),
-- Armas Legendarias adicionales
('w011', 'Grafted Blade Greatsword',  'Colossal Sword',   'Melee', 162,   0,   0,  0,  0, 21.0, 40, 14,  0, 'C', 'E', '',  '',  '',   'seed'),
('w012', 'Sacred Relic Sword',         'Greatsword',       'Melee',  85,   0,   0,  0, 77, 11.0, 14, 24,  0, 'D', 'D', '',  'D', '',   'seed'),
('w013', 'Hand of Malenia',             'Katana',           'Melee', 117,   0,   0,  0,  0,  9.5, 16, 48,  0, 'E', 'B', '',  '',  '',   'seed'),
('w014', 'Winged Scythe',               'Reaper',           'Melee',  87,   0,   0,  0, 104, 7.5, 16, 16,  0, 'E', 'D', '',  'D', '',   'seed'),
('w015', 'Bolt of Gransax',             'Spear',            'Melee',  98,   0,   0, 98,  0,  8.5, 20, 40,  0, 'D', 'C', '',  '',  '',   'seed' )
ON CONFLICT (weapon_id) DO NOTHING;

-- ─── JEFES ──────────────────────────────────────────────────────────────────
INSERT INTO bosses (boss_id, name, location,
    health_points, defense_physical, defense_magic, defense_fire,
    weaknesses, resistances, source)
VALUES
('b001', 'Margit, the Fell Omen',           'Stormveil Castle',    4174,  110, 110, 110,
    ARRAY['holy', 'lightning'], ARRAY['magic', 'fire'], 'seed'),
('b002', 'Godrick the Grafted',             'Stormveil Castle',    6080,  115, 115,  80,
    ARRAY['fire', 'lightning'], ARRAY['magic'], 'seed'),
('b003', 'Rennala, Queen of the Full Moon', 'Raya Lucaria',        7000,  105, 170, 105,
    ARRAY['physical', 'lightning'], ARRAY['magic'], 'seed'),
('b004', 'Starscourge Radahn',             'Caelid',               9572,  120, 120, 120,
    ARRAY['holy', 'scarlet_rot'], ARRAY['magic', 'fire'], 'seed'),
('b005', 'Morgott, the Omen King',          'Leyndell',           10399,  125, 125, 125,
    ARRAY['holy', 'lightning'], ARRAY['magic'], 'seed'),
('b006', 'Fire Giant',                      'Mountaintops of the Giants', 43263, 130, 130, 80,
    ARRAY['fire', 'lightning'], ARRAY['magic'], 'seed'),
('b007', 'Malenia, Blade of Miquella',      'Elphael, Brace of the Haligtree', 33251, 135, 135, 135,
    ARRAY['fire', 'lightning'], ARRAY['magic', 'holy'], 'seed'),
('b008', 'Radagon of the Golden Order',     'Elden Throne',       18907,  140, 140, 140,
    ARRAY['holy'], ARRAY['magic', 'fire'], 'seed')
ON CONFLICT (boss_id) DO NOTHING;

-- ─── EFECTIVIDAD (10 × 8 = 80 pares completos) ─────────────────────────────
-- Fórmula: score = Σ(damage_tipo / (defense_tipo + 1) * 10), capped a [0, 100]
-- Ajustado manualmente para reflejar el metagame real del juego

INSERT INTO weapon_boss_effectiveness (weapon_id, boss_id, effectiveness_score) VALUES
-- Moonveil (w001): katana mágica, buena vs débiles a magia
('w001','b001', 85.5), ('w001','b002', 79.0), ('w001','b003', 62.0), ('w001','b004', 77.0),
('w001','b005', 80.0), ('w001','b006', 72.5), ('w001','b007', 74.0), ('w001','b008', 78.5),
-- Rivers of Blood (w002): bleed + fuego, meta general
('w002','b001', 78.2), ('w002','b002', 82.0), ('w002','b003', 70.0), ('w002','b004', 68.0),
('w002','b005', 73.0), ('w002','b006', 65.0), ('w002','b007', 91.5), ('w002','b008', 71.0),
-- Blasphemous Blade (w003): faith + fuego
('w003','b001', 92.1), ('w003','b002', 89.5), ('w003','b003', 76.0), ('w003','b004', 70.0),
('w003','b005', 87.0), ('w003','b006', 75.0), ('w003','b007', 69.0), ('w003','b008', 72.0),
-- Starscourge Greatsword (w004): colosal + fuego, anti-Radahn
('w004','b001', 74.0), ('w004','b002', 80.0), ('w004','b003', 65.0), ('w004','b004', 90.0),
('w004','b005', 76.0), ('w004','b006', 83.0), ('w004','b007', 63.0), ('w004','b008', 68.0),
-- Dark Moon Greatsword (w005): magia pesada
('w005','b001', 78.0), ('w005','b002', 74.0), ('w005','b003', 95.0), ('w005','b004', 66.0),
('w005','b005', 72.0), ('w005','b006', 70.0), ('w005','b007', 68.0), ('w005','b008', 64.0),
-- Bloodhound's Fang (w006): bleed físico
('w006','b001', 79.0), ('w006','b002', 77.0), ('w006','b003', 72.0), ('w006','b004', 75.0),
('w006','b005', 74.0), ('w006','b006', 69.0), ('w006','b007', 82.3), ('w006','b008', 70.0),
-- Meteorite Staff (w007): INT scaling S, sorcery enabler
('w007','b001', 70.0), ('w007','b002', 68.0), ('w007','b003', 88.5), ('w007','b004', 60.0),
('w007','b005', 65.0), ('w007','b006', 62.0), ('w007','b007', 61.0), ('w007','b008', 59.0),
-- Carian Regal Scepter (w008): mejor staff end-game
('w008','b001', 72.0), ('w008','b002', 70.0), ('w008','b003', 92.0), ('w008','b004', 62.0),
('w008','b005', 68.0), ('w008','b006', 64.0), ('w008','b007', 63.0), ('w008','b008', 61.0),
-- Reduvia (w009): dagger rápida + bleed arc
('w009','b001', 67.0), ('w009','b002', 65.0), ('w009','b003', 60.0), ('w009','b004', 62.0),
('w009','b005', 64.0), ('w009','b006', 58.0), ('w009','b007', 73.0), ('w009','b008', 60.0),
-- Morgott's Cursed Sword (w010): holy + curved GS
('w010','b001', 76.0), ('w010','b002', 74.0), ('w010','b003', 70.0), ('w010','b004', 68.0),
('w010','b005', 79.0), ('w010','b006', 66.0), ('w010','b007', 72.0), ('w010','b008', 84.0),
-- Grafted Blade Greatsword (w011): colossal STR powerhouse
('w011','b001', 77.0), ('w011','b002', 85.0), ('w011','b003', 68.0), ('w011','b004', 80.0),
('w011','b005', 78.0), ('w011','b006', 91.0), ('w011','b007', 62.0), ('w011','b008', 70.0),
-- Sacred Relic Sword (w012): holy AoE wave
('w012','b001', 82.5), ('w012','b002', 80.0), ('w012','b003', 75.0), ('w012','b004', 78.0),
('w012','b005', 84.0), ('w012','b006', 70.0), ('w012','b007', 75.0), ('w012','b008', 92.5),
-- Hand of Malenia (w013): dex waterfowldance
('w013','b001', 80.0), ('w013','b002', 83.5), ('w013','b003', 74.0), ('w013','b004', 72.0),
('w013','b005', 76.0), ('w013','b006', 78.0), ('w013','b007', 60.0), ('w013','b008', 68.0),
-- Winged Scythe (w014): holy reaper, fast attacks
('w014','b001', 74.5), ('w014','b002', 72.0), ('w014','b003', 70.0), ('w014','b004', 71.0),
('w014','b005', 79.5), ('w014','b006', 64.0), ('w014','b007', 73.0), ('w014','b008', 86.0),
-- Bolt of Gransax (w015): lightning sniper spear
('w015','b001', 88.0), ('w015','b002', 92.0), ('w015','b003', 80.0), ('w015','b004', 85.0),
('w015','b005', 86.0), ('w015','b006', 82.0), ('w015','b007', 80.0), ('w015','b008', 75.0)
ON CONFLICT (weapon_id, boss_id) DO NOTHING;
