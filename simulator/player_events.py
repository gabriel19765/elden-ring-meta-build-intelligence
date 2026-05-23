"""
Simulador de eventos de jugadores de Elden Ring
Genera eventos realistas: muertes, victorias, cambios de arma
"""

import json
import random
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SIMULATION_SPEED = float(os.getenv('SIMULATION_SPEED', '1.0'))

# Datos de ejemplo
WEAPONS = [
    'w001', 'w002', 'w003', 'w004', 'w005', 
    'w006', 'w007', 'w008', 'w009', 'w010',
    'w011', 'w012', 'w013', 'w014', 'w015'
]

BOSSES = [
    'b001', 'b002', 'b003', 'b004', 'b005', 
    'b006', 'b007', 'b008'
]

PLAYER_IDS = [f"player_{i:04d}" for i in range(1, 101)]

# Meta actual (cambiará con el tiempo)
CURRENT_META = {
    'b001': 'w003',  # Blasphemous vs Margit
    'b003': 'w005',  # Dark Moon vs Rennala
    'b004': 'w004',  # Starscourge vs Radahn
    'b007': 'w002',  # Rivers vs Malenia
}

# Probabilidades base
BASE_WIN_RATE = 0.35
META_BONUS = 0.25  # Arma meta tiene +25% win rate

class EldenRingSimulator:
    def __init__(self):
        self.producer = None
        self.event_count = 0
        self.connect_kafka()
    
    def connect_kafka(self):
        max_retries = 30
        for i in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda v: v.encode('utf-8') if v else None
                )
                print(f"✅ Conectado a Kafka en {KAFKA_BOOTSTRAP}")
                return
            except NoBrokersAvailable:
                print(f"⏳ Esperando Kafka... ({i+1}/{max_retries})")
                time.sleep(2)
        raise Exception("No se pudo conectar a Kafka")
    
    def generate_event(self):
        """Genera un evento de jugador realista"""
        
        player_id = random.choice(PLAYER_IDS)
        boss_id = random.choice(BOSSES)
        event_type = random.choices(
            ['death', 'victory', 'equip_weapon', 'enter_boss'],
            weights=[40, 20, 15, 25]
        )[0]
        
        # Determinar arma usada
        if random.random() < 0.6:  # 60% usa el meta
            weapon_id = CURRENT_META.get(boss_id, random.choice(WEAPONS))
        else:
            weapon_id = random.choice(WEAPONS)
        
        # Calcular éxito basado en meta
        is_meta = (weapon_id == CURRENT_META.get(boss_id))
        win_probability = BASE_WIN_RATE + (META_BONUS if is_meta else 0)
        
        success = random.random() < win_probability
        
        event = {
            'event_id': f"evt_{datetime.now().strftime('%Y%m%d%H%M%S')}_{random.randint(1000, 9999)}",
            'player_id': player_id,
            'event_type': event_type,
            'weapon_id': weapon_id,
            'boss_id': boss_id,
            'location': self.get_boss_location(boss_id),
            'timestamp': datetime.now().isoformat(),
            'success': success if event_type in ['victory', 'death'] else None,
            'time_to_kill': round(random.uniform(30, 300), 2) if success else None,
            'is_meta_weapon': is_meta
        }
        
        return event
    
    def get_boss_location(self, boss_id):
        locations = {
            'b001': 'Stormveil Castle',
            'b002': 'Stormveil Castle',
            'b003': 'Raya Lucaria',
            'b004': 'Caelid',
            'b005': 'Leyndell',
            'b006': 'Mountaintops',
            'b007': 'Elphael',
            'b008': 'Leyndell'
        }
        return locations.get(boss_id, 'Unknown')
    
    def simulate_meta_shift(self):
        """Simula un cambio de meta cada cierto tiempo"""
        if self.event_count > 0 and self.event_count % 500 == 0:
            # Cambiar el meta aleatoriamente
            old_meta = dict(CURRENT_META)
            random_boss = random.choice(list(CURRENT_META.keys()))
            new_weapon = random.choice(WEAPONS)
            CURRENT_META[random_boss] = new_weapon
            
            print(f"\n🔄 META SHIFT DETECTADO!")
            print(f"   Boss {random_boss}: {old_meta[random_boss]} → {new_weapon}")
            print(f"   Timestamp: {datetime.now().isoformat()}\n")
    
    def run(self):
        print("=" * 50)
        print("ELDEN RING - PLAYER EVENT SIMULATOR")
        print("=" * 50)
        print(f"Speed: {SIMULATION_SPEED}x | Kafka: {KAFKA_BOOTSTRAP}")
        print("=" * 50)
        
        try:
            while True:
                event = self.generate_event()
                
                # Enviar a Kafka
                self.producer.send(
                    'player-events',
                    key=event['player_id'],
                    value=event
                )
                
                self.event_count += 1
                
                # Log cada 100 eventos
                if self.event_count % 100 == 0:
                    print(f"📊 Eventos generados: {self.event_count} | "
                          f"Último: {event['event_type']} - {event['weapon_id']} vs {event['boss_id']}")
                
                # Simular meta shift ocasionalmente
                self.simulate_meta_shift()
                
                # Delay entre eventos
                time.sleep(0.5 / SIMULATION_SPEED)
                
        except KeyboardInterrupt:
            print("\n⛔ Simulador detenido")
            self.producer.close()

if __name__ == "__main__":
    simulator = EldenRingSimulator()
    simulator.run()
