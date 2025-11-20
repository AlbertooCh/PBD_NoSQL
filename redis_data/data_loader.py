import redis
import json

# --- CONFIGURACIÓN DE CONEXIÓN ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# --- DATASETS COMPLETOS ---
SECTORES_DATA = {
    1: {"nombre": "Agricultura y pesca", "ingresos_sum": 0.0, "porcentaje": 0.0},
    2: {"nombre": "Industria y energía", "ingresos_sum": 0.0, "porcentaje": 0.0},
    3: {"nombre": "Servicios", "ingresos_sum": 0.0, "porcentaje": 0.0},
    4: {"nombre": "Construcción", "ingresos_sum": 0.0, "porcentaje": 0.0},
}

POBLACION_DATA = [
    {"_id": "123456789", "nombre": "Carlos", "apellidos": ["Gutiérrez", "Pérez"], "fechanac": "1997-11-15",
     "ingresos": 15000.00, "sector_id": 1, "gastos": {"fijos": 4000, "alim": 4000, "ropa": 3000}},
    {"_id": "777888999", "nombre": "Gerardo", "apellidos": ["Martín", "Duque"], "fechanac": "1961-12-12",
     "ingresos": 23000.00, "sector_id": 1, "gastos": {"fijos": 6000, "alim": 5000, "ropa": 4000}},
    {"_id": "222333444", "nombre": "Pedro", "apellidos": ["Sánchez", "González"], "fechanac": "1960-02-01",
     "ingresos": 22000.00, "sector_id": 1, "gastos": {"fijos": 5000, "alim": 3000, "ropa": 2000}},
    {"_id": "333444555", "nombre": "María", "apellidos": ["García", "Gil"], "fechanac": "1971-04-10",
     "ingresos": 19500.00, "sector_id": 1, "gastos": {"fijos": 3000, "alim": 3000, "ropa": 3000}},
    {"_id": "666884444", "nombre": "Ignacio", "apellidos": ["Costa", "Burgos"], "fechanac": "1982-12-12",
     "ingresos": 37000.00, "sector_id": 1, "gastos": {"fijos": 5000, "alim": 4500, "ropa": 2500}},

    {"_id": "555666777", "nombre": "Vicente", "apellidos": ["Marañón", "Fernández"], "fechanac": "1978-11-15",
     "ingresos": 46000.00, "sector_id": 2, "gastos": {"fijos": 8000, "alim": 5000, "ropa": 4000}},
    {"_id": "666777888", "nombre": "Beatriz", "apellidos": ["Losada", "Gijón"], "fechanac": "1974-04-12",
     "ingresos": 50000.00, "sector_id": 2, "gastos": {"fijos": 15000, "alim": 8000, "ropa": 5000}},
    {"_id": "888999000", "nombre": "Fernando", "apellidos": ["Huertas", "Martínez"], "fechanac": "1991-05-30",  # Corregido: "191" -> "1991"
     "ingresos": 70000.00, "sector_id": 2, "gastos": {"fijos": 20000, "alim": 8000, "ropa": 4500}},
    {"_id": "999000111", "nombre": "Francisco", "apellidos": ["Fernández", "Merchán"], "fechanac": "1979-03-12",
     "ingresos": 63000.00, "sector_id": 2, "gastos": {"fijos": 12000, "alim": 7500, "ropa": 4500}},
    {"_id": "666999333", "nombre": "Paula", "apellidos": ["Ordóñez", "Ruiz"], "fechanac": "1990-02-01",
     "ingresos": 25000.00, "sector_id": 2, "gastos": {"fijos": 10000, "alim": 3000, "ropa": 2000}},

    {"_id": "987654321", "nombre": "Eva", "apellidos": ["Moreno", "Pozo"], "fechanac": "1974-05-10",
     "ingresos": 40000.00, "sector_id": 3, "gastos": {"fijos": 9000, "alim": 6000, "ropa": 3000}},
    {"_id": "111000111", "nombre": "Antonio", "apellidos": ["Muñoz", "Hernández"], "fechanac": "1989-07-01",
     "ingresos": 25000.00, "sector_id": 3, "gastos": {"fijos": 6500, "alim": 3500, "ropa": 4000}},
    {"_id": "111000222", "nombre": "Sara", "apellidos": ["Gálvez", "Montes"], "fechanac": "1973-04-07",
     "ingresos": 40000.00, "sector_id": 3, "gastos": {"fijos": 11000, "alim": 9500, "ropa": 6500}},
    {"_id": "111000333", "nombre": "Cristina", "apellidos": ["Corral", "Palma"], "fechanac": "1976-05-12",
     "ingresos": 48000.00, "sector_id": 3, "gastos": {"fijos": 13000, "alim": 7800, "ropa": 5200}},
    {"_id": "111222333", "nombre": "Susana", "apellidos": ["Ruiz", "Méndez"], "fechanac": "1999-06-22",
     "ingresos": 18000.00, "sector_id": 3, "gastos": {"fijos": 5000, "alim": 4500, "ropa": 2500}},

    {"_id": "444555666", "nombre": "Ángel", "apellidos": ["Montero", "Salas"], "fechanac": "2000-04-07",
     "ingresos": 14000.00, "sector_id": 4, "gastos": {"fijos": 3000, "alim": 3000, "ropa": 3000}},
    {"_id": "888777666", "nombre": "Manuel", "apellidos": ["Vega", "Zarzal"], "fechanac": "1976-11-23",
     "ingresos": 36000.00, "sector_id": 4, "gastos": {"fijos": 12000, "alim": 6000, "ropa": 3000}},
    {"_id": "333445555", "nombre": "Margarita", "apellidos": ["Guillén", "Campos"], "fechanac": "1974-03-19",
     "ingresos": 50000.00, "sector_id": 4, "gastos": {"fijos": 12000, "alim": 7500, "ropa": 6500}},
    {"_id": "222447777", "nombre": "Fermín", "apellidos": ["Hoz", "Torres"], "fechanac": "1988-08-08",
     "ingresos": 25000.00, "sector_id": 4, "gastos": {"fijos": 4000, "alim": 3000, "ropa": 2500}},
]


def cargar_redis():
    """Carga datos de sectores y población en Redis."""

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # 1. Cargar sectores como hashes
    for codS, data in SECTORES_DATA.items():
        r.hset(f"sector:{codS}", mapping=data)

    # 2. Cargar población
    for persona in POBLACION_DATA:
        key = f"persona:{persona['_id']}"
        r.set(key, json.dumps(persona))

    print("Redis: Carga completa.")


if __name__ == "__main__":
    cargar_redis()