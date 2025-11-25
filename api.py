from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
import json
import time
import random
import sys

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURACIÓN ---
RIAK_HOST = 'http://localhost:8098'
HEADERS_JSON = {'Content-Type': 'application/json'}


# --- CLASE PARA LOGS EN EL FRONTEND ---
class Tracer:
    def __init__(self, db_name):
        self.logs = []
        self.db = db_name.upper()
        self.start_time = time.time()

    def log(self, msg, type="info"):
        emoji = {"info": "ℹ️", "success": "✅", "error": "❌", "warn": "⚠️", "timer": "⏱️"}.get(type, "🔹")
        self.logs.append(f"[{self.db}] {emoji} {msg}")

    def get_execution_time(self):
        return round((time.time() - self.start_time) * 1000, 2)


# --- FUNCIONES AUXILIARES (TU CÓDIGO) ---

def check_connection():
    try:
        resp = requests.get(f"{RIAK_HOST}/ping", timeout=1)
        return resp.status_code == 200
    except:
        return False


def get_keys(bucket):
    """ Obtiene todas las claves de un bucket """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys?keys=true"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json().get('keys', [])
    return []


def get_object(bucket, key):
    """ Recupera el JSON de un objeto """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    return None


def store_object_with_indexes(bucket, key, data, indexes=None):
    """
    Guarda el objeto y aplica índices secundarios vía Headers HTTP.
    """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    headers = HEADERS_JSON.copy()
    if indexes:
        for idx_name, idx_value in indexes.items():
            header_name = f"x-riak-index-{idx_name}"
            headers[header_name] = str(idx_value)

    resp = requests.put(url, data=json.dumps(data), headers=headers)
    return resp.status_code in [200, 204]


def query_2i_exact(bucket, index_name, value):
    """ Consulta exacta: /buckets/B/index/IDX/VAL """
    url = f"{RIAK_HOST}/buckets/{bucket}/index/{index_name}/{value}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json().get('keys', [])
    return []


def query_2i_range(bucket, index_name, min_val, max_val):
    """ Consulta de rango: /buckets/B/index/IDX/MIN/MAX """
    url = f"{RIAK_HOST}/buckets/{bucket}/index/{index_name}/{min_val}/{max_val}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json().get('keys', [])
    return []


# --- ENDPOINTS ---

@app.get("/")
def serve_ui():
    return FileResponse("index.html")


@app.get("/load-data")
def load_data():
    tracer = Tracer("DATA_LOADER")

    if not check_connection():
        tracer.log("No se puede conectar a Riak en localhost:8098", "error")
        return {"trace": tracer.logs, "msg": "Error de conexión"}

    tracer.log("Iniciando carga de datos...", "info")

    # Dataset completo
    sectores = [
        {"codS": 1, "nombreS": "Agricultura y pesca", "porcentS": 0, "ingresosS": 0},
        {"codS": 2, "nombreS": "Industria y energía", "porcentS": 0, "ingresosS": 0},
        {"codS": 3, "nombreS": "Servicios", "porcentS": 0, "ingresosS": 0},
        {"codS": 4, "nombreS": "Construcción", "porcentS": 0, "ingresosS": 0},
    ]

    poblacion = [
        {"dni": "123456789", "nombre": "Carlos", "apellido1": "Gutiérrez", "apellido2": "Pérez",
         "fechanac": "1997-11-15", "direccion": "Pz. Colón", "cp": "06300", "sexo": "H", "ingresos": 15000,
         "gastosFijos": 4000, "gastosAlim": 4000, "gastosRopa": 3000, "sector": 1},
        {"dni": "777888999", "nombre": "Gerardo", "apellido1": "Martín", "apellido2": "Duque", "fechanac": "1961-12-12",
         "direccion": "C. del Atín", "cp": "06002", "sexo": "H", "ingresos": 23000, "gastosFijos": 6000,
         "gastosAlim": 5000, "gastosRopa": 4000, "sector": 1},
        {"dni": "222333444", "nombre": "Pedro", "apellido1": "Sánchez", "apellido2": "González",
         "fechanac": "1960-02-01", "direccion": "C. Ancha", "cp": "06300", "sexo": "H", "ingresos": 22000,
         "gastosFijos": 5000, "gastosAlim": 3000, "gastosRopa": 2000, "sector": 1},
        {"dni": "333444555", "nombre": "María", "apellido1": "García", "apellido2": "Gil", "fechanac": "1971-04-10",
         "direccion": "C. Diagonal", "cp": "06400", "sexo": "M", "ingresos": 19500, "gastosFijos": 3000,
         "gastosAlim": 3000, "gastosRopa": 3000, "sector": 1},
        {"dni": "666884444", "nombre": "Ignacio", "apellido1": "Costa", "apellido2": "Burgos", "fechanac": "1982-12-12",
         "direccion": "C. Descubrimiento", "cp": "10005", "sexo": "H", "ingresos": 37000, "gastosFijos": 5000,
         "gastosAlim": 4500, "gastosRopa": 2500, "sector": 1},
        {"dni": "555666777", "nombre": "Vicente", "apellido1": "Marañán", "apellido2": "Fernández",
         "fechanac": "1978-11-15", "direccion": "Pz. América", "cp": "10600", "sexo": "H", "ingresos": 46000,
         "gastosFijos": 8000, "gastosAlim": 5000, "gastosRopa": 4000, "sector": 2},
        {"dni": "666777888", "nombre": "Beatriz", "apellido1": "Losada", "apellido2": "Gijón", "fechanac": "1974-04-12",
         "direccion": "Av. Principal", "cp": "06400", "sexo": "M", "ingresos": 50000, "gastosFijos": 15000,
         "gastosAlim": 8000, "gastosRopa": 5000, "sector": 2},
        {"dni": "888999000", "nombre": "Fernando", "apellido1": "Huertas", "apellido2": "Martínez",
         "fechanac": "1971-05-30", "direccion": "C. Mayor", "cp": "06002", "sexo": "H", "ingresos": 70000,
         "gastosFijos": 20000, "gastosAlim": 8000, "gastosRopa": 4500, "sector": 2},
        {"dni": "999000111", "nombre": "Francisco", "apellido1": "Fernández", "apellido2": "Merchán",
         "fechanac": "1979-03-12", "direccion": "C. Poniente", "cp": "10800", "sexo": "H", "ingresos": 63000,
         "gastosFijos": 12000, "gastosAlim": 7500, "gastosRopa": 4500, "sector": 2},
        {"dni": "666999333", "nombre": "Paula", "apellido1": "Ordóñez", "apellido2": "Ruiz", "fechanac": "1990-02-01",
         "direccion": "C. Atlántico", "cp": "06800", "sexo": "M", "ingresos": 25000, "gastosFijos": 10000,
         "gastosAlim": 3000, "gastosRopa": 2000, "sector": 2},
        {"dni": "987654321", "nombre": "Eva", "apellido1": "Moreno", "apellido2": "Pozo", "fechanac": "1974-05-10",
         "direccion": "C. Justicia", "cp": "10005", "sexo": "M", "ingresos": 40000, "gastosFijos": 9000,
         "gastosAlim": 6000, "gastosRopa": 3000, "sector": 3},
        {"dni": "111000111", "nombre": "Antonio", "apellido1": "Muñoz", "apellido2": "Hernández",
         "fechanac": "1989-07-01", "direccion": "C. Constitución", "cp": "06800", "sexo": "H", "ingresos": 25000,
         "gastosFijos": 6500, "gastosAlim": 3500, "gastosRopa": 4000, "sector": 3},
        {"dni": "111000222", "nombre": "Sara", "apellido1": "Gálvez", "apellido2": "Montes", "fechanac": "1973-04-07",
         "direccion": "C. Cádiz", "cp": "10300", "sexo": "M", "ingresos": 40000, "gastosFijos": 11000,
         "gastosAlim": 9500, "gastosRopa": 6500, "sector": 3},
        {"dni": "111000333", "nombre": "Cristina", "apellido1": "Corral", "apellido2": "Palma",
         "fechanac": "1976-05-12", "direccion": "C. Ermita", "cp": "10600", "sexo": "M", "ingresos": 48000,
         "gastosFijos": 13000, "gastosAlim": 7800, "gastosRopa": 5200, "sector": 3},
        {"dni": "111222333", "nombre": "Susana", "apellido1": "Ruiz", "apellido2": "Méndez", "fechanac": "1999-06-22",
         "direccion": "Av. Libertad", "cp": "10800", "sexo": "M", "ingresos": 18000, "gastosFijos": 5000,
         "gastosAlim": 4500, "gastosRopa": 2500, "sector": 3},
        {"dni": "444555666", "nombre": "Ángel", "apellido1": "Montero", "apellido2": "Salas", "fechanac": "2000-04-07",
         "direccion": "C. Tranquilidad", "cp": "10300", "sexo": "H", "ingresos": 14000, "gastosFijos": 3000,
         "gastosAlim": 3000, "gastosRopa": 3000, "sector": 4},
        {"dni": "888777666", "nombre": "Manuel", "apellido1": "Vega", "apellido2": "Zarzal", "fechanac": "1976-11-23",
         "direccion": "Pz. Azul", "cp": "10005", "sexo": "H", "ingresos": 36000, "gastosFijos": 12000,
         "gastosAlim": 6000, "gastosRopa": 3000, "sector": 4},
        {"dni": "333445555", "nombre": "Margarita", "apellido1": "Guillón", "apellido2": "Campos",
         "fechanac": "1974-03-19", "direccion": "Av. Héroes", "cp": "06800", "sexo": "M", "ingresos": 50000,
         "gastosFijos": 12000, "gastosAlim": 7500, "gastosRopa": 6500, "sector": 4},
        {"dni": "222447777", "nombre": "Fermín", "apellido1": "Hoz", "apellido2": "Torres", "fechanac": "1988-08-08",
         "direccion": "C. Curva", "cp": "06002", "sexo": "H", "ingresos": 25000, "gastosFijos": 4000,
         "gastosAlim": 3000, "gastosRopa": 2500, "sector": 4}
    ]

    # 1. Cargar Sectores
    for s in sectores:
        store_object_with_indexes("sectores", str(s['codS']), s)

    # 2. Cargar Población con Índices
    for p in poblacion:
        indices = {
            'ingresos_int': p['ingresos'],
            'sector_int': p['sector'],
            'sexo_bin': p['sexo']
        }
        store_object_with_indexes("poblacion", p['dni'], p, indices)

    tracer.log(f"Datos insertados: {len(poblacion)} personas y {len(sectores)} sectores.", "success")
    return {"trace": tracer.logs, "msg": "Datos cargados correctamente"}


@app.get("/execute/{operation}/{db}")
def execute_operation(
        operation: str,
        db: str,
        min_val: int = Query(20000),
        max_val: int = Query(50000),
        sector: int = Query(2),
        sexo: str = Query("M")
):
    tracer = Tracer(db)
    result_data = {}

    # ---------------------------------------------------------
    # OPERACIÓN: INDEXAR (Recalcular índices existentes)
    # ---------------------------------------------------------
    if operation == "indexar":
        if db == "riak":
            tracer.log("Obteniendo claves del bucket 'poblacion'...", "info")
            keys = get_keys('poblacion')
            count = 0

            for k in keys:
                data = get_object('poblacion', k)
                if data:
                    mis_indices = {
                        'ingresos_int': data['ingresos'],
                        'sector_int': data['sector'],
                        'sexo_bin': data['sexo']
                    }
                    store_object_with_indexes('poblacion', k, data, mis_indices)
                    count += 1
            tracer.log(f"Datos re-indexados: {count} registros actualizados.", "success")

        elif db == "mongo":
            time.sleep(0.05)
            tracer.log("db.poblacion.createIndex({ ingresos: 1 })", "warn")
            tracer.log("db.poblacion.createIndex({ sector: 1 })", "warn")
        elif db == "redis":
            time.sleep(0.02)
            tracer.log("FT.CREATE idx:poblacion ON HASH PREFIX 1 'poblacion:'...", "warn")

    # ---------------------------------------------------------
    # OPERACIÓN: RANGO
    # ---------------------------------------------------------
    elif operation == "rango":
        tracer.log(f"Buscando ingresos entre {min_val} y {max_val}", "info")

        if db == "riak":
            # 1. Obtener claves
            keys = query_2i_range('poblacion', 'ingresos_int', min_val, max_val)
            tracer.log(f"Riak devolvió {len(keys)} claves por índice.", "info")

            # 2. Obtener objetos
            data = []
            for k in keys:
                obj = get_object('poblacion', k)
                if obj: data.append(obj)
            result_data = data

        elif db == "mongo":
            time.sleep(random.uniform(0.02, 0.05))
            tracer.log(f"db.poblacion.find({{ ingresos: {{ $gte: {min_val}, $lte: {max_val} }} }})", "warn")
            result_data = [{"nombre": "Simulado Mongo", "ingresos": (min_val + max_val) // 2}]

        elif db == "redis":
            time.sleep(random.uniform(0.005, 0.015))
            tracer.log(f"FT.SEARCH idx:poblacion '@ingresos:[{min_val} {max_val}]'", "warn")
            result_data = [{"nombre": "Simulado Redis", "ingresos": (min_val + max_val) // 2}]

    # ---------------------------------------------------------
    # OPERACIÓN: FILTRO
    # ---------------------------------------------------------
    elif operation == "filtro":
        tracer.log(f"Filtro: Sector {sector} AND Sexo {sexo}", "info")

        if db == "riak":
            # 1. Filtro primario (Sector)
            keys = query_2i_exact('poblacion', 'sector_int', sector)
            tracer.log(f"Índice 'sector_int'={sector} devolvió {len(keys)} claves.", "info")

            # 2. Filtro secundario (App Side)
            found = []
            for k in keys:
                d = get_object('poblacion', k)
                if d and d['sexo'] == sexo:
                    found.append(d)
            result_data = found

        elif db == "mongo":
            time.sleep(random.uniform(0.02, 0.06))
            tracer.log(f"db.poblacion.find({{ sector: {sector}, sexo: '{sexo}' }})", "warn")

        elif db == "redis":
            time.sleep(random.uniform(0.005, 0.015))
            tracer.log(f"FT.SEARCH idx:poblacion '@sector:[{sector} {sector}] @sexo:{{{sexo}}}'", "warn")

    # ---------------------------------------------------------
    # OPERACIÓN: AGREGACIÓN
    # ---------------------------------------------------------
    elif operation == "agregacion":
        if db == "riak":
            keys = get_keys('poblacion')
            total = 0
            for k in keys:
                obj = get_object('poblacion', k)
                if obj and str(obj['sector']) == str(sector):
                    total += obj['ingresos']

            # Guardar resumen como en tu script
            resumen = {str(sector): total}
            store_object_with_indexes('resumenes', 'resumen_sector', resumen)
            tracer.log(f"Resumen guardado en bucket 'resumenes'.", "success")
            result_data = {"sector": sector, "total": total}

        elif db == "mongo":
            time.sleep(random.uniform(0.03, 0.08))
            tracer.log("Aggregation Pipeline ejecutado", "warn")

        elif db == "redis":
            time.sleep(random.uniform(0.01, 0.02))
            tracer.log("FT.AGGREGATE ejecutado en memoria", "warn")

    exec_time = tracer.get_execution_time()
    tracer.log(f"Tiempo total: {exec_time} ms", "timer")

    return {"trace": tracer.logs, "data": result_data, "time_ms": exec_time}