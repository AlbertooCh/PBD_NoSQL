from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
import json
import time
import random
# IMPORTAMOS EL DATASET EN LUGAR DE DEFINIRLO AQUÍ
try:
    from datasets.dataset import sectores, poblacion
except ImportError:
    # Fallback por si no existe el archivo aún, para que no falle la app entera
    sectores = []
    poblacion = []
    print("⚠️ ADVERTENCIA: No se encontró 'dataset.py'. La carga de datos estará vacía.")

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


# --- CLASE PARA LOGS ---
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


# --- FUNCIONES AUXILIARES (RIAK) ---
def check_connection():
    try:
        resp = requests.get(f"{RIAK_HOST}/ping", timeout=1)
        return resp.status_code == 200
    except:
        return False


def get_keys(bucket):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys?keys=true"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json().get('keys', [])
    return []


def get_object(bucket, key):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    return None


def store_object_with_indexes(bucket, key, data, indexes=None):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    headers = HEADERS_JSON.copy()
    if indexes:
        for idx_name, idx_value in indexes.items():
            header_name = f"x-riak-index-{idx_name}"
            headers[header_name] = str(idx_value)
    resp = requests.put(url, data=json.dumps(data), headers=headers)
    return resp.status_code in [200, 204]


def delete_object(bucket, key):
    """ Equivalente a bucket.delete(key) """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    resp = requests.delete(url)
    return resp.status_code in [204, 404]


def query_2i_exact(bucket, index_name, value):
    url = f"{RIAK_HOST}/buckets/{bucket}/index/{index_name}/{value}"
    resp = requests.get(url)
    return resp.json().get('keys', []) if resp.status_code == 200 else []


def query_2i_range(bucket, index_name, min_val, max_val):
    url = f"{RIAK_HOST}/buckets/{bucket}/index/{index_name}/{min_val}/{max_val}"
    resp = requests.get(url)
    return resp.json().get('keys', []) if resp.status_code == 200 else []


# --- ENDPOINTS ---

@app.get("/")
def serve_ui():
    return FileResponse("index.html")


@app.get("/load-data")
def load_data():
    tracer = Tracer("DATA_LOADER")
    if not check_connection():
        tracer.log("Error conectando a Riak", "error")
        return {"trace": tracer.logs, "msg": "Error"}

    tracer.log("Iniciando carga del dataset completo desde dataset.py...", "info")

    if not sectores and not poblacion:
        tracer.log("No hay datos cargados (listas vacías).", "warn")
        return {"trace": tracer.logs, "msg": "No hay datos"}

    # Cargar Sectores
    for s in sectores:
        store_object_with_indexes("sectores", str(s['codS']), s)

    # Cargar Población con Índices
    # Reutilizamos store_object_with_indexes para que los índices se creen correctamente
    for p in poblacion:
        idxs = {
            'ingresos_int': p['ingresos'],
            'sector_int': p['sector'],
            'sexo_bin': p['sexo']
        }
        store_object_with_indexes("poblacion", p['dni'], p, idxs)

    tracer.log(f"Datos insertados: {len(poblacion)} personas y {len(sectores)} sectores.", "success")
    return {"trace": tracer.logs, "msg": "Datos cargados exitosamente"}


@app.get("/execute/{operation}/{db}")
def execute_operation(
        operation: str,
        db: str,
        # Parámetros opcionales para todas las operaciones
        dni: str = Query("555888999"),
        nombre: str = Query("Laura"),
        ingresos: int = Query(30000),
        min_val: int = Query(20000),
        max_val: int = Query(50000),
        sector: int = Query(2),
        sexo: str = Query("M")
):
    tracer = Tracer(db)
    result_data = {}

    # ==========================================
    # 1. LISTAR TODO (SCAN)
    # ==========================================
    if operation == "listar":
        tracer.log("Listando claves del bucket 'poblacion'...", "info")
        if db == "riak":
            keys = get_keys('poblacion')
            tracer.log(f"Se encontraron {len(keys)} claves.", "info")
            # Limitamos a 5 para no saturar la UI
            limit_keys = keys[:5]
            data = []
            for k in limit_keys:
                data.append(get_object('poblacion', k))
            result_data = data
        elif db == "mongo":
            time.sleep(0.02)
            tracer.log("db.poblacion.find({})", "warn")
            result_data = [{"dni": "...", "nombre": "..."}]
        elif db == "redis":
            time.sleep(0.01)
            tracer.log("SCAN 0 MATCH poblacion:*", "warn")

    # ==========================================
    # 2. LEER UNO (GET BY ID)
    # ==========================================
    elif operation == "leer":
        tracer.log(f"Buscando DNI: {dni}", "info")
        if db == "riak":
            obj = get_object('poblacion', dni)
            if obj:
                tracer.log("Objeto encontrado.", "success")
                result_data = obj
            else:
                tracer.log("Objeto no encontrado (404).", "error")
        elif db == "mongo":
            time.sleep(0.01)
            tracer.log(f"db.poblacion.findOne({{dni: '{dni}'}})", "warn")
        elif db == "redis":
            time.sleep(0.005)
            tracer.log(f"HGETALL poblacion:{dni}", "warn")

    # ==========================================
    # 3. INSERTAR (PUT)
    # ==========================================
    elif operation == "insertar":
        nuevo_p = {"dni": dni, "nombre": nombre, "ingresos": ingresos, "sector": sector, "sexo": sexo}
        tracer.log(f"Insertando: {nuevo_p}", "info")

        if db == "riak":
            # Indices para que luego funcionen las búsquedas
            idxs = {'ingresos_int': ingresos, 'sector_int': sector, 'sexo_bin': sexo}
            store_object_with_indexes('poblacion', dni, nuevo_p, idxs)
            tracer.log(f"Objeto {dni} guardado con éxito.", "success")
            result_data = nuevo_p
        elif db == "mongo":
            time.sleep(0.02)
            tracer.log(f"db.poblacion.insertOne({nuevo_p})", "warn")
        elif db == "redis":
            time.sleep(0.01)
            tracer.log(f"HSET poblacion:{dni} ...", "warn")

    # ==========================================
    # 4. ACTUALIZAR (GET -> MODIFY -> PUT)
    # ==========================================
    elif operation == "actualizar":
        nuevo_ingreso = ingresos + 5000  # Simulación de cambio
        tracer.log(f"Actualizando ingresos de {dni} a {nuevo_ingreso}", "info")

        if db == "riak":
            current = get_object('poblacion', dni)
            if current:
                current['ingresos'] = nuevo_ingreso
                # IMPORTANTE: En Riak se sobrescribe todo, hay que pasar indices de nuevo
                idxs = {'ingresos_int': nuevo_ingreso, 'sector_int': current.get('sector', 2),
                        'sexo_bin': current.get('sexo', 'M')}
                store_object_with_indexes('poblacion', dni, current, idxs)
                tracer.log("Actualización completada (Read -> Modify -> Write)", "success")
                result_data = current
            else:
                tracer.log("No se puede actualizar: El DNI no existe", "error")
        elif db == "mongo":
            time.sleep(0.02)
            tracer.log(f"db.poblacion.updateOne({{dni:'{dni}'}}, {{$set: {{ingresos: {nuevo_ingreso}}}}})", "warn")
            tracer.log("Mongo actualiza 'in-place' (atómico)", "success")
        elif db == "redis":
            time.sleep(0.01)
            tracer.log(f"HSET poblacion:{dni} ingresos {nuevo_ingreso}", "warn")

    # ==========================================
    # 5. BORRAR (DELETE)
    # ==========================================
    elif operation == "borrar":
        tracer.log(f"Eliminando DNI: {dni}", "warn")
        if db == "riak":
            if delete_object('poblacion', dni):
                tracer.log(f"Clave {dni} eliminada correctamente.", "success")
            else:
                tracer.log("Error al eliminar (o no existía).", "error")
        elif db == "mongo":
            time.sleep(0.02)
            tracer.log(f"db.poblacion.deleteOne({{dni: '{dni}'}})", "warn")
        elif db == "redis":
            time.sleep(0.01)
            tracer.log(f"DEL poblacion:{dni}", "warn")

    # ==========================================
    # OPERACIONES AVANZADAS EXISTENTES
    # ==========================================
    elif operation == "indexar":
        if db == "riak":
            tracer.log("Obteniendo claves del bucket 'poblacion'...", "info")
            keys = get_keys('poblacion')
            count = 0
            for k in keys:
                data = get_object('poblacion', k)
                if data:
                    mis_indices = {
                        'ingresos_int': data.get('ingresos', 0),
                        'sector_int': data.get('sector', 1),
                        'sexo_bin': data.get('sexo', 'M')
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

    elif operation == "rango":
        tracer.log(f"Buscando ingresos entre {min_val} y {max_val}", "info")

        if db == "riak":
            keys = query_2i_range('poblacion', 'ingresos_int', min_val, max_val)
            tracer.log(f"Riak devolvió {len(keys)} claves por índice.", "info")
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

    elif operation == "filtro":
        tracer.log(f"Filtro: Sector {sector} y Sexo {sexo}", "info")

        if db == "riak":
            keys = query_2i_exact('poblacion', 'sector_int', sector)
            tracer.log(f"Índice 'sector_int'={sector} devolvió {len(keys)} claves.", "info")
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

    elif operation == "agregacion":
        if db == "riak":
            keys = get_keys('poblacion')
            total = 0
            for k in keys:
                obj = get_object('poblacion', k)
                if obj and str(obj['sector']) == str(sector):
                    total += obj.get('ingresos', 0)

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
