from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
import json
import time
import random

# --- IMPORTACIONES REDIS ---
from redis import Redis
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.query import Query as RedisQuery
from redis.commands.search.index_definition import IndexDefinition, IndexType

# IMPORTAMOS EL DATASET
try:
    from datasets.dataset import sectores, poblacion
except ImportError:
    sectores = []
    poblacion = []
    print("⚠️ ADVERTENCIA: No se encontró 'dataset.py'.")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURACIÓN RIAK ---
RIAK_HOST = 'http://localhost:8098'
HEADERS_JSON = {'Content-Type': 'application/json'}

# --- CONFIGURACIÓN REDIS ---
# decode_responses=True para recibir strings en lugar de bytes
r = Redis(host='localhost', port=6379, decode_responses=True)
INDEX_NAME = "idx_poblacion"


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
def check_riak_connection():
    try:
        resp = requests.get(f"{RIAK_HOST}/ping", timeout=1)
        return resp.status_code == 200
    except:
        return False


def get_keys_riak(bucket):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys?keys=true"
    resp = requests.get(url)
    return resp.json().get('keys', []) if resp.status_code == 200 else []


def get_object_riak(bucket, key):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    resp = requests.get(url)
    return resp.json() if resp.status_code == 200 else None


def store_object_riak(bucket, key, data, indexes=None):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    headers = HEADERS_JSON.copy()
    if indexes:
        for idx_name, idx_value in indexes.items():
            headers[f"x-riak-index-{idx_name}"] = str(idx_value)
    resp = requests.put(url, data=json.dumps(data), headers=headers)
    return resp.status_code in [200, 204]


def delete_object_riak(bucket, key):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    resp = requests.delete(url)
    return resp.status_code in [204, 404]


def query_2i_riak(bucket, index_name, value):
    url = f"{RIAK_HOST}/buckets/{bucket}/index/{index_name}/{value}"
    resp = requests.get(url)
    return resp.json().get('keys', []) if resp.status_code == 200 else []


def query_2i_range_riak(bucket, index_name, min_val, max_val):
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

    if not sectores and not poblacion:
        return {"trace": ["No hay datos en dataset.py"], "msg": "Error"}

    # 1. CARGA RIAK
    if check_riak_connection():
        tracer.log("Cargando en Riak...", "info")
        for s in sectores:
            store_object_riak("sectores", str(s['codS']), s)
        for p in poblacion:
            idxs = {'ingresos_int': p['ingresos'], 'sector_int': p['sector'], 'sexo_bin': p['sexo']}
            store_object_riak("poblacion", p['dni'], p, idxs)
    else:
        tracer.log("Riak no disponible, saltando...", "error")

    # 2. CARGA REDIS
    tracer.log("Cargando en Redis (JSON + Index)...", "info")
    try:
        # Limpiar índice anterior si existe
        try:
            r.ft(INDEX_NAME).dropindex(delete_documents=True)
            tracer.log("Índice y documentos anteriores eliminados.", "warn")
        except:
            pass  # No existía

        # Crear Pipeline para inserción masiva (más rápido)
        pipe = r.pipeline()
        for s in sectores:
            pipe.json().set(f"sector:{s['codS']}", "$", s)
        for p in poblacion:
            pipe.json().set(f"poblacion:{p['dni']}", "$", p)
        pipe.execute()
        tracer.log(f"Insertados {len(poblacion)} registros JSON en Redis.", "success")

        # Crear Índice RediSearch
        schema = (
            TextField("$.nombre", as_name="nombre"),
            NumericField("$.ingresos", as_name="ingresos"),
            NumericField("$.sector", as_name="sector"),
            TextField("$.sexo", as_name="sexo")
        )
        r.ft(INDEX_NAME).create_index(
            schema,
            definition=IndexDefinition(prefix=["poblacion:"], index_type=IndexType.JSON)
        )
        tracer.log("Índice 'idx_poblacion' creado correctamente.", "success")

    except Exception as e:
        tracer.log(f"Error Redis: {str(e)}", "error")

    return {"trace": tracer.logs, "msg": "Carga finalizada"}


@app.get("/execute/{operation}/{db}")
def execute_operation(
        operation: str,
        db: str,
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
        tracer.log("Listando claves...", "info")
        if db == "riak":
            keys = get_keys_riak('poblacion')
            limit_keys = keys[:5]  # Limitamos
            result_data = [get_object_riak('poblacion', k) for k in limit_keys]
            tracer.log(f"Mostrando {len(result_data)} de {len(keys)} encontrados.", "info")

        elif db == "mongo":
            time.sleep(0.02)
            tracer.log("db.poblacion.find({})", "warn")
            result_data = [{"dni": "...", "nombre": "Simulado"}]

        elif db == "redis":
            # Usamos KEYS (en producción usar SCAN)
            keys = r.keys("poblacion:*")
            tracer.log(f"Encontradas {len(keys)} claves con patrón 'poblacion:*'", "info")
            limit_keys = keys[:5]
            # Obtenemos los objetos JSON completos
            result_data = [r.json().get(k) for k in limit_keys]
            tracer.log("r.json().get(key) ejecutado para los primeros 5.", "success")

    # ==========================================
    # 2. LEER UNO (GET BY ID)
    # ==========================================
    elif operation == "leer":
        tracer.log(f"Buscando DNI: {dni}", "info")
        if db == "riak":
            obj = get_object_riak('poblacion', dni)
            result_data = obj if obj else {"error": "Not found"}

        elif db == "mongo":
            time.sleep(0.01)
            tracer.log(f"db.poblacion.findOne({{dni: '{dni}'}})", "warn")

        elif db == "redis":
            key = f"poblacion:{dni}"
            # Redis JSON GET
            data = r.json().get(key)
            if data:
                tracer.log(f"r.json().get('{key}') -> Éxito", "success")
                result_data = data
            else:
                tracer.log("Clave no encontrada.", "error")

    # ==========================================
    # 3. INSERTAR (PUT)
    # ==========================================
    elif operation == "insertar":
        nuevo_p = {"dni": dni, "nombre": nombre, "ingresos": ingresos, "sector": sector, "sexo": sexo}
        tracer.log(f"Insertando: {nuevo_p}", "info")

        if db == "riak":
            idxs = {'ingresos_int': ingresos, 'sector_int': sector, 'sexo_bin': sexo}
            store_object_riak('poblacion', dni, nuevo_p, idxs)
            tracer.log("Guardado en Riak.", "success")
            result_data = nuevo_p

        elif db == "mongo":
            time.sleep(0.02)
            tracer.log("db.poblacion.insertOne(...)", "warn")

        elif db == "redis":
            key = f"poblacion:{dni}"
            # Redis JSON SET
            r.json().set(key, "$", nuevo_p)
            tracer.log(f"r.json().set('{key}', '$', data)", "success")
            # Simular evento Pub/Sub como en el ejemplo
            r.publish("nueva_persona", f"Nuevo usuario: {nombre}")
            tracer.log("Evento publicado en canal 'nueva_persona'", "info")
            result_data = nuevo_p

    # ==========================================
    # 4. ACTUALIZAR (GET -> MODIFY -> PUT)
    # ==========================================
    elif operation == "actualizar":
        nuevo_ingreso = ingresos + 5000
        tracer.log(f"Actualizando ingresos de {dni} a {nuevo_ingreso}", "info")

        if db == "riak":
            current = get_object_riak('poblacion', dni)
            if current:
                current['ingresos'] = nuevo_ingreso
                idxs = {'ingresos_int': nuevo_ingreso, 'sector_int': current.get('sector', 2),
                        'sexo_bin': current.get('sexo', 'M')}
                store_object_riak('poblacion', dni, current, idxs)
                result_data = current

        elif db == "mongo":
            tracer.log("db.poblacion.updateOne(...)", "warn")

        elif db == "redis":
            key = f"poblacion:{dni}"
            # Redis JSON permite actualizar solo un campo (Path)
            try:
                r.json().set(key, "$.ingresos", nuevo_ingreso)
                tracer.log(f"r.json().set('{key}', '$.ingresos', {nuevo_ingreso})", "success")
                result_data = r.json().get(key)
            except Exception as e:
                tracer.log(f"Error actualizando: {e}", "error")

    # ==========================================
    # 5. BORRAR (DELETE)
    # ==========================================
    elif operation == "borrar":
        tracer.log(f"Eliminando DNI: {dni}", "warn")
        if db == "riak":
            delete_object_riak('poblacion', dni)
            tracer.log("Borrado de Riak.", "success")

        elif db == "mongo":
            tracer.log("db.poblacion.deleteOne(...)", "warn")

        elif db == "redis":
            key = f"poblacion:{dni}"
            deleted = r.delete(key)
            if deleted:
                tracer.log(f"r.delete('{key}') -> 1 (OK)", "success")
            else:
                tracer.log("La clave no existía.", "error")

    # ==========================================
    # OPERACIONES AVANZADAS
    # ==========================================
    elif operation == "indexar":
        if db == "riak":
            keys = get_keys_riak('poblacion')
            count = 0
            for k in keys:
                data = get_object_riak('poblacion', k)
                if data:
                    mis_indices = {'ingresos_int': data.get('ingresos', 0), 'sector_int': data.get('sector', 1),
                                   'sexo_bin': data.get('sexo', 'M')}
                    store_object_riak('poblacion', k, data, mis_indices)
                    count += 1
            tracer.log(f"Re-indexados {count} objetos.", "success")

        elif db == "redis":
            tracer.log("En Redis Stack los índices se actualizan automáticamente al insertar JSON.", "info")
            try:
                info = r.ft(INDEX_NAME).info()
                num_docs = info.get('num_docs')
                tracer.log(f"Estado del índice: {num_docs} documentos indexados.", "success")
                result_data = str(info)
            except:
                tracer.log("El índice no existe. Ejecuta 'Cargar Datos' primero.", "error")

    elif operation == "rango":
        tracer.log(f"Buscando ingresos entre {min_val} y {max_val}", "info")

        if db == "riak":
            keys = query_2i_range_riak('poblacion', 'ingresos_int', min_val, max_val)
            tracer.log(f"Claves encontradas: {len(keys)}", "info")
            result_data = [get_object_riak('poblacion', k) for k in keys[:5]]

        elif db == "redis":
            # RediSearch Query
            query_str = f"@ingresos:[{min_val} {max_val}]"
            q = RedisQuery(query_str).paging(0, 10)  # Paginación nativa

            try:
                res = r.ft(INDEX_NAME).search(q)
                tracer.log(f"r.ft('{INDEX_NAME}').search('{query_str}')", "info")
                tracer.log(f"Encontrados: {res.total} documentos.", "success")
                # Parsear resultado
                result_data = [json.loads(doc.json) for doc in res.docs]
            except Exception as e:
                tracer.log(f"Error búsqueda: {e}", "error")

        elif db == "mongo":
            time.sleep(random.uniform(0.02, 0.05))
            tracer.log("db.poblacion.find(...)", "warn")

    elif operation == "filtro":
        tracer.log(f"Filtro: Sector {sector} y Sexo {sexo}", "info")

        if db == "riak":
            keys = query_2i_riak('poblacion', 'sector_int', sector)
            found = []
            for k in keys:
                d = get_object_riak('poblacion', k)
                if d and d['sexo'] == sexo: found.append(d)
            result_data = found
            tracer.log(f"Filtrado manual (App-side join): {len(found)} resultados.", "success")

        elif db == "redis":
            # RediSearch Composite Query
            query_str = f"@sector:[{sector} {sector}] @sexo:{sexo}"
            q = RedisQuery(query_str)
            try:
                res = r.ft(INDEX_NAME).search(q)
                tracer.log(f"Query: {query_str}", "info")
                tracer.log(f"Motor de búsqueda devolvió {res.total} resultados.", "success")
                result_data = [json.loads(doc.json) for doc in res.docs]
            except Exception as e:
                tracer.log(f"Error: {e}", "error")

    elif operation == "agregacion":
        tracer.log(f"Calculando total de ingresos para Sector {sector}", "info")

        if db == "riak":
            # ... lógica riak existente ...
            keys = get_keys_riak('poblacion')
            total = 0
            for k in keys:
                obj = get_object_riak('poblacion', k)
                if obj and str(obj['sector']) == str(sector): total += obj.get('ingresos', 0)
            result_data = {"sector": sector, "total": total}
            tracer.log("Cálculo lado cliente completado.", "success")

        elif db == "redis":
            # Lógica basada en tu ejemplo: Iterar keys, sumar y guardar resumen
            tracer.log("Iterando claves 'poblacion:*' para sumar ingresos...", "info")
            keys = r.keys("poblacion:*")
            total = 0
            count = 0
            for k in keys:
                # Obtenemos solo los campos necesarios para optimizar
                d = r.json().get(k, "$.sector", "$.ingresos")
                # r.json().get devuelve diccionario si pedimos paths específicos
                if d:
                    # La estructura devuelta puede variar según versión, asumimos dict simple o lista
                    # Si es $.path devuelve lista de matches, tomamos el [0]
                    sec_val = d.get('$.sector', [0])[0] if isinstance(d.get('$.sector'), list) else d.get('$.sector')
                    ing_val = d.get('$.ingresos', [0])[0] if isinstance(d.get('$.ingresos'), list) else d.get(
                        '$.ingresos')

                    if str(sec_val) == str(sector):
                        total += ing_val
                        count += 1

            # Guardamos el resumen como pediste
            r.hset("resumen_sector", str(sector), total)
            tracer.log(f"Hash 'resumen_sector' actualizado: {sector} -> {total}", "success")
            result_data = {"sector": sector, "total": total, "procesados": count}

    exec_time = tracer.get_execution_time()
    tracer.log(f"Tiempo total: {exec_time} ms", "timer")
    return {"trace": tracer.logs, "data": result_data, "time_ms": exec_time}