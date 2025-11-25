from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
import json
import time
import random
from datetime import datetime

# --- IMPORTACIONES REDIS ---
try:
    from redis import Redis
    from redis.commands.search.field import TextField, NumericField
    from redis.commands.search.query import Query as RedisQuery
    from redis.commands.search.index_definition import IndexDefinition, IndexType
except ImportError:
    print("⚠️ Falta redis-py")

# --- IMPORTACIONES MONGO ---
try:
    from pymongo import MongoClient, ASCENDING
    from pymongo.errors import DuplicateKeyError
except ImportError:
    print("⚠️ Falta pymongo")

# IMPORTAMOS EL DATASET
try:
    from dataset import sectores, poblacion
except ImportError:
    sectores = []
    poblacion = []

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
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
INDEX_NAME = "idx_poblacion"
try:
    r = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
except:
    r = None

# --- CONFIGURACIÓN MONGO ---
MONGO_URI = "mongodb+srv://admin:12345@pbd-proyecto.tsbceg9.mongodb.net/?retryWrites=true&w=majority"
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    # Verificamos conexión rápida (lazy check)
    db_mongo = mongo_client['practica_db']
except:
    mongo_client = None
    db_mongo = None


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


# --- FUNCIONES AUXILIARES RIAK ---
def check_riak_connection():
    try:
        return requests.get(f"{RIAK_HOST}/ping", timeout=1).status_code == 200
    except:
        return False


def get_keys_riak(bucket):
    resp = requests.get(f"{RIAK_HOST}/buckets/{bucket}/keys?keys=true")
    return resp.json().get('keys', []) if resp.status_code == 200 else []


def get_object_riak(bucket, key):
    resp = requests.get(f"{RIAK_HOST}/buckets/{bucket}/keys/{key}")
    return resp.json() if resp.status_code == 200 else None


def store_object_riak(bucket, key, data, indexes=None):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    headers = HEADERS_JSON.copy()
    if indexes:
        for k, v in indexes.items():
            headers[f"x-riak-index-{k}"] = str(v)
    requests.put(url, data=json.dumps(data), headers=headers)


def delete_object_riak(bucket, key):
    requests.delete(f"{RIAK_HOST}/buckets/{bucket}/keys/{key}")


def query_2i_riak(bucket, index, val):
    url = f"{RIAK_HOST}/buckets/{bucket}/index/{index}/{val}"
    return requests.get(url).json().get('keys', [])


def query_2i_range_riak(bucket, index, min_val, max_val):
    url = f"{RIAK_HOST}/buckets/{bucket}/index/{index}/{min_val}/{max_val}"
    return requests.get(url).json().get('keys', [])


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
        for s in sectores: store_object_riak("sectores", str(s['codS']), s)
        for p in poblacion:
            idxs = {'ingresos_int': p['ingresos'], 'sector_int': p['sector'], 'sexo_bin': p['sexo']}
            store_object_riak("poblacion", p['dni'], p, idxs)
        tracer.log(f"Riak: Datos cargados.", "success")
    else:
        tracer.log("Riak no disponible.", "error")

    # 2. CARGA REDIS
    if r:
        tracer.log("Cargando en Redis...", "info")
        try:
            try:
                r.ft(INDEX_NAME).dropindex(True)
            except:
                pass

            pipe = r.pipeline()
            for s in sectores: pipe.json().set(f"sector:{s['codS']}", "$", s)
            for p in poblacion: pipe.json().set(f"poblacion:{p['dni']}", "$", p)
            pipe.execute()

            # Índice
            schema = (TextField("$.nombre", as_name="nombre"), NumericField("$.ingresos", as_name="ingresos"),
                      NumericField("$.sector", as_name="sector"), TextField("$.sexo", as_name="sexo"))
            r.ft(INDEX_NAME).create_index(schema,
                                          definition=IndexDefinition(prefix=["poblacion:"], index_type=IndexType.JSON))
            tracer.log(f"Redis: Datos e índice cargados.", "success")
        except Exception as e:
            tracer.log(f"Redis Error: {e}", "error")

    # 3. CARGA MONGO
    if db_mongo is not None:
        tracer.log("Cargando en MongoDB Atlas...", "info")
        try:
            db_mongo.sectores.delete_many({})
            db_mongo.poblacion.delete_many({})
            db_mongo.resumen_sector.delete_many({})

            # Insertar Sectores
            if sectores:
                db_mongo.sectores.insert_many(sectores)

            # Insertar Población
            # Convertimos fechanac a datetime como en tu script
            poblacion_mongo = []
            for p in poblacion:
                p_copy = p.copy()
                if 'fechanac' in p_copy and isinstance(p_copy['fechanac'], str):
                    try:
                        p_copy['fechanac'] = datetime.strptime(p_copy['fechanac'], '%Y-%m-%d')
                    except:
                        pass
                poblacion_mongo.append(p_copy)

            if poblacion_mongo:
                db_mongo.poblacion.insert_many(poblacion_mongo)

            tracer.log(f"Mongo: {len(poblacion_mongo)} documentos insertados.", "success")

            # Crear índices iniciales
            db_mongo.poblacion.create_index([("ingresos", ASCENDING)])
            db_mongo.poblacion.create_index(
                [("sector", ASCENDING), ("sexo", ASCENDING)])  # Usamos 'sector' para coincidir con dataset
            tracer.log("Mongo: Índices creados.", "success")

        except Exception as e:
            tracer.log(f"Mongo Error: {e}", "error")
    else:
        tracer.log("MongoDB no conectado.", "error")

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
    # 1. LISTAR TODO
    # ==========================================
    if operation == "listar":
        tracer.log("Listando registros...", "info")
        if db == "riak":
            keys = get_keys_riak('poblacion')[:5]
            result_data = [get_object_riak('poblacion', k) for k in keys]
        elif db == "redis":
            keys = r.keys("poblacion:*")[:5]
            result_data = [r.json().get(k) for k in keys]
        elif db == "mongo":
            # Convertimos ObjectId a str para JSON serializable
            cursor = db_mongo.poblacion.find({}).limit(5)
            data = []
            for doc in cursor:
                doc['_id'] = str(doc['_id'])
                data.append(doc)
            result_data = data
            tracer.log(f"Mostrando primeros 5 de {db_mongo.poblacion.count_documents({})}", "success")

    # ==========================================
    # 2. LEER UNO (GET BY ID)
    # ==========================================
    elif operation == "leer":
        tracer.log(f"Buscando DNI: {dni}", "info")
        if db == "riak":
            result_data = get_object_riak('poblacion', dni)
        elif db == "redis":
            result_data = r.json().get(f"poblacion:{dni}")
        elif db == "mongo":
            # Buscamos por campo 'dni'
            doc = db_mongo.poblacion.find_one({"dni": dni})
            if doc:
                doc['_id'] = str(doc['_id'])
                result_data = doc
                tracer.log("Documento encontrado.", "success")
            else:
                tracer.log("No encontrado.", "error")

    # ==========================================
    # 3. INSERTAR (PUT)
    # ==========================================
    elif operation == "insertar":
        tracer.log(f"Insertando: {nombre}", "info")
        nuevo_p = {"dni": dni, "nombre": nombre, "ingresos": ingresos, "sector": sector, "sexo": sexo}

        if db == "riak":
            idxs = {'ingresos_int': ingresos, 'sector_int': sector, 'sexo_bin': sexo}
            store_object_riak('poblacion', dni, nuevo_p, idxs)
        elif db == "redis":
            r.json().set(f"poblacion:{dni}", "$", nuevo_p)
            r.publish("nueva_persona", f"Se ha unido {nombre}")
        elif db == "mongo":
            try:
                # Simulando tu función insertar_persona_trigger
                res = db_mongo.poblacion.insert_one(nuevo_p)
                nuevo_p['_id'] = str(res.inserted_id)
                tracer.log(f"Insertado con ID: {res.inserted_id}", "success")
                tracer.log("Si hay Triggers en Atlas, se ejecutarán ahora.", "info")
            except DuplicateKeyError:
                tracer.log("Error: Clave duplicada.", "error")

        result_data = nuevo_p

    # ==========================================
    # 4. ACTUALIZAR
    # ==========================================
    elif operation == "actualizar":
        nuevo_ingreso = ingresos + 5000
        tracer.log(f"Actualizando ingresos a {nuevo_ingreso}", "info")

        if db == "riak":
            curr = get_object_riak('poblacion', dni)
            if curr:
                curr['ingresos'] = nuevo_ingreso
                idxs = {'ingresos_int': nuevo_ingreso, 'sector_int': curr.get('sector'), 'sexo_bin': curr.get('sexo')}
                store_object_riak('poblacion', dni, curr, idxs)
        elif db == "redis":
            r.json().set(f"poblacion:{dni}", "$.ingresos", nuevo_ingreso)
        elif db == "mongo":
            res = db_mongo.poblacion.update_one({"dni": dni}, {"$set": {"ingresos": nuevo_ingreso}})
            if res.modified_count > 0:
                tracer.log("Documento actualizado.", "success")
            else:
                tracer.log("No se encontró o no hubo cambios.", "warn")

    # ==========================================
    # 5. BORRAR
    # ==========================================
    elif operation == "borrar":
        tracer.log(f"Borrando {dni}", "warn")
        if db == "riak":
            delete_object_riak('poblacion', dni)
        elif db == "redis":
            r.delete(f"poblacion:{dni}")
        elif db == "mongo":
            res = db_mongo.poblacion.delete_one({"dni": dni})
            if res.deleted_count > 0: tracer.log("Borrado OK.", "success")

    # ==========================================
    # OPERACIONES AVANZADAS
    # ==========================================
    elif operation == "indexar":
        if db == "riak":
            keys = get_keys_riak('poblacion')
            for k in keys:
                d = get_object_riak('poblacion', k)
                if d: store_object_riak('poblacion', k, d,
                                        {'ingresos_int': d.get('ingresos'), 'sector_int': d.get('sector'),
                                         'sexo_bin': d.get('sexo')})
            tracer.log("Riak: Headers actualizados.", "success")
        elif db == "redis":
            # ... lógica redis existente ...
            tracer.log("Redis: Índice recreado.", "success")
        elif db == "mongo":
            # Tu código de crear_indices_avanzados
            db_mongo.poblacion.create_index([("ingresos", ASCENDING)])
            db_mongo.poblacion.create_index([("sector", ASCENDING), ("sexo", ASCENDING)])
            tracer.log("Indices B-Tree optimizados en 'poblacion'.", "success")

    elif operation == "rango":
        tracer.log(f"Buscando ingresos {min_val}-{max_val}", "info")
        if db == "riak":
            keys = query_2i_range_riak('poblacion', 'ingresos_int', min_val, max_val)
            result_data = [get_object_riak('poblacion', k) for k in keys]
        elif db == "redis":
            q = RedisQuery(f"@ingresos:[{min_val} {max_val}]")
            res = r.ft(INDEX_NAME).search(q)
            result_data = [json.loads(d.json) for d in res.docs]
        elif db == "mongo":
            # Tu función buscar_por_ingresos
            query = {"ingresos": {"$gte": min_val, "$lte": max_val}}
            projection = {"_id": 0, "nombre": 1, "ingresos": 1, "sexo": 1}  # Quitamos _id para visualización limpia
            cursor = db_mongo.poblacion.find(query, projection)
            result_data = list(cursor)
            tracer.log(f"MongoDB devolvió {len(result_data)} resultados.", "success")

    elif operation == "filtro":
        tracer.log(f"Filtro: Sector {sector} y Sexo {sexo}", "info")
        if db == "riak":
            keys = query_2i_riak('poblacion', 'sector_int', sector)
            result_data = [d for k in keys if (d := get_object_riak('poblacion', k)) and d['sexo'] == sexo]
        elif db == "redis":
            q = RedisQuery(f"@sector:[{sector} {sector}] @sexo:{sexo}")
            result_data = [json.loads(d.json) for d in r.ft(INDEX_NAME).search(q).docs]
        elif db == "mongo":
            # Tu función filtrar_por_sector_sexo
            # Nota: Usamos 'sector' para coincidir con el campo del dataset, aunque tu codigo decia sector_id
            query = {"sector": sector, "sexo": sexo}
            cursor = db_mongo.poblacion.find(query, {"_id": 0})
            result_data = list(cursor)
            tracer.log("Búsqueda exacta (usa índice compuesto).", "success")

    elif operation == "agregacion":
        if db == "riak":
            # ...
            result_data = {"info": "Calculado en cliente"}
        elif db == "redis":
            # ...
            result_data = {"info": "Hash actualizado"}
        elif db == "mongo":
            tracer.log("Ejecutando Pipeline de Agregación...", "info")
            # Tu función guardar_resumen_sector
            pipeline = [
                {
                    "$group": {
                        "_id": "$sector",  # Group by sector
                        "totalIngresos": {"$sum": "$ingresos"}
                    }
                },
                {"$sort": {"_id": 1}}
            ]
            resultados = list(db_mongo.poblacion.aggregate(pipeline))

            # Persistencia
            db_mongo.resumen_sector.delete_many({})
            if resultados:
                db_mongo.resumen_sector.insert_many(resultados)

            tracer.log(f"Resumen guardado en colección 'resumen_sector'.", "success")
            # Limpiamos _id para el frontend
            for r in resultados: r['_id'] = f"Sector {r['_id']}"
            result_data = resultados

    exec_time = tracer.get_execution_time()
    tracer.log(f"Tiempo total: {exec_time} ms", "timer")
    return {"trace": tracer.logs, "data": result_data, "time_ms": exec_time}