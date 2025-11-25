from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
import json
import time
import random
import os
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
    try:
        from datasets.dataset import sectores, poblacion
    except ImportError:
        from dataset import sectores, poblacion
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

# --- CONFIGURACIÓN ---
RIAK_HOST = 'http://localhost:8098'
HEADERS_JSON = {'Content-Type': 'application/json'}
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
INDEX_NAME = "idx_poblacion"
MONGO_URI = "mongodb+srv://admin:12345@pbd-proyecto.tsbceg9.mongodb.net/?retryWrites=true&w=majority"

# --- INICIALIZACIÓN CLIENTES ---
try:
    r = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
except: r = None

try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db_mongo = mongo_client['practica_db']
except:
    mongo_client = None
    db_mongo = None


# --- CLASE TRAZA ---
class Tracer:
    def __init__(self, db_name):
        self.logs = []
        self.db = db_name.upper()
        self.start_time = time.time()

    def log(self, msg, type="info"):
        # Iconos para hacer la terminal más visual
        emoji = {
            "info": "ℹ️", "success": "✅", "error": "❌", "warn": "⚠️", 
            "timer": "⏱️", "pubsub": "📢", "net": "🌐", "db": "🗄️"
        }.get(type, "🔹")
        self.logs.append(f"[{self.db}] {emoji} {msg}")

    def get_execution_time(self):
        return round((time.time() - self.start_time) * 1000, 2)


# --- HELPERS RIAK ---
def check_riak_connection():
    try: return requests.get(f"{RIAK_HOST}/ping", timeout=1).status_code == 200
    except: return False

def get_keys_riak(bucket):
    try: return requests.get(f"{RIAK_HOST}/buckets/{bucket}/keys?keys=true").json().get('keys', [])
    except: return []

def get_object_riak(bucket, key):
    try: return requests.get(f"{RIAK_HOST}/buckets/{bucket}/keys/{key}").json()
    except: return None

def store_object_riak(bucket, key, data, indexes=None):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    headers = HEADERS_JSON.copy()
    if indexes:
        for k, v in indexes.items(): headers[f"x-riak-index-{k}"] = str(v)
    requests.put(url, data=json.dumps(data), headers=headers)

def delete_object_riak(bucket, key):
    requests.delete(f"{RIAK_HOST}/buckets/{bucket}/keys/{key}")

def query_2i_riak(bucket, index, val):
    return requests.get(f"{RIAK_HOST}/buckets/{bucket}/index/{index}/{val}").json().get('keys', [])

def query_2i_range_riak(bucket, index, min_val, max_val):
    return requests.get(f"{RIAK_HOST}/buckets/{bucket}/index/{index}/{min_val}/{max_val}").json().get('keys', [])

def serialize_mongo(doc):
    if not doc: return None
    doc['_id'] = str(doc['_id'])
    if 'fechanac' in doc and isinstance(doc['fechanac'], datetime):
        doc['fechanac'] = doc['fechanac'].strftime('%Y-%m-%d')
    return doc


# --- ENDPOINTS ---

@app.get("/")
def serve_ui():
    return FileResponse("index.html")

@app.get("/load-data")
def load_data():
    tracer = Tracer("SYSTEM")
    
    # RIAK
    if check_riak_connection():
        tracer.log("Conectando a Riak Node 1...", "net")
        if sectores and poblacion:
            count = 0
            for p in poblacion:
                idxs = {'ingresos_int': p['ingresos'], 'sector_int': p['sector'], 'sexo_bin': p['sexo']}
                store_object_riak("poblacion", p['dni'], p, idxs)
                count += 1
            tracer.log(f"Riak: Insertados {count} objetos con índices secundarios (2i).", "success")
    
    # REDIS
    if r:
        tracer.log("Conectando a Redis Stack...", "net")
        try:
            try: r.ft(INDEX_NAME).dropindex(True)
            except: pass
            pipe = r.pipeline()
            for p in poblacion: pipe.json().set(f"poblacion:{p['dni']}", "$", p)
            pipe.execute()
            
            schema = (TextField("$.nombre", as_name="nombre"), NumericField("$.ingresos", as_name="ingresos"), NumericField("$.sector", as_name="sector"), TextField("$.sexo", as_name="sexo"))
            r.ft(INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=["poblacion:"], index_type=IndexType.JSON))
            tracer.log(f"Redis: {len(poblacion)} JSONs cargados e indexados (RediSearch).", "success")
        except Exception as e: tracer.log(f"Redis Err: {e}", "error")

    # MONGO
    if db_mongo is not None:
        tracer.log("Conectando a MongoDB Atlas Cluster...", "net")
        try:
            db_mongo.poblacion.delete_many({})
            poblacion_mongo = []
            for p in poblacion:
                p_c = p.copy()
                p_c['_id'] = p_c['dni'] # ID forzado
                if 'fechanac' in p_c and isinstance(p_c['fechanac'], str):
                    try: p_c['fechanac'] = datetime.strptime(p_c['fechanac'], '%Y-%m-%d')
                    except: pass
                poblacion_mongo.append(p_c)
            
            if poblacion_mongo: db_mongo.poblacion.insert_many(poblacion_mongo)
            db_mongo.poblacion.create_index([("ingresos", ASCENDING)])
            db_mongo.poblacion.create_index([("sector", ASCENDING), ("sexo", ASCENDING)])
            tracer.log(f"Mongo: Colección 'poblacion' regenerada con {len(poblacion_mongo)} docs.", "success")
        except Exception as e: tracer.log(f"Mongo Err: {e}", "error")

    return {"trace": tracer.logs, "msg": "Carga completa"}


@app.get("/execute/{operation}/{db}")
def execute_operation(operation: str, db: str, dni: str=Query("123456789"), nombre: str=Query("Laura"), ingresos: int=Query(30000), min_val: int=Query(20000), max_val: int=Query(50000), sector: int=Query(2), sexo: str=Query("M")):
    tracer = Tracer(db)
    result_data = {}

    # --- LISTAR ---
    if operation == "listar":
        tracer.log(f"Iniciando escaneo de claves en {db}...", "info")
        if db == "riak":
            tracer.log("HTTP GET /buckets/poblacion/keys?keys=true", "net")
            keys = get_keys_riak('poblacion')
            tracer.log(f"Bucket devuelve {len(keys)} claves.", "db")
            tracer.log("Obteniendo objetos completos (GET secuencial para los primeros 5)...", "info")
            result_data = [get_object_riak('poblacion', k) for k in keys[:5]]
        elif db == "redis":
            tracer.log("Ejecutando 'KEYS poblacion:*' (Scan pattern match)", "db")
            keys = r.keys("poblacion:*")[:5]
            tracer.log("Recuperando valores JSON con 'JSON.GET'...", "info")
            result_data = [r.json().get(k) for k in keys]
        elif db == "mongo":
            tracer.log("Ejecutando db.poblacion.find({}).limit(5)", "db")
            cursor = db_mongo.poblacion.find({}).limit(5)
            result_data = [serialize_mongo(d) for d in cursor]
            tracer.log(f"Cursor agotado. Recuperados {len(result_data)} documentos.", "success")

    # --- LEER ---
    elif operation == "leer":
        tracer.log(f"Consulta directa por Clave Primaria: {dni}", "info")
        if db == "riak":
            tracer.log(f"HTTP GET /buckets/poblacion/keys/{dni}", "net")
            result_data = get_object_riak('poblacion', dni)
            if result_data: tracer.log("Objeto recuperado (200 OK)", "success")
            else: tracer.log("404 Not Found", "error")
        elif db == "redis":
            tracer.log(f"Comando: JSON.GET poblacion:{dni}", "db")
            result_data = r.json().get(f"poblacion:{dni}")
            if result_data: tracer.log("JSON obtenido de memoria.", "success")
            else: tracer.log("Clave no existe.", "error")
        elif db == "mongo":
            tracer.log(f"Query: db.poblacion.findOne({{_id: '{dni}'}})", "db")
            doc = db_mongo.poblacion.find_one({"_id": dni})
            if doc:
                result_data = serialize_mongo(doc)
                tracer.log("Documento BSON encontrado.", "success")
            else: tracer.log("Null result.", "error")

    # --- INSERTAR ---
    elif operation == "insertar":
        tracer.log(f"Preparando inserción de '{nombre}'...", "info")
        nuevo_p = {"dni": dni, "nombre": nombre, "ingresos": ingresos, "sector": sector, "sexo": sexo}
        
        if db == "riak":
            tracer.log(f"Verificando duplicados (GET previo)...", "net")
            if get_object_riak('poblacion', dni):
                tracer.log("Error: DNI ya existe. Riak sobrescribiría sin este chequeo manual.", "error")
            else:
                idxs = {'ingresos_int': ingresos, 'sector_int': sector, 'sexo_bin': sexo}
                tracer.log(f"HTTP PUT /keys/{dni} + Headers (x-riak-index)", "net")
                store_object_riak('poblacion', dni, nuevo_p, idxs)
                tracer.log("Objeto guardado e indexado.", "success")
                result_data = nuevo_p
        elif db == "redis":
            key = f"poblacion:{dni}"
            if r.exists(key): tracer.log(f"Error: Clave {key} ya existe.", "error")
            else:
                tracer.log(f"JSON.SET {key} $ ...", "db")
                r.json().set(key, "$", nuevo_p)
                r.publish("nueva_persona", f"Se ha unido {nombre}")
                tracer.log("Evento Pub/Sub enviado a canal 'nueva_persona'.", "pubsub")
                result_data = nuevo_p
        elif db == "mongo":
            try:
                doc = nuevo_p.copy(); doc['_id'] = dni
                tracer.log(f"db.poblacion.insertOne(doc)", "db")
                res = db_mongo.poblacion.insert_one(doc)
                tracer.log(f"Insertado. WriteConcern: Acknowledged.", "success")
                result_data = nuevo_p
            except DuplicateKeyError: tracer.log("Error MongoDB: E11000 duplicate key error", "error")

    # --- ACTUALIZAR ---
    elif operation == "actualizar":
        tracer.log(f"Operación de modificación de ingresos...", "info")
        nuevo_ing = ingresos + 1000
        if db == "riak":
            tracer.log("Riak requiere lectura previa (Fetch)", "info")
            curr = get_object_riak('poblacion', dni)
            if curr:
                tracer.log("Objeto recuperado. Modificando en memoria...", "info")
                curr['ingresos'] = nuevo_ing
                idxs = {'ingresos_int': nuevo_ing, 'sector_int': curr.get('sector'), 'sexo_bin': curr.get('sexo')}
                store_object_riak('poblacion', dni, curr, idxs)
                tracer.log("HTTP PUT ejecutado (Store).", "success")
                result_data = curr
            else: tracer.log("Error: Objeto no existe.", "error")
        elif db == "redis":
            tracer.log(f"JSON.SET poblacion:{dni} $.ingresos {nuevo_ing}", "db")
            try: 
                r.json().set(f"poblacion:{dni}", "$.ingresos", nuevo_ing)
                tracer.log("Campo actualizado atómicamente en RAM.", "success")
            except: tracer.log("Clave no encontrada.", "error")
        elif db == "mongo":
            tracer.log(f"db.poblacion.updateOne({{_id:'{dni}'}}, {{$set:{{ingresos:{nuevo_ing}}}}})", "db")
            res = db_mongo.poblacion.update_one({"_id": dni}, {"$set": {"ingresos": nuevo_ing}})
            if res.modified_count > 0: tracer.log("Documento actualizado en disco.", "success")
            else: tracer.log("Sin cambios o documento no encontrado.", "warn")

    # --- BORRAR ---
    elif operation == "borrar":
        tracer.log(f"Solicitud de borrado para ID: {dni}", "warn")
        if db == "riak":
            tracer.log(f"HTTP DELETE /keys/{dni}", "net")
            delete_object_riak('poblacion', dni)
            tracer.log("Tombstone creado (Borrado eventual).", "success")
        elif db == "redis":
            tracer.log(f"DEL poblacion:{dni}", "db")
            if r.delete(f"poblacion:{dni}"): tracer.log("Clave eliminada de memoria.", "success")
            else: tracer.log("Clave no existía.", "warn")
        elif db == "mongo":
            tracer.log(f"db.poblacion.deleteOne({{_id:'{dni}'}})", "db")
            if db_mongo.poblacion.delete_one({"_id": dni}).deleted_count > 0: tracer.log("Documento eliminado.", "success")
            else: tracer.log("Documento no encontrado.", "warn")

    # --- INDEXAR ---
    elif operation == "indexar":
        if db == "riak":
            tracer.log("Iniciando re-indexado masivo (Map manual)...", "info")
            keys = get_keys_riak('poblacion')
            c = 0
            for k in keys:
                d = get_object_riak('poblacion', k)
                if d:
                    idxs = {'ingresos_int': d.get('ingresos',0), 'sector_int': d.get('sector',1), 'sexo_bin': d.get('sexo','M')}
                    store_object_riak('poblacion', k, d, idxs)
                    c+=1
            tracer.log(f"Procesados {c} objetos. Cabeceras HTTP 2i actualizadas.", "success")
        elif db == "redis":
            tracer.log("Verificando definición de índice RediSearch...", "info")
            try: 
                info = r.ft(INDEX_NAME).info()
                tracer.log(f"Índice '{INDEX_NAME}' activo. Documentos: {info['num_docs']}", "success")
                result_data = str(info['num_docs'])
            except: tracer.log("Índice no encontrado.", "error")
        elif db == "mongo":
            tracer.log("Creando índices B-Tree en background...", "info")
            db_mongo.poblacion.create_index([("ingresos", ASCENDING)])
            db_mongo.poblacion.create_index([("sector", ASCENDING), ("sexo", ASCENDING)])
            tracer.log("Indices ensureIndex() completado.", "success")

    # --- RANGO ---
    elif operation == "rango":
        tracer.log(f"Buscando ingresos [{min_val} - {max_val}]", "info")
        if db == "riak":
            tracer.log(f"HTTP GET /index/ingresos_int/{min_val}/{max_val}", "net")
            keys = query_2i_range_riak('poblacion', 'ingresos_int', min_val, max_val)
            tracer.log(f"Riak devolvió {len(keys)} claves candidatas.", "db")
            result_data = [get_object_riak('poblacion', k) for k in keys]
            tracer.log("Objetos recuperados (Map phase client-side).", "success")
        elif db == "redis":
            q = RedisQuery(f"@ingresos:[{min_val} {max_val}]")
            tracer.log(f"FT.SEARCH {INDEX_NAME} '{q.query_string()}'", "db")
            res = r.ft(INDEX_NAME).search(q)
            result_data = [json.loads(d.json) for d in res.docs]
            tracer.log(f"RediSearch encontró {len(result_data)} resultados.", "success")
        elif db == "mongo":
            q = {"ingresos": {"$gte": min_val, "$lte": max_val}}
            tracer.log(f"db.poblacion.find({q})", "db")
            cursor = db_mongo.poblacion.find(q)
            result_data = [serialize_mongo(d) for d in cursor]
            tracer.log(f"Cursor retornó {len(result_data)} documentos.", "success")

    # --- FILTRO ---
    elif operation == "filtro":
        tracer.log(f"Filtro Compuesto: Sector {sector} AND Sexo '{sexo}'", "info")
        if db == "riak":
            tracer.log("Paso 1: Consulta 2i por Sector (Filtro primario)", "info")
            tracer.log(f"HTTP GET /index/sector_int/{sector}", "net")
            keys = query_2i_riak('poblacion', 'sector_int', sector)
            tracer.log(f"Riak retornó {len(keys)} claves del sector {sector}.", "db")
            tracer.log("Paso 2: Filtrado 'Application Side' por Sexo...", "info")
            result_data = []
            for k in keys:
                obj = get_object_riak('poblacion', k)
                if obj and obj.get('sexo') == sexo: result_data.append(obj)
            tracer.log(f"Filtrado finalizado: {len(result_data)} coincidencias.", "success")
        elif db == "redis":
            q_str = f"@sector:[{sector} {sector}] @sexo:{sexo}"
            tracer.log(f"FT.SEARCH {INDEX_NAME} '{q_str}'", "db")
            res = r.ft(INDEX_NAME).search(RedisQuery(q_str))
            result_data = [json.loads(d.json) for d in res.docs]
            tracer.log(f"Motor de búsqueda devolvió {res.total} resultados.", "success")
        elif db == "mongo":
            q = {"$or": [{"sector": sector}, {"sector_id": sector}], "sexo": sexo}
            tracer.log(f"db.poblacion.find({q})", "db")
            cursor = db_mongo.poblacion.find(q)
            result_data = [serialize_mongo(d) for d in cursor]
            tracer.log(f"Mongo aprovechó índice compuesto. {len(result_data)} docs.", "success")

    # --- AGREGACION ---
    elif operation == "agregacion":
        tracer.log(f"Calculando suma de ingresos del Sector {sector}", "info")
        if db == "riak":
            tracer.log("Riak no tiene 'GROUP BY'. Iniciando MapReduce manual...", "warn")
            keys = get_keys_riak('poblacion')
            total = 0
            for k in keys:
                obj = get_object_riak('poblacion', k)
                if obj and str(obj.get('sector')) == str(sector):
                    total += obj.get('ingresos', 0)
            tracer.log(f"Iteradas {len(keys)} claves. Total calculado en Python.", "success")
            result_data = {"sector": sector, "total": total}
        elif db == "redis":
            tracer.log("Iterando claves 'poblacion:*' (Scan)...", "info")
            keys = r.keys("poblacion:*")
            total = 0
            for k in keys:
                # Optimización: traemos solo campos necesarios
                p = r.json().get(k, "$.sector", "$.ingresos") 
                # Redis devuelve lista de resultados para path $
                s_val = p.get('$.sector')[0] if p and p.get('$.sector') else 0
                i_val = p.get('$.ingresos')[0] if p and p.get('$.ingresos') else 0
                if str(s_val) == str(sector): total += i_val
            r.hset("resumen_sector", str(sector), total)
            tracer.log(f"Calculado en memoria. Hash 'resumen_sector' actualizado.", "success")
            result_data = {"sector": sector, "total": total}
        elif db == "mongo":
            tracer.log("Construyendo Pipeline de Agregación...", "info")
            pipeline = [
                {"$match": {"$or": [{"sector": sector}, {"sector_id": sector}]}},
                {"$group": {"_id": "$sector", "total": {"$sum": "$ingresos"}}}
            ]
            tracer.log(f"db.poblacion.aggregate({json.dumps(pipeline)})", "db")
            res = list(db_mongo.poblacion.aggregate(pipeline))
            result_data = [{"sector": r['_id'], "total": r['total']} for r in res]
            tracer.log("Agregación ejecutada nativamente en servidor.", "success")

    exec_time = tracer.get_execution_time()
    tracer.log(f"Tiempo de ejecución: {exec_time} ms", "timer")
    return {"trace": tracer.logs, "data": result_data, "time_ms": exec_time}