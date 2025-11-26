from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
import json
import time
from datetime import datetime

try:
    from redis import Redis
    from redis.commands.search.field import TextField, NumericField
    from redis.commands.search.query import Query as RedisQuery
    from redis.commands.search.index_definition import IndexDefinition, IndexType
except ImportError:
    print("Falta redis-py")

try:
    from pymongo import MongoClient, ASCENDING
    from pymongo.errors import DuplicateKeyError, BulkWriteError
except ImportError:
    print("Falta pymongo")

try:
    try:
        from datasets.dataset import sectores, poblacion
    except ImportError:
        from dataset import sectores, poblacion
except ImportError:
    sectores = []
    poblacion = []
    print("ADVERTENCIA: No se encontró 'dataset.py'.")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

RIAK_HOST = 'http://localhost:8098'
HEADERS_JSON = {'Content-Type': 'application/json'}
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
INDEX_NAME = "idx_poblacion"
MONGO_URI = "mongodb+srv://admin:12345@pbd-proyecto.tsbceg9.mongodb.net/?retryWrites=true&w=majority"

try:
    r = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
except:
    r = None

try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db_mongo = mongo_client['practica_db']
except:
    mongo_client = None
    db_mongo = None


class Tracer:
    def __init__(self, db_name):
        self.logs = []
        self.db = db_name.upper()
        self.start_time = time.time()

    def log(self, msg, type="info"):
        emoji = {"info": "ℹ️", "success": "✅", "error": "❌", "warn": "⚠️", "timer": "⏱️", "pubsub": "📢", "net": "🌐",
                 "db": "🗄️"}.get(type, "🔹")
        self.logs.append(f"[{self.db}] {emoji} {msg}")

    def get_execution_time(self):
        return round((time.time() - self.start_time) * 1000, 2)


def check_riak_connection():
    try:
        return requests.get(f"{RIAK_HOST}/ping", timeout=1).status_code == 200
    except:
        return False


def get_keys_riak(bucket):
    try:
        return requests.get(f"{RIAK_HOST}/buckets/{bucket}/keys?keys=true").json().get('keys', [])
    except:
        return []


def get_object_riak(bucket, key):
    try:
        return requests.get(f"{RIAK_HOST}/buckets/{bucket}/keys/{key}").json()
    except:
        return None


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


@app.get("/")
def serve_ui():
    return FileResponse("index.html")


@app.get("/load-data")
def load_data():
    tracer = Tracer("SYSTEM")

    poblacion_unica = list({p['dni']: p for p in poblacion}.values())
    tracer.log(f"Dataset procesado: {len(poblacion_unica)} registros únicos.", "info")

    if check_riak_connection():
        tracer.log("Limpiando Riak...", "warn")
        old_keys = get_keys_riak('poblacion')
        for k in old_keys: delete_object_riak('poblacion', k)

        count = 0
        for p in poblacion_unica:
            idxs = {'ingresos_int': p['ingresos'], 'sector_int': p['sector'], 'sexo_bin': p['sexo']}
            store_object_riak("poblacion", p['dni'], p, idxs)
            count += 1
        tracer.log(f"Riak: Insertados {count} registros.", "success")

    if r:
        try:
            try:
                r.ft(INDEX_NAME).dropindex(True)
            except:
                pass

            keys = r.keys("poblacion:*")
            if keys: r.delete(*keys)

            pipe = r.pipeline()
            for p in poblacion_unica: pipe.json().set(f"poblacion:{p['dni']}", "$", p)
            pipe.execute()

            schema = (TextField("$.nombre", as_name="nombre"), NumericField("$.ingresos", as_name="ingresos"),
                      NumericField("$.sector", as_name="sector"), TextField("$.sexo", as_name="sexo"))
            r.ft(INDEX_NAME).create_index(schema,
                                          definition=IndexDefinition(prefix=["poblacion:"], index_type=IndexType.JSON))
            tracer.log(f"Redis: Insertados e indexados {len(poblacion_unica)} registros.", "success")
        except Exception as e:
            tracer.log(f"Redis Err: {e}", "error")

    if db_mongo is not None:
        try:
            db_mongo.poblacion.delete_many({})
            poblacion_mongo = []
            for p in poblacion_unica:
                p_c = p.copy()
                p_c['_id'] = p_c['dni']
                if 'fechanac' in p_c and isinstance(p_c['fechanac'], str):
                    try:
                        p_c['fechanac'] = datetime.strptime(p_c['fechanac'], '%Y-%m-%d')
                    except:
                        pass
                poblacion_mongo.append(p_c)

            if poblacion_mongo:
                db_mongo.poblacion.insert_many(poblacion_mongo, ordered=False)

            db_mongo.poblacion.create_index([("ingresos", ASCENDING)])
            db_mongo.poblacion.create_index([("sector", ASCENDING), ("sexo", ASCENDING)])
            tracer.log(f"Mongo: Insertados {len(poblacion_mongo)} documentos.", "success")
        except Exception as e:
            tracer.log(f"Mongo Err: {e}", "error")

    return {"trace": tracer.logs, "msg": f"Carga sincronizada: {len(poblacion_unica)} registros en las 3 BDs"}


@app.get("/execute/{operation}/{db}")
def execute_operation(operation: str, db: str, dni: str = Query("123456789"), nombre: str = Query("Laura"),
                      ingresos: int = Query(30000), min_val: int = Query(20000), max_val: int = Query(50000),
                      sector: int = Query(2), sexo: str = Query("M")):
    tracer = Tracer(db)
    result_data = {}
    executed_query = ""

    if operation == "listar":
        tracer.log(f"Listando TODO el contenido de {db}", "info")
        if db == "riak":
            executed_query = f"requests.get('{RIAK_HOST}/buckets/poblacion/keys?keys=true')\n# Iterar keys y hacer GET por cada una"
            keys = get_keys_riak('poblacion')
            result_data = [get_object_riak('poblacion', k) for k in keys]
            tracer.log(f"Riak: {len(result_data)} objetos recuperados.", "success")
        elif db == "redis":
            executed_query = 'keys = r.keys("poblacion:*")\n# Por cada clave:\nregistro = r.json().get(k)'
            keys = r.keys("poblacion:*")
            if keys:
                result_data = r.json().mget(keys, "$")
                if result_data: result_data = [d[0] for d in result_data if d]
            tracer.log(f"Redis: {len(result_data) if result_data else 0} objetos recuperados.", "success")
        elif db == "mongo":
            executed_query = "cursor = db.poblacion.find({})"
            cursor = db_mongo.poblacion.find({})
            result_data = [serialize_mongo(d) for d in cursor]
            tracer.log(f"Mongo: {len(result_data)} documentos recuperados.", "success")

    elif operation == "leer":
        if db == "riak":
            executed_query = f"requests.get('{RIAK_HOST}/buckets/poblacion/keys/{dni}')"
            result_data = get_object_riak('poblacion', dni)
        elif db == "redis":
            executed_query = f"r.json().get('poblacion:{dni}')"
            result_data = r.json().get(f"poblacion:{dni}")
        elif db == "mongo":
            executed_query = f"db.poblacion.find_one({{'_id': '{dni}'}})"
            result_data = serialize_mongo(db_mongo.poblacion.find_one({"_id": dni}))

        if result_data:
            tracer.log("Registro encontrado.", "success")
        else:
            tracer.log("No encontrado.", "error")

    elif operation == "insertar":
        nuevo_p = {"dni": dni, "nombre": nombre, "ingresos": ingresos, "sector": sector, "sexo": sexo}
        if db == "riak":
            executed_query = (f"url = '{RIAK_HOST}/buckets/poblacion/keys/{dni}'\n"
                              f"headers = {{'x-riak-index-ingresos_int': {ingresos}, ...}}\n"
                              f"requests.put(url, data=json.dumps(p), headers=headers)")
            idxs = {'ingresos_int': ingresos, 'sector_int': sector, 'sexo_bin': sexo}
            store_object_riak('poblacion', dni, nuevo_p, idxs)
            result_data = nuevo_p
        elif db == "redis":
            executed_query = (f"r.json().set('poblacion:{dni}', '$', p)\n"
                              f"r.publish('nueva_persona', 'Se ha unido {nombre}')")
            r.json().set(f"poblacion:{dni}", "$", nuevo_p)
            r.publish("nueva_persona", f"Se ha unido {nombre}")
            result_data = nuevo_p
        elif db == "mongo":
            executed_query = f"db.poblacion.insert_one({{...}})"
            try:
                p = nuevo_p.copy();
                p['_id'] = dni
                db_mongo.poblacion.insert_one(p)
                result_data = nuevo_p
            except DuplicateKeyError:
                tracer.log("Duplicado (Ya existe)", "error")

    elif operation == "actualizar":
        nuevo_ing = ingresos + 1000
        if db == "riak":
            executed_query = (f"# 1. GET objeto\n"
                              f"# 2. Modificar en memoria\n"
                              f"requests.put('{RIAK_HOST}/buckets/poblacion/keys/{dni}', ...)")
            curr = get_object_riak('poblacion', dni)
            if curr:
                curr['ingresos'] = nuevo_ing
                idxs = {'ingresos_int': nuevo_ing, 'sector_int': curr.get('sector'), 'sexo_bin': curr.get('sexo')}
                store_object_riak('poblacion', dni, curr, idxs)
                result_data = curr
        elif db == "redis":
            executed_query = f"r.json().set('poblacion:{dni}', '$.ingresos', {nuevo_ing})"
            r.json().set(f"poblacion:{dni}", "$.ingresos", nuevo_ing)
            result_data = {"msg": "Actualizado"}
        elif db == "mongo":
            executed_query = f"db.poblacion.update_one({{'_id': '{dni}'}}, {{'$set': {{'ingresos': {nuevo_ing}}}}})"
            db_mongo.poblacion.update_one({"_id": dni}, {"$set": {"ingresos": nuevo_ing}})
            result_data = {"msg": "Actualizado"}

    elif operation == "borrar":
        if db == "riak":
            executed_query = f"requests.delete('{RIAK_HOST}/buckets/poblacion/keys/{dni}')"
            delete_object_riak('poblacion', dni)
        elif db == "redis":
            executed_query = f"r.delete('poblacion:{dni}')"
            r.delete(f"poblacion:{dni}")
        elif db == "mongo":
            executed_query = f"db.poblacion.delete_one({{'_id': '{dni}'}})"
            db_mongo.poblacion.delete_one({"_id": dni})
        tracer.log(f"Operación de borrado enviada.", "success")

    elif operation == "indexar":
        if db == "riak":
            executed_query = ("# Recorrer todas las keys\n"
                              "headers = {'x-riak-index-ingresos_int': ...}\n"
                              "requests.put(url, data=d, headers=headers)")
            tracer.log("Riak: Iniciando re-indexado (Map manual)...", "info")
            keys = get_keys_riak('poblacion')
            count = 0
            for k in keys:
                obj = get_object_riak('poblacion', k)
                if obj:
                    idxs = {'ingresos_int': obj.get('ingresos'), 'sector_int': obj.get('sector'),
                            'sexo_bin': obj.get('sexo')}
                    store_object_riak('poblacion', k, obj, idxs)
                    count += 1
            tracer.log(f"Riak: Índices regenerados en {count} objetos.", "success")
            result_data = {"msg": f"Reindexado Riak OK ({count} docs)"}

        elif db == "redis":
            executed_query = ("schema = (TextField('$.nombre'), NumericField('$.ingresos')...)\n"
                              "r.ft('idx_poblacion').create_index(schema, ...)")
            tracer.log("Redis: Verificando índice...", "info")
            try:
                schema = (TextField("$.nombre", as_name="nombre"), NumericField("$.ingresos", as_name="ingresos"),
                          NumericField("$.sector", as_name="sector"), TextField("$.sexo", as_name="sexo"))
                r.ft(INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=["poblacion:"],
                                                                                 index_type=IndexType.JSON))
                tracer.log("Redis: Índice creado.", "success")
            except:
                tracer.log("Redis Info: Índice ya existía.", "warn")

            info = r.ft(INDEX_NAME).info()
            result_data = {"docs_indexed": info['num_docs'], "index_name": INDEX_NAME}

        elif db == "mongo":
            executed_query = ("db.poblacion.create_index([('ingresos', ASCENDING)])\n"
                              "db.poblacion.create_index([('sector', ASCENDING), ('sexo', ASCENDING)])")
            tracer.log("Mongo: Ejecutando create_index()...", "info")
            db_mongo.poblacion.create_index([("ingresos", ASCENDING)])
            db_mongo.poblacion.create_index([("sector", ASCENDING), ("sexo", ASCENDING)])
            tracer.log("Mongo: Índices B-Tree asegurados.", "success")
            result_data = {"msg": "Indices Mongo creados/verificados"}

    elif operation == "rango":
        tracer.log(f"Buscando ingresos [{min_val} - {max_val}]", "info")
        if db == "riak":
            executed_query = f"requests.get('{RIAK_HOST}/buckets/poblacion/index/ingresos_int/{min_val}/{max_val}')"
            keys = query_2i_range_riak('poblacion', 'ingresos_int', min_val, max_val)
            result_data = [get_object_riak('poblacion', k) for k in keys]
        elif db == "redis":
            executed_query = (f"q = Query('@ingresos:[{min_val} {max_val}]')\n"
                              f"res = r.ft('{INDEX_NAME}').search(q)")
            q = RedisQuery(f"@ingresos:[{min_val} {max_val}]").paging(0, 10000)
            res = r.ft(INDEX_NAME).search(q)
            result_data = [json.loads(d.json) for d in res.docs]
        elif db == "mongo":
            executed_query = f"db.poblacion.find({{'ingresos': {{'$gte': {min_val}, '$lte': {max_val}}}}})"
            q = {"ingresos": {"$gte": min_val, "$lte": max_val}}
            result_data = [serialize_mongo(d) for d in db_mongo.poblacion.find(q)]
        tracer.log(f"Encontrados: {len(result_data)}", "success")

    elif operation == "filtro":
        if db == "riak":
            executed_query = (f"keys = requests.get('.../index/sector_int/{sector}').json()\n"
                              f"# Filtrado en Python por sexo == '{sexo}'")
            keys = query_2i_riak('poblacion', 'sector_int', sector)
            temp = [get_object_riak('poblacion', k) for k in keys]
            result_data = [d for d in temp if d and d.get('sexo') == sexo]
        elif db == "redis":
            executed_query = (f"q = Query('@sector:[{sector} {sector}] @sexo:{sexo}')\n"
                              f"res = r.ft('{INDEX_NAME}').search(q)")
            q = RedisQuery(f"@sector:[{sector} {sector}] @sexo:{sexo}")
            res = r.ft(INDEX_NAME).search(q)
            result_data = [json.loads(d.json) for d in res.docs]
        elif db == "mongo":
            executed_query = f"db.poblacion.find({{'sector': {sector}, 'sexo': '{sexo}'}})"
            q = {"$or": [{"sector": sector}, {"sector_id": sector}], "sexo": sexo}
            result_data = [serialize_mongo(d) for d in db_mongo.poblacion.find(q)]

    elif operation == "agregacion":
        if db == "riak":
            executed_query = ("# Manual MapReduce en Python:\n"
                              "keys = get_keys()\n"
                              "total = sum(d['ingresos'] for d in data if d['sector'] == ...)")
            keys = get_keys_riak('poblacion')
            total = sum([get_object_riak('poblacion', k).get('ingresos', 0) for k in keys if
                         str(get_object_riak('poblacion', k).get('sector')) == str(sector)])
            result_data = {"sector": sector, "total": total}
        elif db == "redis":
            executed_query = ("# Scan & Sum en Python:\n"
                              "keys = r.keys('poblacion:*')\n"
                              "total += r.json().get(k)['ingresos']")
            keys = r.keys("poblacion:*")
            total = 0
            for k in keys:
                d = r.json().get(k)
                if str(d.get('sector')) == str(sector): total += d.get('ingresos', 0)
            result_data = {"sector": sector, "total": total}
        elif db == "mongo":
            executed_query = (f"pipeline = [\n"
                              f"  {{'$match': {{'sector': {sector}}}}},\n"
                              f"  {{'$group': {{'_id': '$sector', 'total': {{'$sum': '$ingresos'}}}}}}\n"
                              f"]\n"
                              f"db.poblacion.aggregate(pipeline)")
            pipeline = [{"$match": {"sector": sector}}, {"$group": {"_id": "$sector", "total": {"$sum": "$ingresos"}}}]
            res = list(db_mongo.poblacion.aggregate(pipeline))
            result_data = [{"sector": r['_id'], "total": r['total']} for r in res]

    exec_time = tracer.get_execution_time()
    return {"trace": tracer.logs, "data": result_data, "time_ms": exec_time, "query_str": executed_query}