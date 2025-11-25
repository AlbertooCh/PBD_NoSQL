import time
import json
import threading

from redis import Redis
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.query import Query
from redis.commands.json.path import Path
from redis.commands.search.index_definition import IndexDefinition, IndexType


# --- CONFIGURACIÓN ---
r = Redis(host='localhost', port=6379, decode_responses=True)
INDEX_NAME = "idx_poblacion"

try:
    r.ft(INDEX_NAME).dropindex(delete_documents=False)
    print(f"Índice '{INDEX_NAME}' limpiado.")
except Exception:
    pass

try:
    schema = (
        TextField("$.nombre", as_name="nombre"),
        TextField("$.apellido1", as_name="apellido1"),
        TextField("$.apellido2", as_name="apellido2"),
        NumericField("$.ingresos", as_name="ingresos"),
        NumericField("$.sector", as_name="sector"),
        TextField("$.sexo", as_name="sexo")
    )
    r.ft(INDEX_NAME).create_index(
        schema,
        definition=IndexDefinition(prefix=["poblacion:"], index_type=IndexType.JSON)
    )
    print("Índice creado correctamente.")
except Exception as e:
    print(f"Error índice: {e}")

while True:
    try:
        info = r.ft(INDEX_NAME).info()
        if int(info.get('indexing', 0)) == 0:
            break
        time.sleep(0.1)
    except:
        break


# ==========================================
# 2. FUNCIONES DE BÚSQUEDA Y ANÁLISIS
# ==========================================

def buscar_por_ingresos(min_ing, max_ing):
    print(f"\n[Búsqueda] Ingresos entre {min_ing} y {max_ing}...")
    q = Query(f"@ingresos:[{min_ing} {max_ing}]").paging(0, 100)

    try:
        res = r.ft(INDEX_NAME).search(q)
        print(f"Encontrados: {res.total}")
        print("-" * 50)
        print(f"{'NOMBRE':<12} | {'INGRESOS':>10} | {'SEXO':^4}")
        print("-" * 50)

        for doc in res.docs:
            d = json.loads(doc.json)
            print(f"{d.get('nombre', 'N/A'):<12} | {d.get('ingresos', 0):>10} | {d.get('sexo', '?'):^4}")
        print("-" * 50)
    except Exception as e:
        print(f"Error: {e}")


def filtrar_por_sector_sexo(sector, sexo):
    print(f"\n[Filtro] Sector {sector} y Sexo {sexo}...")
    q = Query(f"@sector:[{sector} {sector}] @sexo:{sexo}").paging(0, 100)
    res = r.ft(INDEX_NAME).search(q)

    print("-" * 50)
    for doc in res.docs:
        d = json.loads(doc.json)
        print(f"ID: {doc.id.split(':')[-1]} | Nombre: {d.get('nombre')} | Sector: {d.get('sector')}")
    print("-" * 50)


def guardar_resumen_sector():
    print("\n[Agregación] Calculando total ingresos por sector...")
    keys = r.keys("poblacion:*")
    resumen = {}

    for k in keys:
        p = r.json().get(k)
        if p:
            s = p.get('sector')
            resumen[s] = resumen.get(s, 0) + p.get('ingresos', 0)

    r.delete("resumen_sector")
    for s, total in resumen.items():
        r.hset("resumen_sector", str(s), total)

    print("Hash 'resumen_sector' actualizado:")
    print(r.hgetall("resumen_sector"))

# ==========================================
# 3. PUB/SUB (EVENTOS EN TIEMPO REAL)
# ==========================================

def escuchar_canal():
    p = r.pubsub()
    p.subscribe('nueva_persona')

    print("\n[Listener] Escuchando canal 'nueva_persona' (esperando mensajes)...")

    count = 0
    for message in p.listen():
        if message['type'] == 'message':
            print(f"¡ALERTA RECIBIDA!: {message['data']}")
            count += 1
            if count >= 1: break


def insertar_persona_pubsub(p):
    key = f"poblacion:{p['dni']}"

    r.json().set(key, Path.root_path(), p)

    mensaje = f"Se ha unido {p['nombre']} {p['apellido1']} (Sector {p['sector']})"
    r.publish("nueva_persona", mensaje)
    print(f"\n[Insert] {p['nombre']} guardado en base de datos.")


# ==========================================
# 4. EJECUCIÓN DEL FLUJO
# ==========================================
if __name__ == "__main__":
    buscar_por_ingresos(20000, 50000)
    filtrar_por_sector_sexo(1, "H")
    guardar_resumen_sector()

    print("\n--- INICIANDO PUB/SUB ---")

    # 1. Arrancamos el "oyente" en un hilo aparte para que no bloquee
    hilo_escucha = threading.Thread(target=escuchar_canal)
    hilo_escucha.start()

    time.sleep(1)

    # 2. Definimos una persona nueva
    nueva_persona = {
        "dni": "999888777",
        "nombre": "Miguel",
        "apellido1": "García",
        "apellido2": "López",
        "fechanac": "1985-06-10",
        "direccion": "C. Nueva", "cp": "06000",
        "sexo": "H",
        "ingresos": 45000,
        "gastosFijos": 8000, "gastosAlim": 5000, "gastosRopa": 2000,
        "sector": 2
    }

    # 3. Insertamos (Esto disparará el mensaje que el hilo escuchará)
    insertar_persona_pubsub(nueva_persona)

    # Esperamos a que el hilo termine
    hilo_escucha.join()
    print("\nProceso finalizado.")