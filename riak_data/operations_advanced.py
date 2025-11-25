import requests
import json
import sys

# --- CONFIGURACIÓN ---
RIAK_HOST = 'http://localhost:8098'
HEADERS_JSON = {'Content-Type': 'application/json'}


def check_connection():
    try:
        resp = requests.get(f"{RIAK_HOST}/ping")
        if resp.status_code != 200:
            raise ConnectionError("No se recibió 200 OK")
        print("Conexión exitosa a Riak (HTTP)")
    except Exception as e:
        print(f"Error de conexión: {e}")
        sys.exit(1)


# --- FUNCIONES AUXILIARES DE BAJO NIVEL ---

def get_keys(bucket):
    """ Obtiene todas las claves de un bucket  """
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
    indexes debe ser un dict: {'ingresos_int': 20000, 'sexo_bin': 'H'}
    """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"

    # Copiamos las cabeceras base
    headers = HEADERS_JSON.copy()

    # IMPORTANTE: Mapeo de índices a Cabeceras HTTP
    # Ejemplo: 'ingresos_int' -> 'x-riak-index-ingresos_int'
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


# ------------------------------
# LÓGICA DE NEGOCIO
# ------------------------------

def indexar_datos_existentes():
    print("Indexando datos existentes en Riak (Actualizando headers HTTP)...")
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

    print(f"Datos re-indexados correctamente ({count} registros).\n")


def buscar_por_ingresos(min_ing=0, max_ing=100000):
    print(f"\nPersonas con ingresos entre {min_ing} y {max_ing}:")

    # 1. Obtenemos las claves que cumplen el criterio 2i
    keys_found = query_2i_range('poblacion', 'ingresos_int', min_ing, max_ing)

    # 2. Obtenemos los objetos completos
    for k in keys_found:
        d = get_object('poblacion', k)
        if d:
            print(f"{d['dni']} - {d['nombre']} - {d['ingresos']}€ - {d['sexo']}")


def filtrar_por_sector_sexo(sector, sexo):
    print(f"\nPersonas del sector {sector} y sexo {sexo}:")

    # 1. Filtro primario eficiente: Índice de Riak
    keys_sector = query_2i_exact('poblacion', 'sector_int', sector)

    # 2. Filtro secundario: Application Side
    for k in keys_sector:
        d = get_object('poblacion', k)
        if d and d['sexo'] == sexo:
            print(f"{d['dni']} - {d['nombre']} - {d['sexo']} - Sector: {d['sector']}")


def guardar_resumen_sector():
    keys = get_keys('poblacion')
    resumen = {}

    for k in keys:
        p = get_object('poblacion', k)
        if p:
            s = str(p['sector'])
            resumen.setdefault(s, 0)
            resumen[s] += p['ingresos']

    # Guardamos en bucket 'resumenes' sin índices extra
    store_object_with_indexes('resumenes', 'resumen_sector', resumen)

    print("\nResumen por sector guardado en bucket 'resumenes', key 'resumen_sector':")
    print(get_object('resumenes', 'resumen_sector'))


def insertar_persona_pubsub(p):
    key = p['dni']

    # Preparamos los índices para insertarlos ATOMICAMENTE con el objeto
    indices = {
        'ingresos_int': p['ingresos'],
        'sector_int': p['sector'],
        'sexo_bin': p['sexo']
    }

    store_object_with_indexes('poblacion', key, p, indices)

    # Simulación Pub/Sub
    print(f"\n[MOCK PUBSUB] Evento: 'nueva_persona' -> {p['dni']} - {p['nombre']} insertada")


# ------------------------------
# EJECUCIÓN
# ------------------------------
if __name__ == "__main__":
    check_connection()

    # Paso 1: Asegurar índices
    indexar_datos_existentes()

    # Paso 2: Consultas
    buscar_por_ingresos(20000, 50000)
    filtrar_por_sector_sexo(1, "H")

    # Paso 3: Agregación
    guardar_resumen_sector()

    # Paso 4: Inserción nueva
    nueva = {
        "dni": "999888777",
        "nombre": "Miguel",
        "apellido1": "García",
        "apellido2": "López",
        "fechanac": "1985-06-10",
        "direccion": "C. Ficticia",
        "cp": "06000",
        "sexo": "H",
        "ingresos": 45000,
        "gastosFijos": 8000,
        "gastosAlim": 5000,
        "gastosRopa": 2000,
        "sector": 2
    }
    insertar_persona_pubsub(nueva)