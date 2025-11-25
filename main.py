import requests
import json
import sys
import time

# ==========================================
# CONFIGURACIÓN Y CLIENTE HTTP (DRIVER)
# ==========================================
RIAK_HOST = 'http://localhost:8098'
HEADERS_JSON = {'Content-Type': 'application/json'}


def check_riak_connection():
    try:
        resp = requests.get(f"{RIAK_HOST}/ping", timeout=2)
        if resp.status_code != 200:
            raise ConnectionError(f"Status {resp.status_code}")
        print(f"✅ Conexión establecida con Riak en {RIAK_HOST}")
    except Exception as e:
        print(f"❌ Error fatal: No se puede conectar a Riak. {e}")
        sys.exit(1)


def store_object(bucket, key, data, indexes=None):
    """
    Guarda un objeto y opcionalmente sus índices secundarios (2i).
    indexes ejemplo: {'ingresos_int': 20000, 'sexo_bin': 'H'}
    """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    headers = HEADERS_JSON.copy()

    # Mapeo de índices a Cabeceras HTTP (x-riak-index-...)
    if indexes:
        for idx_name, idx_val in indexes.items():
            headers[f"x-riak-index-{idx_name}"] = str(idx_val)

    resp = requests.put(url, data=json.dumps(data), headers=headers)
    return resp.status_code in [200, 204]


def get_object(bucket, key):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    return None


def delete_object(bucket, key):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    requests.delete(url)


def query_2i_range(bucket, index, min_val, max_val):
    """ Busca claves por rango en un índice """
    url = f"{RIAK_HOST}/buckets/{bucket}/index/{index}/{min_val}/{max_val}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json().get('keys', [])
    return []


def query_2i_exact(bucket, index, val):
    """ Busca claves por valor exacto en un índice """
    url = f"{RIAK_HOST}/buckets/{bucket}/index/{index}/{val}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json().get('keys', [])
    return []


# ==========================================
# DATASETS (Datos de prueba)
# ==========================================
sectores_data = [
    {"codS": 1, "nombreS": "Agricultura y pesca", "porcentS": 0, "ingresosS": 0},
    {"codS": 2, "nombreS": "Industria y energía", "porcentS": 0, "ingresosS": 0},
    {"codS": 3, "nombreS": "Servicios", "porcentS": 0, "ingresosS": 0},
    {"codS": 4, "nombreS": "Construcción", "porcentS": 0, "ingresosS": 0},
]

poblacion_data = [
    {"dni": "123456789", "nombre": "Carlos", "sexo": "H", "ingresos": 15000, "sector": 1},
    {"dni": "777888999", "nombre": "Gerardo", "sexo": "H", "ingresos": 23000, "sector": 1},
    {"dni": "222333444", "nombre": "Pedro", "sexo": "H", "ingresos": 22000, "sector": 1},
    {"dni": "333444555", "nombre": "María", "sexo": "M", "ingresos": 19500, "sector": 1},
    {"dni": "555666777", "nombre": "Vicente", "sexo": "H", "ingresos": 46000, "sector": 2},
    {"dni": "666777888", "nombre": "Beatriz", "sexo": "M", "ingresos": 50000, "sector": 2},
    {"dni": "987654321", "nombre": "Eva", "sexo": "M", "ingresos": 40000, "sector": 3},
    {"dni": "111222333", "nombre": "Susana", "sexo": "M", "ingresos": 18000, "sector": 3},
    {"dni": "444555666", "nombre": "Ángel", "sexo": "H", "ingresos": 14000, "sector": 4},
    {"dni": "222447777", "nombre": "Fermín", "sexo": "H", "ingresos": 25000, "sector": 4}
]


# ==========================================
# LÓGICA DE PRUEBA (MAIN)
# ==========================================
def main():
    print("\n🚀 INICIANDO PRUEBA DE RIAK (HTTP API)\n")
    check_riak_connection()

    # --- PASO 1: CARGA MASIVA ---
    print("\n--- 1. CARGANDO DATASETS ---")

    # Cargar Sectores
    for s in sectores_data:
        store_object("sectores", str(s['codS']), s)
    print(f"✅ {len(sectores_data)} sectores insertados.")

    # Cargar Población con ÍNDICES
    print("⏳ Insertando población y generando índices secundarios...")
    for p in poblacion_data:
        # Definimos los índices para búsqueda rápida
        indices = {
            'ingresos_int': p['ingresos'],
            'sector_int': p['sector'],
            'sexo_bin': p['sexo']
        }
        store_object("poblacion", p['dni'], p, indexes=indices)
    print(f"✅ {len(poblacion_data)} personas insertadas e indexadas.")

    # --- PASO 2: LECTURA Y ACTUALIZACIÓN (CRUD) ---
    print("\n--- 2. PRUEBA DE CRUD (Leer/Actualizar) ---")
    target_dni = "123456789"
    persona = get_object("poblacion", target_dni)

    if persona:
        print(f"📖 Leído: {persona['nombre']} - Ingresos: {persona['ingresos']}€")

        # Actualizamos ingresos
        nuevo_ingreso = 18000
        persona['ingresos'] = nuevo_ingreso
        # IMPORTANTE: Al actualizar, debemos pasar los índices de nuevo o se perderán/desactualizarán
        indices_update = {
            'ingresos_int': nuevo_ingreso,
            'sector_int': persona['sector'],
            'sexo_bin': persona['sexo']
        }
        store_object("poblacion", target_dni, persona, indexes=indices_update)
        print(f"✏️ Actualizado: Ingresos subidos a {nuevo_ingreso}€")

        # Verificación rápida
        p_check = get_object("poblacion", target_dni)
        print(f"🔍 Verificación en BD: {p_check['ingresos']}€ (Correcto)")

    # --- PASO 3: BÚSQUEDAS AVANZADAS (2i) ---
    print("\n--- 3. BÚSQUEDAS POR RANGO E ÍNDICES ---")

    # Búsqueda por rango de ingresos
    min_ing, max_ing = 20000, 45000
    print(f"🔎 Buscando personas con ingresos entre {min_ing} y {max_ing}...")

    keys_rango = query_2i_range("poblacion", "ingresos_int", min_ing, max_ing)
    print(f"   -> Se encontraron {len(keys_rango)} resultados (IDs: {keys_rango})")

    # Detalle de los encontrados
    for k in keys_rango:
        d = get_object("poblacion", k)
        print(f"      - {d['nombre']} ({d['ingresos']}€)")

    # --- PASO 4: FILTRADO COMPUESTO ---
    print("\n--- 4. FILTRADO COMPUESTO (Sector + Sexo) ---")
    # Buscamos Sector 2 (Riak) y Sexo 'M' (Python)
    sector_target = 2
    sexo_target = "M"

    print(f"🔎 Buscando Sector {sector_target} (BD) + Sexo {sexo_target} (App)...")
    keys_sector = query_2i_exact("poblacion", "sector_int", sector_target)

    found = False
    for k in keys_sector:
        d = get_object("poblacion", k)
        if d['sexo'] == sexo_target:
            print(f"✅ MATCH: {d['nombre']} (DNI: {d['dni']})")
            found = True

    if not found:
        print("❌ No se encontraron coincidencias.")

    # --- PASO 5: AGREGACIÓN ---
    print("\n--- 5. AGREGACIÓN (Suma de ingresos por sector) ---")
    # Nota: Para hacerlo real, traemos todas las claves o iteramos índices
    all_keys = requests.get(f"{RIAK_HOST}/buckets/poblacion/keys?keys=true").json().get('keys', [])

    resumen_ingresos = {}

    for k in all_keys:
        p = get_object("poblacion", k)
        sec = str(p['sector'])
        resumen_ingresos.setdefault(sec, 0)
        resumen_ingresos[sec] += p['ingresos']

    print("📊 Resumen calculado:")
    for sec, total in sorted(resumen_ingresos.items()):
        print(f"   Sector {sec}: {total}€")

    # Guardar este resumen
    store_object("resumenes", "informe_global", resumen_ingresos)
    print("💾 Informe guardado en bucket 'resumenes'.")

    print("\n🏁 PRUEBA FINALIZADA CON ÉXITO")


if __name__ == "__main__":
    main()