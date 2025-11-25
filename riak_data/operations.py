import requests
import json
import sys

# --- CONFIGURACIÓN DE CONEXIÓN ---
RIAK_HOST = 'http://localhost:8098'
HEADERS = {'Content-Type': 'application/json'}


def check_connection():
    try:
        response = requests.get(f"{RIAK_HOST}/ping")
        if response.status_code != 200:
            raise ConnectionError("No se recibió 200 OK del ping")
        print("Conexión exitosa a Riak (HTTP)")
    except Exception as e:
        print(f"Error de conexión: {e}")
        sys.exit(1)


# --- FUNCIONES AUXILIARES () ---

def get_keys(bucket):
    """Equivalente a bucket.get_keys() """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys?keys=true"
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        return data.get('keys', [])
    return []


def get_object(bucket, key):
    """ Equivalente a bucket.get(key) """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    return None


def store_object(bucket, key, data):
    """ Equivalente a object.store() """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    resp = requests.put(url, data=json.dumps(data), headers=HEADERS)
    return resp.status_code in [200, 204]


def delete_object(bucket, key):
    """ Equivalente a bucket.delete(key) """
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    resp = requests.delete(url)
    return resp.status_code in [204, 404]


# ------------------------------
# INICIO DEL SCRIPT
# ------------------------------

check_connection()

# ------------------------------
# OPERACIONES DE CONSULTA
# ------------------------------

# --- Listar todos los registros de Población ---
print("\nRecuperando claves de población...")
keys_pob = get_keys('poblacion')

print(f"Lista completa de población ({len(keys_pob)} registros):")
for k in keys_pob:
    obj_data = get_object('poblacion', k)
    if obj_data:
        print(obj_data)

# --- Listar y ordenar Sectores ---
print("\nRecuperando claves de sectores...")
keys_sec = get_keys('sectores')

sectores = []
for k in keys_sec:
    obj_data = get_object('sectores', k)
    if obj_data:
        sectores.append(obj_data)

# --- Ordenar por codS ---
sectores_ordenados = sorted(sectores, key=lambda x: x['codS'])

print("Lista completa de sectores ordenada por codS:")
for s in sectores_ordenados:
    print(s)

# --- Leer un registro específico ---
dni_consulta = "123456789"
print(f"\nConsultando DNI: {dni_consulta}")
obj_consulta = get_object('poblacion', dni_consulta)
print(obj_consulta)

# --- Leer un campo concreto ---
print(f"Ingresos del DNI {dni_consulta}:")
if obj_consulta:
    print(obj_consulta.get('ingresos'))

# ------------------------------
# OPERACIONES DE INSERCIÓN
# ------------------------------
nuevo_p = {
    "dni": "555888999",
    "nombre": "Laura",
    "apellido1": "López",
    "apellido2": "Ramírez",
    "fechanac": "1990-08-15",
    "direccion": "C. Nueva",
    "cp": "06001",
    "sexo": "M",
    "ingresos": 30000,
    "gastosFijos": 7000,
    "gastosAlim": 4000,
    "gastosRopa": 2000,
    "sector": 2
}

# Creamos un nuevo objeto en el bucket
new_key = nuevo_p['dni']
if store_object('poblacion', new_key, nuevo_p):
    print(f"\nInsertado nuevo registro: {new_key}")
else:
    print(f"\nError al insertar {new_key}")

# ------------------------------
# OPERACIONES DE ACTUALIZACIÓN
# ------------------------------
# Fetch -> Modify -> Store
dni_actualizar = "555888999"
print(f"\nIntentando actualizar {dni_actualizar}...")

# 1. Obtenemos los datos actuales (GET)
datos_actuales = get_object('poblacion', dni_actualizar)

if datos_actuales:
    # 2. Modificamos los campos (en memoria Python)
    datos_actuales['ingresos'] = 16000
    datos_actuales['direccion'] = "C. Actualizada"

    # 3. Guardamos los datos modificados (PUT)
    if store_object('poblacion', dni_actualizar, datos_actuales):
        print(f"Registro {dni_actualizar} actualizado correctamente")
        print("Dato actualizado:", get_object('poblacion', dni_actualizar))
else:
    print(f"No se encontró el registro {dni_actualizar} para actualizar")

# ------------------------------
# BORRAR UN REGISTRO
# ------------------------------
dni_borrar = "555888999"
if delete_object('poblacion', dni_borrar):
    print(f"\nRegistro {dni_borrar} eliminado")
else:
    print(f"\nError al eliminar {dni_borrar}")