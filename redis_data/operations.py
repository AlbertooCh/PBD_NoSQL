from redis import Redis
from redis.commands.json.path import Path

# --- Conexión a Redis ---
r = Redis(host='localhost', port=6379, decode_responses=True)

# ------------------------------
# OPERACIONES DE CONSULTA
# ------------------------------
keysPob = r.keys("poblacion:*") # Obtener todas las claves de población y sectores

# --- Listar todos los registros ---
print("Lista completa de población:")
for k in keysPob:
    registro = r.json().get(k)
    print(registro)


keysSec = r.keys("sector:*")
sectores = [r.json().get(k) for k in keysSec]

# --- Ordenar por codS ---
sectores_ordenados = sorted(sectores, key=lambda x: x['codS'])

print("Lista completa de sectores ordenada por codS:")
for s in sectores_ordenados:
    print(s)


# Leer un registro
print(r.json().get("poblacion:123456789"))

# Leer un campo concreto
print(r.json().get("poblacion:123456789", "$.ingresos"))


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
r.json().set(f"poblacion:{nuevo_p['dni']}", Path.root_path(), nuevo_p)
print(f"Insertado nuevo registro: {nuevo_p['dni']}")

# ------------------------------
# OPERACIONES DE ACTUALIZACIÓN
# ------------------------------
dni_actualizar = "555888999"
r.json().set(f"poblacion:{dni_actualizar}", "$.ingresos", 16000)
r.json().set(f"poblacion:{dni_actualizar}", "$.direccion", "C. Actualizada")
print(f"Registro {dni_actualizar} actualizado")

# ------------------------------
# BORRAR UN REGISTRO
# ------------------------------
dni_borrar = "555888999"
r.delete(f"poblacion:{dni_borrar}")
print(f"Registro {dni_borrar} eliminado")