from redis import Redis
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.query import Query

# --- Conexión ---
r = Redis(host='localhost', port=6379, decode_responses=True)

# ------------------------------
# Crear índice para búsqueda avanzada
# ------------------------------
try:
    r.ft("idx_poblacion").create_index(
        fields=[
            TextField("$.nombre", as_name="nombre"),
            TextField("$.apellido1", as_name="apellido1"),
            TextField("$.apellido2", as_name="apellido2"),
            NumericField("$.ingresos", as_name="ingresos"),
            NumericField("$.sector", as_name="sector"),
            TextField("$.sexo", as_name="sexo")
        ],
    )
    print("Índice 'idx_poblacion' creado.")
except Exception as e:
    print("Índice ya existe o error:", e)

# ------------------------------
# Función: Buscar personas por rango de ingresos
# ------------------------------
def buscar_por_ingresos(min_ing=0, max_ing=100000):
    q = Query(f"@ingresos:[{min_ing} {max_ing}]")
    res = r.ft("idx_poblacion").search(q)
    print(f"\nPersonas con ingresos entre {min_ing} y {max_ing}:")
    for doc in res.docs:
        print(doc.id, doc.nombre, doc.ingresos, doc.sexo)
#
# # ------------------------------
# # Función: Filtrar por sector y sexo
# # ------------------------------
# def filtrar_por_sector_sexo(sector, sexo):
#     q = Query(f"@sector:[{sector} {sector}] @sexo:{sexo}")
#     res = r.ft("idx_poblacion").search(q)
#     print(f"\nPersonas del sector {sector} y sexo {sexo}:")
#     for doc in res.docs:
#         print(doc.id, doc.nombre, doc.sexo, doc.sector)
#
# # ------------------------------
# # Función: Guardar un hash de resumen por sector
# # ------------------------------
# def guardar_resumen_sector():
#     keys = r.keys("poblacion:*")
#     resumen = {}
#     for k in keys:
#         p = r.json().get(k)
#         s = p['sector']
#         resumen.setdefault(s, 0)
#         resumen[s] += p['ingresos']
#
#     # almacenar resumen en hash
#     for s, total in resumen.items():
#         r.hset("resumen_sector", s, total)
#
#     print("\nResumen por sector guardado en hash 'resumen_sector':")
#     print(r.hgetall("resumen_sector"))
#
# # ------------------------------
# # Función: Publicar evento (Pub/Sub) al insertar persona
# # ------------------------------
# def insertar_persona_pubsub(p):
#     r.json().set(f"poblacion:{p['dni']}", Path.root_path(), p)
#     r.publish("nueva_persona", f"{p['dni']} - {p['nombre']} insertada")
#     print(f"\nPersona {p['dni']} insertada y mensaje publicado.")
#
# # ------------------------------
# # EJEMPLOS DE USO
# # ------------------------------
# if __name__ == "__main__":
#     buscar_por_ingresos(20000, 50000)
#     filtrar_por_sector_sexo(1, "H")
#     guardar_resumen_sector()
#
#     nueva = {
#         "dni": "999888777",
#         "nombre": "Miguel",
#         "apellido1": "García",
#         "apellido2": "López",
#         "fechanac": "1985-06-10",
#         "direccion": "C. Ficticia",
#         "cp": "06000",
#         "sexo": "H",
#         "ingresos": 45000,
#         "gastosFijos": 8000,
#         "gastosAlim": 5000,
#         "gastosRopa": 2000,
#         "sector": 2
#     }
#     insertar_persona_pubsub(nueva)
