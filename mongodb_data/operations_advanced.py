import pymongo
from datetime import datetime

# ------------------------------
# Crear índices para optimización
# ------------------------------
def crear_indices_avanzados(db):
    """
    Genera índices B-Tree: uno simple para rangos numéricos y 
    uno compuesto para filtros de igualdad múltiple.
    """
    try:
        # Índice simple para el campo 'ingresos'
        db.poblacion.create_index([("ingresos", pymongo.ASCENDING)])
        
        # Índice compuesto
        db.poblacion.create_index([("sector_id", pymongo.ASCENDING), ("sexo", pymongo.ASCENDING)])
        
        print("[ÍNDICES] Optimizados correctamente en la colección 'poblacion'.")
    except Exception as e:
        print(f"Error gestionando índices: {e}")

# ------------------------------
# Función: Buscar personas por rango de ingresos
# ------------------------------
def buscar_por_ingresos(db, min_ing=0, max_ing=100000):
    query = {"ingresos": {"$gte": min_ing, "$lte": max_ing}}

    projection = {"_id": 1, "nombre": 1, "ingresos": 1, "sexo": 1}
    
    res = db.poblacion.find(query, projection)
    
    print(f"\n[CONSULTA] Personas con ingresos entre {min_ing} y {max_ing}:")
    for doc in res:
        print(f"   - {doc['_id']} | {doc['nombre']} | {doc['ingresos']}€ | {doc['sexo']}")

# ------------------------------
# Función: Filtrar por sector y sexo
# ------------------------------
def filtrar_por_sector_sexo(db, sector_id, sexo):
    query = {"sector_id": sector_id, "sexo": sexo}
    res = db.poblacion.find(query)
    
    print(f"\n[FILTRO] Personas del sector {sector_id} y sexo '{sexo}':")
    for doc in res:
        print(f"   - {doc['_id']} | {doc['nombre']} | {doc['sexo']}")

# ------------------------------
# Función: Agregación (Map-Reduce moderno) y persistencia
# ------------------------------
def guardar_resumen_sector(db):
    pipeline = [
        {
            "$group": {
                "_id": "$sector_id",                    # Group By sector
                "totalIngresos": {"$sum": "$ingresos"}  # Sum() acumulativo
            }
        },
        {"$sort": {"_id": 1}} 
    ]
    
    # Ejecución en el servidor de base de datos
    resultados = list(db.poblacion.aggregate(pipeline))
    
    # Persistencia de datos
    db.resumen_sector.delete_many({}) 
    if resultados:
        db.resumen_sector.insert_many(resultados)
    
    print("\n[AGREGACIÓN] Resumen guardado en colección 'resumen_sector':")
    for r in resultados:
        print(f"   - Sector {r['_id']}: {r['totalIngresos']}€")

# ------------------------------
# Función: Insertar persona (Disparador en Atlas)
# ------------------------------
def insertar_persona_trigger(db, p):
    try:
        if isinstance(p['fechanac'], str):
            p['fechanac'] = datetime.strptime(p['fechanac'], '%Y-%m-%d')

        db.poblacion.insert_one(p)
        
        print(f"\n[INSERT] Persona {p['_id']} guardada.")
        print("La lógica reactiva (Trigger) se gestiona externamente en MongoDB Atlas.")
        
    except pymongo.errors.DuplicateKeyError:
        print(f"\nError: Duplicado. Ya existe el DNI {p['_id']}.")


def ejecutar_pruebas_avanzadas(db):
    print("\n--- INICIO DE PRUEBAS MONGODB ---")
    
    crear_indices_avanzados(db)
    
    buscar_por_ingresos(db, 20000, 50000)
    
    filtrar_por_sector_sexo(db, 1, "H") 
    
    guardar_resumen_sector(db)
    
    nueva_persona = {
        "_id": "999888777",
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
        "sector_id": 2
    }
    insertar_persona_trigger(db, nueva_persona)