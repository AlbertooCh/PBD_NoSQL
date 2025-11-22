import sys
from pymongo import MongoClient
from data_loader import MONGO_URI, cargar_datos_prueba, get_mongo_client

def mostrar_datos_mongo(db):
    """Imprime todos los documentos de la colección."""
    print(f"\n--- LISTA ACTUAL DE PERSONAS ---")
    cursor = db.poblacion.find()
    docs = list(cursor)
    if not docs:
        print("   (Lista vacía)")
    else:
        for doc in docs:
            print(f"   📄 {doc}")

def ejecutar_ejercicios(db):
    print("\n[FASE 2: EJECUCIÓN DE EJERCICIOS]")

    # --- EJERCICIO 1: CONSULTA (Buscar a Carlos) ---
    print("\n🔹 1. Consulta Simple: Buscando a Carlos (ID 123456789)")
    carlos = db.poblacion.find_one({"_id": "123456789"})
    
    if carlos:
        print(f"   Resultado: {carlos['nombre']} encontrado. Ingresos: {carlos['ingresos']}")
    else:
        print("   Resultado: No encontrado.")

    # --- EJERCICIO 2: ELIMINACIÓN (Borrar a Gerardo - Equivalente Riak R4) ---
    print("\n🔹 2. Eliminación: Borrando a Gerardo (ID 777888999)")
    dni_borrar = "777888999"
    
    resultado = db.poblacion.delete_one({"_id": dni_borrar})
    
    if resultado.deleted_count > 0:
        print(f"   ✅ ÉXITO: El usuario {dni_borrar} ha sido eliminado.")
    else:
        print(f"   ⚠️ AVISO: No se encontró al usuario {dni_borrar}.")

def main():
    print("--- INICIANDO SISTEMA ---")

    # 1. Conectar
    client = get_mongo_client()
    if not client: return

    # 2. Cargar Datos iniciales (Resetea la BD)
    cargar_datos_prueba(client)

    # 3. Definir la Base de Datos
    db = client['practica_db']

    # 4. Mostrar estado INICIAL
    mostrar_datos_mongo(db)
    
    # 5. Ejecutar lógica (Consultar Carlos y Borrar Gerardo)
    ejecutar_ejercicios(db)
    
    # 6. Mostrar estado FINAL (Gerardo ya no debe estar)
    mostrar_datos_mongo(db)

    print("\n--- FINALIZADO ---")

if __name__ == "__main__":
    main()