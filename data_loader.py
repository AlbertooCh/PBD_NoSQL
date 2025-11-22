from pymongo import MongoClient
import sys

# --- CONFIGURACIÓN ---
MONGO_URI = "mongodb+srv://admin:12345@pbd-proyecto.tsbceg9.mongodb.net/?retryWrites=true&w=majority"

def get_mongo_client():
    """Intenta conectar a MongoDB Atlas."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') # Verificación de conexión
        return client
    except Exception as e:
        print(f"\n🔴 ERROR CRÍTICO MONGO: {e}")
        return None

def cargar_datos_prueba(m_client):
    """Borra todo y carga los 3 usuarios iniciales."""
    if m_client:
        try:
            db = m_client['practica_db']
            
            # 1. Limpiamos la colección para empezar de cero
            db.poblacion.delete_many({})

            # 2. Datos iniciales (Carlos, Gerardo, Ana)
            poblacion_data = [
                {"_id": "123456789", "nombre": "Carlos", "ingresos": 25000, "sector_id": 1},
                {"_id": "777888999", "nombre": "Gerardo", "ingresos": 18000, "sector_id": 2},
                {"_id": "111222333", "nombre": "Ana", "ingresos": 30000, "sector_id": 1}
            ]
            
            # 3. Insertamos
            db.poblacion.insert_many(poblacion_data)
            print(f"✅ (Carga) {len(poblacion_data)} personas insertadas en Atlas.")
            
        except Exception as e:
            print(f"❌ Error cargando datos: {e}")

if __name__ == "__main__":
    print("--- INICIANDO CARGA DE DATOS ---")
    client = get_mongo_client()
    if client:
        cargar_datos_prueba(client)
        print("✅ PROCESO DE CARGA FINALIZADO.")