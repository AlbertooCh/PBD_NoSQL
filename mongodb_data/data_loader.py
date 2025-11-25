from pymongo import MongoClient
import json
import os
from datetime import datetime

# --- CONFIGURACIÓN ---
MONGO_URI = "mongodb+srv://admin:12345@pbd-proyecto.tsbceg9.mongodb.net/?retryWrites=true&w=majority"

def get_mongo_client():
    """Intenta conectar a MongoDB Atlas y devuelve el cliente."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        return client
    except Exception as e:
        print(f"\nERROR CRÍTICO MONGO: {e}")
        return None

def cargar_datos_prueba(m_client):
    """Borra todo y carga usuarios y sectores desde los archivos JSON en la carpeta 'datasets'."""
    if m_client:
        try:
            db = m_client['practica_db']
            
            # 1. Limpiamos las colecciones
            db.poblacion.delete_many({})
            db.sectores.delete_many({})
            db.logs_poblacion.delete_many({})
            db.resumen_sectores.delete_many({})

            # Obtenemos la ruta donde está este script (mongodb_data)
            base_path = os.path.dirname(os.path.abspath(__file__))
            
            # --- CAMBIO AQUÍ ---
            # Usamos '..' para subir un nivel (a PBD_NOSQL) y luego entramos a 'datasets'
            ruta_sectores = os.path.join(base_path, '..', 'datasets', 'sectores.json')
            ruta_poblacion = os.path.join(base_path, '..', 'datasets', 'poblacion.json')
            # -------------------

            # 2. Cargar SECTORES
            # Verificamos si existe el archivo antes de abrirlo para evitar errores confusos
            if os.path.exists(ruta_sectores):
                with open(ruta_sectores, 'r', encoding='utf-8') as f:
                    sectores_data = json.load(f)
                if sectores_data:
                    db.sectores.insert_many(sectores_data)
                    print(f"CARGA INICIAL: {len(sectores_data)} sectores cargados.")
            else:
                print(f"Error: No se encontró el archivo {ruta_sectores}")

            # 3. Cargar POBLACION
            if os.path.exists(ruta_poblacion):
                with open(ruta_poblacion, 'r', encoding='utf-8') as f:
                    poblacion_data = json.load(f)
                
                # Convertir fechas
                for p in poblacion_data:
                     p['fechanac'] = datetime.strptime(p['fechanac'], '%Y-%m-%d')

                if poblacion_data:
                    db.poblacion.insert_many(poblacion_data)
                    print(f"CARGA INICIAL: {len(poblacion_data)} personas cargadas.")
            else:
                print(f"Error: No se encontró el archivo {ruta_poblacion}")
            
        except Exception as e:
            print(f"Error cargando datos: {e}")