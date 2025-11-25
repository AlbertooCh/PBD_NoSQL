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
    """Borra todo y carga usuarios y sectores desde los archivos JSON en la carpeta 'json'."""
    if m_client:
        try:
            db = m_client['practica_db']
            
            # 1. Limpiamos las colecciones
            db.poblacion.delete_many({})
            db.sectores.delete_many({})
            db.logs_poblacion.delete_many({})
            db.resumen_sectores.delete_many({})

            # Obtenemos la ruta donde está este script (data_loader.py)
            base_path = os.path.dirname(os.path.abspath(__file__))
            
            # Los datos se encuentran en la carpeta json'
            ruta_sectores = os.path.join(base_path, 'json', 'sectores.json')
            ruta_poblacion = os.path.join(base_path, 'json', 'poblacion.json')

            # 2. Cargar SECTORES
            with open(ruta_sectores, 'r', encoding='utf-8') as f:
                sectores_data = json.load(f)
            if sectores_data:
                db.sectores.insert_many(sectores_data)
                print(f"CARGA INICIAL: {len(sectores_data)} sectores cargados.")

            # 3. Cargar POBLACION
            with open(ruta_poblacion, 'r', encoding='utf-8') as f:
                poblacion_data = json.load(f)
            
            # Convertir fechas
            for p in poblacion_data:
                 p['fechanac'] = datetime.strptime(p['fechanac'], '%Y-%m-%d')

            if poblacion_data:
                db.poblacion.insert_many(poblacion_data)
                print(f"CARGA INICIAL: {len(poblacion_data)} personas cargadas.")
            
        except Exception as e:
            print(f"Error cargando datos: {e}")
            print(f"Ruta intentada: {ruta_sectores}")