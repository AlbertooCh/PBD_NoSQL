import certifi
from pymongo import MongoClient
import sys

# Usamos la cadena CORTA (+srv) que es la correcta
URI = "mongodb+srv://alberto:12345@pbd-proyecto.tsbceg9.mongodb.net/?retryWrites=true&w=majority"

print("⏳ Intentando conectar a Atlas...")

try:
    # tlsCAFile=certifi.where() es la CLAVE para que Windows no falle
    client = MongoClient(URI, tlsCAFile=certifi.where())

    # Forzamos una operación real
    client.admin.command('ping')

    print("✅ ¡CONEXIÓN ÉXITOSA!")
    print("Ahora copia la parte de 'tlsCAFile=certifi.where()' a tu código principal.")

except Exception as e:
    print("\n❌ FALLO DE CONEXIÓN")
    print(f"Error: {e}")
    print("\nConsejo: Si el error dice 'bad auth', la contraseña no es 12345.")
    print("Consejo: Si el error es timeout, revisa el PASO 1 (0.0.0.0/0).")