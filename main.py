import sys
import json
import redis

# Importamos la función de carga desde el archivo anterior adaptado a Redis
from redis_data.data_loader import cargar_redis, REDIS_HOST, REDIS_PORT


# --- CONEXIÓN REDIS ---
def inicializar_cliente_redis():
    """Conecta únicamente a Redis."""
    try:
        r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r_client.ping()
        print("Conexión Redis establecida.")
        return r_client
    except Exception as e:
        print(f"ERROR: No se pudo conectar a Redis. Detalles: {e}")
        return None


# --- VISUALIZACIÓN DE TABLAS EN REDIS ---
def mostrar_datos_redis(r_client, patron):
    """Muestra hasta 10 registros que coincidan con un patrón en Redis."""
    print(f"\n--- VISUALIZACIÓN REDIS: {patron} ---")

    keys = r_client.keys(patron)

    if not keys:
        print("  No hay registros.")
        return

    for key in keys[:10]:
        # Determinar el tipo de dato y mostrar apropiadamente
        key_type = r_client.type(key)

        if key_type == 'hash':
            value = r_client.hgetall(key)
            print(f"  [{key}]: HASH -> {value}")
        elif key_type == 'string':
            value = r_client.get(key)
            try:
                # Intentar parsear JSON para mejor visualización
                parsed_value = json.loads(value)
                print(f"  [{key}]: STRING -> {parsed_value}")
            except json.JSONDecodeError:
                print(f"  [{key}]: STRING -> {value}")
        else:
            value = r_client.get(key)
            print(f"  [{key}]: {key_type} -> {value}")


# --- OPERACIONES CRUD SOLO CON REDIS ---
def ejecutar_operaciones_crud(r_client):
    """Ejecuta operaciones CRUD simuladas usando únicamente Redis."""

    print("\n[FASE 2: EJECUCIÓN DE OPERACIONES DE PRÁCTICA]")

    # --- 1. Consulta de un documento ---
    print("\n--- 1. Redis: Consulta de persona ---")
    persona_raw = r_client.get("persona:123456789")

    if persona_raw:
        persona = json.loads(persona_raw)
        print(f"  Consulta: {persona['nombre']} | Ingresos: {persona['ingresos']}")
    else:
        print("  Persona no encontrada.")

    # --- 2. UPDATE Atómico sobre Hash ---
    print("\n--- 2. Redis: UPDATE Atómico en sector ---")
    ingresos_antes = r_client.hget("sector:1", "ingresos_sum")
    print(f"  Ingresos Sector 1 antes: {ingresos_antes}")

    r_client.hincrbyfloat("sector:1", "ingresos_sum", 10000)

    ingresos_despues = r_client.hget("sector:1", "ingresos_sum")
    print(f"  Ingresos Sector 1 después: {ingresos_despues}")

    # --- 3. DELETE ---
    print("\n--- 3. Redis: Eliminación de persona ---")
    dni_a_borrar = "777888999"
    deleted = r_client.delete(f"persona:{dni_a_borrar}")
    if deleted:
        print(f"DNI {dni_a_borrar} eliminado de Redis.")
    else:
        print("No se encontró el registro a eliminar.")


# --- ORQUESTADOR PRINCIPAL ---
def main():
    print("--- INICIANDO PRÁCTICA NoSQL SOLO CON REDIS (ORQUESTADOR) ---")

    # 1. Conectamos Redis
    r_client = inicializar_cliente_redis()
    if not r_client:
        sys.exit(1)

    # 2. Cargamos datos
    print("\n[FASE 1: CARGA DE DATOS INICIAL]")
    try:
        cargar_redis()
        print("CARGA COMPLETA EN REDIS.")
    except Exception as e:
        print(f"ERROR FATAL DURANTE LA CARGA: {e}")
        sys.exit(1)

    # 3. Mostrar datos
    print("\n[FASE 1.5: VERIFICACIÓN DE ESTRUCTURAS EN REDIS]")
    mostrar_datos_redis(r_client, "sector:*")
    mostrar_datos_redis(r_client, "persona:*")

    # 4. CRUD
    ejecutar_operaciones_crud(r_client)

    print("\n--- EJECUCIÓN DEL ORQUESTADOR FINALIZADA ---")


if __name__ == "__main__":
    main()