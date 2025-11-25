import pymongo
from datetime import datetime

def ejecutar_pruebas_crud(db):
    print("\n" + "="*50)
    print("       INICIANDO PROCESO DE PRUEBAS CRUD")
    print("="*50)

    # Identificadores para las pruebas
    id_prueba_nuevo = "99999999Z"
    id_existente_busqueda = "123456789"
    id_objetivo_borrado = "777888999"

    # ---------------------------------------------------------
    # PASO 1: CREATE
    # ---------------------------------------------------------
    print("\nPASO 1: CREATE (Crear Usuario de Prueba)")
    print("-" * 40)
    nuevo_usuario = {
        "_id": id_prueba_nuevo,
        "nombre": "Usuario Test",
        "ingresos": 2000,
        "sector_id": 1,
        "fechanac": datetime(2000, 1, 1),
        "sexo": "H"
    }
    
    try:
        db.poblacion.insert_one(nuevo_usuario)
        print(f"ÉXITO: Usuario {id_prueba_nuevo} insertado en la base de datos.")
    except pymongo.errors.DuplicateKeyError:
        print(f"AVISO: El usuario {id_prueba_nuevo} ya existía.")
    except Exception as e:
        print(f"ERROR: No se pudo crear: {e}")


    # ---------------------------------------------------------
    # PASO 2: READ (Verificar lo creado)
    # ---------------------------------------------------------
    print("\nPASO 2: READ (Leer Usuario Creado)")
    print("-" * 40)
    res = db.poblacion.find_one({"_id": id_prueba_nuevo})
    if res:
        print(f"LEÍDO: {res.get('nombre')} | Ingresos: {res.get('ingresos')}€ | Sector: {res.get('sector_id')}")
    else:
        print("ERROR: El usuario no aparece en la base de datos.")


    # ---------------------------------------------------------
    # PASO 3: READ (Buscar uno existente)
    # ---------------------------------------------------------
    print(f"\nPASO 3: READ (Buscar Datos Originales - ID: {id_existente_busqueda})")
    print("-" * 40)
    usuario_existente = db.poblacion.find_one({"_id": id_existente_busqueda})
    
    if usuario_existente:
        print(f"LEÍDO: {usuario_existente.get('nombre')} {usuario_existente.get('apellido1')} | Dirección: {usuario_existente.get('direccion')}")
    else:
        print("No encontrado.")


    # ---------------------------------------------------------
    # PASO 4: UPDATE
    # ---------------------------------------------------------
    print("\nPASO 4: UPDATE (Actualizar Sueldo)")
    print("-" * 40)
    
    print("Ejecutando subida de sueldo a 50.000 €...")
    try:
        db.poblacion.update_one({"_id": id_prueba_nuevo}, {"$set": {"ingresos": 50000}})
        
        # Verificación inmediata
        despues = db.poblacion.find_one({"_id": id_prueba_nuevo})
        if despues and despues.get('ingresos') == 50000:
            print(f"UPDATE CORRECTO: Sueldo actual es {despues.get('ingresos')} €")
        else:
            print("UPDATE FALLIDO o no reflejado.")
    except Exception as e:
        print(f"ERROR en UPDATE: {e}")


    # ---------------------------------------------------------
    # PASO 5: DELETE (Borrar un registro específico)
    # ---------------------------------------------------------
    print(f"\nPASO 5: DELETE (Borrar Usuario Objetivo - {id_objetivo_borrado})")
    print("-" * 40)
    try:
        borrado = db.poblacion.delete_one({"_id": id_objetivo_borrado})
        if borrado.deleted_count > 0:
            print(f"ÉXITO: El usuario con ID {id_objetivo_borrado} ha sido eliminado de la BD.")
        else:
            print(f"AVISO: El usuario {id_objetivo_borrado} no se encontró (quizás ya fue borrado).")
    except Exception as e:
        print(f"ERROR en DELETE: {e}")


    # ---------------------------------------------------------
    # PASO 6: DELETE (Limpieza Usuario Test)
    # ---------------------------------------------------------
    print("\nPASO 6: DELETE (Limpieza Usuario Test)")
    print("-" * 40)
    try:
        db.poblacion.delete_one({"_id": id_prueba_nuevo})
        final_check = db.poblacion.find_one({"_id": id_prueba_nuevo})
        
        if final_check is None:
            print("LIMPIEZA COMPLETADA: Usuario Test eliminado.")
        else:
            print("El documento de prueba aún existe tras el intento de borrado.")
    except Exception as e:
        print(f"ERROR en limpieza final: {e}")


    # ---------------------------------------------------------
    # AVISO FINAL DE LOGS
    # ---------------------------------------------------------
    
    print("\n" + "="*70)
    print("TRIGGER: La operación ha sido enviada a MongoDB Atlas.") 
    print("            (Revisar 'App Services > Logs' en la nube para verificar).")
    print("="*70 + "\n")