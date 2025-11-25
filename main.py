import time
import data_loader
import crud
import operations_advanced

def main():
    print("--- CONECTANDO AL SISTEMA ---")
    
    client = data_loader.get_mongo_client()
    
    if client:
        # 1. Carga inicial (Limpia y restaura JSONs)
        data_loader.cargar_datos_prueba(client)
        
        # Se incluye este tiempo de espera debido a que los triggers en Atlas son asíncronos 
        # y algunas operaciones se pueden hacer antes de actualizar ciertos valores.
        time.sleep(4)
        
        db = client['practica_db']
        
        # 2. Ejecutar las pruebas CRUD
        print("\n>>> EJECUTANDO CRUD BÁSICO...")
        crud.ejecutar_pruebas_crud(db) 

        # Se incluye este tiempo de espera debido a que los triggers en Atlas son asíncronos 
        # y algunas operaciones se pueden hacer antes de actualizar ciertos valores.
        time.sleep(4)
        
        # 3. Ejecutar las operaciones avanzadas
        operations_advanced.ejecutar_pruebas_avanzadas(db)
        
        print("\nPROCESO FINALIZADO.")

if __name__ == "__main__":
    main()