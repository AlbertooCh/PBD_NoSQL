import requests
import json

RIAK_HOST = 'http://localhost:8098'
HEADERS = {'Content-Type': 'application/json'}


def guardar_en_riak(bucket, key, data):
    url = f"{RIAK_HOST}/buckets/{bucket}/keys/{key}"
    try:
        response = requests.put(url, data=json.dumps(data), headers=HEADERS)

        if response.status_code in [200, 204]:
            return True
        else:
            print(f"Error insertando {key}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Excepción conectando a Riak: {e}")
        return False


sectores = [
    {"codS": 1, "nombreS": "Agricultura y pesca", "porcentS": 0, "ingresosS": 0},
    {"codS": 2, "nombreS": "Industria y energía", "porcentS": 0, "ingresosS": 0},
    {"codS": 3, "nombreS": "Servicios", "porcentS": 0, "ingresosS": 0},
    {"codS": 4, "nombreS": "Construcción", "porcentS": 0, "ingresosS": 0},
]

print("--- INSERTANDO SECTORES ---")
for s in sectores:
    bucket = "sectores"
    key = str(s['codS'])

    if guardar_en_riak(bucket, key, s):
        print(f"Sector {key} insertado en bucket '{bucket}'")

poblacion = [
    {"dni": "123456789", "nombre": "Carlos", "apellido1": "Gutiérrez", "apellido2": "Pérez", "fechanac": "1997-11-15",
     "direccion": "Pz. Colón", "cp": "06300", "sexo": "H", "ingresos": 15000, "gastosFijos": 4000, "gastosAlim": 4000,
     "gastosRopa": 3000, "sector": 1},
    {"dni": "777888999", "nombre": "Gerardo", "apellido1": "Martín", "apellido2": "Duque", "fechanac": "1961-12-12",
     "direccion": "C. del Atín", "cp": "06002", "sexo": "H", "ingresos": 23000, "gastosFijos": 6000, "gastosAlim": 5000,
     "gastosRopa": 4000, "sector": 1},
    {"dni": "222333444", "nombre": "Pedro", "apellido1": "Sánchez", "apellido2": "González", "fechanac": "1960-02-01",
     "direccion": "C. Ancha", "cp": "06300", "sexo": "H", "ingresos": 22000, "gastosFijos": 5000, "gastosAlim": 3000,
     "gastosRopa": 2000, "sector": 1},
    {"dni": "333444555", "nombre": "María", "apellido1": "García", "apellido2": "Gil", "fechanac": "1971-04-10",
     "direccion": "C. Diagonal", "cp": "06400", "sexo": "M", "ingresos": 19500, "gastosFijos": 3000, "gastosAlim": 3000,
     "gastosRopa": 3000, "sector": 1},
    {"dni": "666884444", "nombre": "Ignacio", "apellido1": "Costa", "apellido2": "Burgos", "fechanac": "1982-12-12",
     "direccion": "C. Descubrimiento", "cp": "10005", "sexo": "H", "ingresos": 37000, "gastosFijos": 5000,
     "gastosAlim": 4500,
     "gastosRopa": 2500, "sector": 1},
    {"dni": "555666777", "nombre": "Vicente", "apellido1": "Marañán", "apellido2": "Fernández",
     "fechanac": "1978-11-15",
     "direccion": "Pz. América", "cp": "10600", "sexo": "H", "ingresos": 46000, "gastosFijos": 8000, "gastosAlim": 5000,
     "gastosRopa": 4000, "sector": 2},
    {"dni": "666777888", "nombre": "Beatriz", "apellido1": "Losada", "apellido2": "Gijón", "fechanac": "1974-04-12",
     "direccion": "Av. Principal", "cp": "06400", "sexo": "M", "ingresos": 50000, "gastosFijos": 15000,
     "gastosAlim": 8000,
     "gastosRopa": 5000, "sector": 2},
    {"dni": "888999000", "nombre": "Fernando", "apellido1": "Huertas", "apellido2": "Martínez",
     "fechanac": "1971-05-30",
     "direccion": "C. Mayor", "cp": "06002", "sexo": "H", "ingresos": 70000, "gastosFijos": 20000, "gastosAlim": 8000,
     "gastosRopa": 4500, "sector": 2},
    {"dni": "999000111", "nombre": "Francisco", "apellido1": "Fernández", "apellido2": "Merchán",
     "fechanac": "1979-03-12",
     "direccion": "C. Poniente", "cp": "10800", "sexo": "H", "ingresos": 63000, "gastosFijos": 12000,
     "gastosAlim": 7500,
     "gastosRopa": 4500, "sector": 2},
    {"dni": "666999333", "nombre": "Paula", "apellido1": "Ordóñez", "apellido2": "Ruiz", "fechanac": "1990-02-01",
     "direccion": "C. Atlántico", "cp": "06800", "sexo": "M", "ingresos": 25000, "gastosFijos": 10000,
     "gastosAlim": 3000,
     "gastosRopa": 2000, "sector": 2},
    {"dni": "987654321", "nombre": "Eva", "apellido1": "Moreno", "apellido2": "Pozo", "fechanac": "1974-05-10",
     "direccion": "C. Justicia", "cp": "10005", "sexo": "M", "ingresos": 40000, "gastosFijos": 9000, "gastosAlim": 6000,
     "gastosRopa": 3000, "sector": 3},
    {"dni": "111000111", "nombre": "Antonio", "apellido1": "Muñoz", "apellido2": "Hernández", "fechanac": "1989-07-01",
     "direccion": "C. Constitución", "cp": "06800", "sexo": "H", "ingresos": 25000, "gastosFijos": 6500,
     "gastosAlim": 3500,
     "gastosRopa": 4000, "sector": 3},
    {"dni": "111000222", "nombre": "Sara", "apellido1": "Gálvez", "apellido2": "Montes", "fechanac": "1973-04-07",
     "direccion": "C. Cádiz", "cp": "10300", "sexo": "M", "ingresos": 40000, "gastosFijos": 11000, "gastosAlim": 9500,
     "gastosRopa": 6500, "sector": 3},
    {"dni": "111000333", "nombre": "Cristina", "apellido1": "Corral", "apellido2": "Palma", "fechanac": "1976-05-12",
     "direccion": "C. Ermita", "cp": "10600", "sexo": "M", "ingresos": 48000, "gastosFijos": 13000, "gastosAlim": 7800,
     "gastosRopa": 5200, "sector": 3},
    {"dni": "111222333", "nombre": "Susana", "apellido1": "Ruiz", "apellido2": "Méndez", "fechanac": "1999-06-22",
     "direccion": "Av. Libertad", "cp": "10800", "sexo": "M", "ingresos": 18000, "gastosFijos": 5000,
     "gastosAlim": 4500,
     "gastosRopa": 2500, "sector": 3},
    {"dni": "444555666", "nombre": "Ángel", "apellido1": "Montero", "apellido2": "Salas", "fechanac": "2000-04-07",
     "direccion": "C. Tranquilidad", "cp": "10300", "sexo": "H", "ingresos": 14000, "gastosFijos": 3000,
     "gastosAlim": 3000,
     "gastosRopa": 3000, "sector": 4},
    {"dni": "888777666", "nombre": "Manuel", "apellido1": "Vega", "apellido2": "Zarzal", "fechanac": "1976-11-23",
     "direccion": "Pz. Azul", "cp": "10005", "sexo": "H", "ingresos": 36000, "gastosFijos": 12000, "gastosAlim": 6000,
     "gastosRopa": 3000, "sector": 4},
    {"dni": "333445555", "nombre": "Margarita", "apellido1": "Guillón", "apellido2": "Campos", "fechanac": "1974-03-19",
     "direccion": "Av. Héroes", "cp": "06800", "sexo": "M", "ingresos": 50000, "gastosFijos": 12000, "gastosAlim": 7500,
     "gastosRopa": 6500, "sector": 4},
    {"dni": "222447777", "nombre": "Fermín", "apellido1": "Hoz", "apellido2": "Torres", "fechanac": "1988-08-08",
     "direccion": "C. Curva", "cp": "06002", "sexo": "H", "ingresos": 25000, "gastosFijos": 4000, "gastosAlim": 3000,
     "gastosRopa": 2500, "sector": 4}
]

print("\n--- INSERTANDO POBLACIÓN ---")
for p in poblacion:
    bucket = "poblacion"
    key = p['dni']

    if guardar_en_riak(bucket, key, p):
        print(f"Población {key} insertada en bucket '{bucket}'")