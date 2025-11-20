from flask import Flask, render_template, request, jsonify
import redis
import json
import sys
import os

# Agregar el directorio actual al path para importar los módulos
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from redis_data.data_loader import cargar_redis, REDIS_HOST, REDIS_PORT

app = Flask(__name__)


def get_redis_client():
    """Conecta a Redis"""
    try:
        r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r_client.ping()
        return r_client
    except Exception as e:
        print(f"ERROR: No se pudo conectar a Redis. Detalles: {e}")
        return None


@app.route('/')
def index():
    """Página principal"""
    return render_template('index.html')


@app.route('/api/datos')
def obtener_datos():
    """Obtiene todos los datos de Redis"""
    r_client = get_redis_client()
    if not r_client:
        return jsonify({'error': 'No se pudo conectar a Redis'}), 500

    datos = {}

    # Obtener sectores
    sectores_keys = r_client.keys('sector:*')
    datos['sectores'] = {}
    for key in sectores_keys:
        sector_data = r_client.hgetall(key)
        datos['sectores'][key] = sector_data

    # Obtener personas
    personas_keys = r_client.keys('persona:*')
    datos['personas'] = {}
    for key in personas_keys:
        persona_raw = r_client.get(key)
        if persona_raw:
            try:
                persona_data = json.loads(persona_raw)
                datos['personas'][key] = persona_data
            except json.JSONDecodeError:
                datos['personas'][key] = persona_raw

    return jsonify(datos)


@app.route('/api/operaciones/consulta/<dni>')
def consultar_persona(dni):
    """Consulta una persona por DNI"""
    r_client = get_redis_client()
    if not r_client:
        return jsonify({'error': 'No se pudo conectar a Redis'}), 500

    persona_raw = r_client.get(f"persona:{dni}")
    if persona_raw:
        persona = json.loads(persona_raw)
        return jsonify({'encontrado': True, 'persona': persona})
    else:
        return jsonify({'encontrado': False})


@app.route('/api/operaciones/actualizar-sector', methods=['POST'])
def actualizar_sector():
    """Actualiza los ingresos de un sector"""
    r_client = get_redis_client()
    if not r_client:
        return jsonify({'error': 'No se pudo conectar a Redis'}), 500

    data = request.json
    sector_id = data.get('sector_id')
    incremento = float(data.get('incremento', 0))

    if not sector_id:
        return jsonify({'error': 'Se requiere sector_id'}), 400

    # Obtener ingresos antes
    ingresos_antes = r_client.hget(f"sector:{sector_id}", "ingresos_sum") or 0

    # Actualizar
    r_client.hincrbyfloat(f"sector:{sector_id}", "ingresos_sum", incremento)

    # Obtener ingresos después
    ingresos_despues = r_client.hget(f"sector:{sector_id}", "ingresos_sum")

    return jsonify({
        'sector_id': sector_id,
        'ingresos_antes': float(ingresos_antes),
        'ingresos_despues': float(ingresos_despues),
        'incremento': incremento
    })


@app.route('/api/operaciones/eliminar-persona/<dni>', methods=['DELETE'])
def eliminar_persona(dni):
    """Elimina una persona por DNI"""
    r_client = get_redis_client()
    if not r_client:
        return jsonify({'error': 'No se pudo conectar a Redis'}), 500

    deleted = r_client.delete(f"persona:{dni}")

    return jsonify({
        'eliminado': bool(deleted),
        'dni': dni
    })


@app.route('/api/cargar-datos', methods=['POST'])
def cargar_datos_iniciales():
    """Carga los datos iniciales en Redis"""
    try:
        cargar_redis()
        return jsonify({'mensaje': 'Datos cargados exitosamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)