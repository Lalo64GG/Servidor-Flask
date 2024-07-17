from flask import Flask, request, jsonify
import sqlite3
import queue
import requests
import threading
import time
import socket
import urllib3

# Deshabilitar las advertencias de urllib3 para las solicitudes HTTPS no verificadas
urllib3.disable_warnings()

app = Flask(__name__)
db_path = 'data.db'
data_queue = queue.Queue()
table_queue = queue.Queue()

def get_db():
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    return db

def create_table():
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value1 TEXT,
            value2 TEXT,
            synced INTEGER DEFAULT 0
        )
    ''')
    db.commit()

def create_tableTables():
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seat1 INTEGER,
            seat2 INTEGER DEFAULT 0,
            seat3 INTEGER DEFAULT 0,
            seat4 INTEGER DEFAULT 0,
            synced INTEGER DEFAULT 0
        )
    ''')
    db.commit()

def insert_into_db(value1, value2):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('INSERT INTO data (value1, value2) VALUES (?, ?)', (value1, value2))
    db.commit()

def insert_into_table(seat1, seat2, seat3, seat4):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('INSERT INTO tables (seat1, seat2, seat3, seat4) VALUES (?, ?, ?, ?)', (seat1, seat2, seat3, seat4))
    db.commit()

def mark_synced(id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('UPDATE data SET synced = 1 WHERE id = ?', (id,))
    db.commit()

def mark_table_synced(id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('UPDATE tables SET synced = 1 WHERE id = ?', (id,))
    db.commit()

def is_internet_available():
    try:
        # Intentar conectar con un host externo para verificar la conectividad
        socket.create_connection(("www.google.com", 80))
        return True
    except OSError:
        pass
    return False

def process_queue():
    while True:
        try:
            data = data_queue.get(timeout=1)  # Obtener datos de la cola con timeout de 1 segundo
            if is_internet_available():
                print(f"Enviando datos a la API de AWS: value1={data['value1']}, value2={data['value2']}")
                response = send_to_aws(data['value1'], data['value2'])
                if response and response.status_code == 201:
                    mark_synced(data['id'])  # Marcar como sincronizado en la base de datos local
                    print(f"Datos enviados correctamente a AWS para id={data['id']}")
                else:
                    data_queue.put(data)  # Reintentar si falla la sincronización
                    print(f"Fallo al enviar datos a AWS, reintentando para id={data['id']}")
                    time.sleep(5)  # Esperar antes de reintentar
            else:
                data_queue.put(data)  # Agregar de nuevo a la cola si no hay conexión
                print("Esperando red para enviar los datos a AWS...")
                time.sleep(5)  # Esperar antes de volver a verificar
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error processing queue: {e}")
            continue

def process_table_queue():
    while True:
        try:
            data = table_queue.get(timeout=1)  # Obtener datos de la cola con timeout de 1 segundo
            if is_internet_available():
                print(f"Enviando datos a la API de AWS: seat1={data['seat1']}, seat2={data['seat2']}, seat3={data['seat3']}, seat4={data['seat4']}")
                response = send_table_to_aws(data['seat1'], data['seat2'], data['seat3'], data['seat4'])
                if response and response.status_code == 201:
                    mark_table_synced(data['id'])  # Marcar como sincronizado en la base de datos local
                    print(f"Datos de mesa enviados correctamente a AWS para id={data['id']}")
                else:
                    table_queue.put(data)  # Reintentar si falla la sincronización
                    print(f"Fallo al enviar datos de mesa a AWS, reintentando para id={data['id']}")
                    time.sleep(5)  # Esperar antes de reintentar
            else:
                table_queue.put(data)  # Agregar de nuevo a la cola si no hay conexión
                print("Esperando red para enviar los datos de mesa a AWS...")
                time.sleep(5)  # Esperar antes de volver a verificar
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error processing table queue: {e}")
            continue

def send_to_aws(value1, value2):
    url = 'https://100.24.202.224/record'
    headers = {'Content-Type': 'application/json'}
    data = {
        'temperature': value2, 
        'humedity': value1,     
        'gas_level': "a",       
        'light': False
    }

    try:
        response = requests.post(url, json=data, headers=headers, verify=False)
        return response
    except Exception as e:
        print(f"Error sending to AWS: {e}")
        return None

def send_table_to_aws(seat1, seat2, seat3, seat4):
    url = 'https://100.24.202.224/table'
    headers = {'Content-Type': 'application/json'}
    data = {
        'seat1': seat1, 
        'seat2': seat2,
        'seat3': seat3,
        'seat4': seat4
    }

    try:
        response = requests.post(url, json=data, headers=headers, verify=False)
        return response
    except Exception as e:
        print(f"Error sending table data to AWS: {e}")
        return None

@app.route('/data', methods=['POST'])
def receive_data():
    data = request.get_json()
    
    # Verificar si 'value1' y 'value2' están presentes en los datos recibidos
    if 'value1' not in data or 'value2' not in data:
        return jsonify({'error': 'Missing required fields (value1, value2)'}), 400
    
    # Insertar datos en la base de datos local
    insert_into_db(data['value1'], data['value2'])
    
    # Agregar datos a la cola para procesar en segundo plano
    # Generar un ID ficticio para los fines de demostración
    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT last_insert_rowid() as id')
    result = cursor.fetchone()
    if result:
        data_id = result['id']
        data_queue.put({'id': data_id, 'value1': data['value1'], 'value2': data['value2']})

    return jsonify({'status': 'ok'})

@app.route('/table', methods=['POST'])
def receive_dataTable():
    data = request.get_json() 

    # Verify that the data is valid
    if 'seat1' not in data:
        return jsonify({'error': 'Missing required field (seat1)' }), 400

    insert_into_table(data['seat1'], data.get('seat2', 0), data.get('seat3', 0), data.get('seat4', 0))

    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT last_insert_rowid() as id')
    result = cursor.fetchone()
    if result: 
        data_id = result['id']
        table_queue.put({'id': data_id, 'seat1': data['seat1'], 'seat2': data.get('seat2', 0), 'seat3': data.get('seat3', 0), 'seat4': data.get('seat4', 0)})

    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    create_table()
    create_tableTables()
    # Iniciar hilos para procesar las colas en segundo plano
    data_queue_thread = threading.Thread(target=process_queue)
    data_queue_thread.start()

    table_queue_thread = threading.Thread(target=process_table_queue)
    table_queue_thread.start()

    # Ejecutar Flask en el puerto 5000
    app.run(host='0.0.0.0', port=5000, debug=True)
