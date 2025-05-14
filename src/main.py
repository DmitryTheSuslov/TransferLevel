
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
from textwrap import wrap
import time
import requests


CHUNK_SIZE = 10


bootstrap_servers = 'localhost:29092' 
topic = 'topic_2'

app = Flask(__name__)


def split_string_to_chunks(input_string, n):
    result = []
    current_chunk = ""

    for char in input_string:
        current_chunk += char
        if len(current_chunk.encode('utf-8')) > n:
            result.append(current_chunk[:-1]) 
            current_chunk = char 

    if current_chunk:
        result.append(current_chunk)

    return result

def segmented_send(message):
    try:
        ts = time.time()
        chunks = split_string_to_chunks(message, CHUNK_SIZE)
        total = len(chunks)
        for i, chunk in enumerate(chunks):
            data = {
                'payload': chunk,
                'total_segments': total,
                'segment_number': i,
                'username': 'user',
                'timestamp': ts
            }
            url = 'http://127.0.0.1:5000/transfer' # должен быть урл code
            requests.post(url, json = data)
            pass
            # producer.send(topic, chunk)
        
        producer.flush()

        return 'OK'

    except Exception as e:
        return str(e)


producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         api_version=(0,10),
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


@app.route('/send_message', methods=['POST'])
def send_message():
    message = request.json.get('message')
    if not message:
        return jsonify({'error': 'Отсутствует сообщение'}), 400

    status = segmented_send(message)
    if status != 'OK':
        return jsonify({'error': status}), 500

    return jsonify({'status': 'Сообщение успешно отправлено на канальный уровень'}), 200


@app.route('/transfer', methods=['POST'])
def transfer():
    segment = request.json
    message = segment['payload']
    if not message:
        return jsonify({'error': 'Отсутствует сообщение'}), 400

    print(type(segment))
    producer.send(topic, segment)
    return jsonify({'status': 'Сообщение успешно отправлено на транспортный уровень'}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
