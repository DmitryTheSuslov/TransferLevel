import json
import time
from kafka import KafkaProducer, KafkaConsumer
import requests
from threading import Thread


FRAME_TIME = 1
bootstrap_servers = ['localhost:29092'] 
topic = 'topic_2' 

consumer = KafkaConsumer(
   topic,
   bootstrap_servers=bootstrap_servers,
   api_version=(0,10),
   auto_offset_reset='latest',  # или 'latest'
   enable_auto_commit=False,
   value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

message_storage = {}

def add_message(segment):
   message_storage[segment['timestamp']] = {
      'received': 0,
      'total': segment['total_segments'],
      'last': time.time(),
      'username': segment['username'],
      'segments': []
   }

def add_segment(segment):
   ts = segment['timestamp']
   if ts not in message_storage:
      add_message(segment)
   
   message_storage[ts]['received'] += 1
   message_storage[ts]['last'] = time.time()
   message_storage[ts]['segments'].append(segment['payload'])
 
def concat_messages(ts):
   message = message_storage[ts]
   result = ''.join(message['segments'])
   return result

def receiveMessage(data):
   url = ""
   requests.post(url, json=data)

def scan_storage(sender):
   global message_storage
   for ts, message in message_storage.items():
      if message['received'] == message['total']:
         data = {
            'username': message['username'],
            'data': concat_messages(ts),
            'send_time': ts,
            'error': ''
         }
      else: 
         data = {
            'username': message['username'],
            'data': concat_messages(ts),
            'send_time': ts,
            'error': ''
         }
      print(data)
   message_storage = {}
      

def tick():
   while True:
      scan_storage(lambda x: x)
      time.sleep(FRAME_TIME)

t1 = Thread(target=tick)
t1.start()

for segment in consumer:
   value = segment.value
   # print(value['timestamp'])
   add_segment(value)
   # print("#" * 100)
   # print(message_storage)
   # print("#" * 100)

consumer.close()