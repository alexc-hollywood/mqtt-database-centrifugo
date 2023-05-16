import os
import paho.mqtt.client as mqtt
import psycopg2
from pymongo import MongoClient
import redis
import requests
import json
from dotenv import load_dotenv
import time

# Load variables from .env
load_dotenv()

# MQTT settings
MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT'))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
MQTT_TOPIC = os.getenv('MQTT_TOPIC')

# Redis settings
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_DB = int(os.getenv('REDIS_DB'))
REDIS_TOPIC = os.getenv('REDIS_TOPIC')

# MongoDB settings
MONGODB_URI = os.getenv('MONGODB_URI')
MONGODB_DB = os.getenv('MONGODB_DB')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION')

# PostgreSQL settings
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Centrifugo settings
CENTRIFUGO_API_ENDPOINT = os.getenv('CENTRIFUGO_API_ENDPOINT')
CENTRIFUGO_API_KEY = os.getenv('CENTRIFUGO_API_KEY')

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    
    try:
        # Store the data in Redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        r.set(REDIS_TOPIC, msg.payload)
    except Exception as e:
        print(f'Failed to store data in Redis: {e}')

    # Connect to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]
    try:
        # Insert the message into MongoDB
        data = {"topic": msg.topic, "message": msg.payload.decode()}
        collection.insert_one(data)
    except Exception as e:
        print(f'Failed to store data in MongoDB: {e}')

    # Connect to PostgreSQL
    conn = psycopg2.connect(POSTGRES_URI)
    cur = conn.cursor()
    try:
        # Insert the message into PostgreSQL
        cur.execute("INSERT INTO mqtt_data (topic, message) VALUES (%s, %s)", (msg.topic, msg.payload.decode()))
        conn.commit()
    except Exception as e:
        print(f'Failed to store data in PostgreSQL: {e}')
    finally:
        cur.close()
        conn.close()

    try:
        # Post the data to Centrifugo server
        headers = {'Content-type': 'application/json', 'Authorization': 'apikey ' + CENTRIFUGO_API_KEY}
        data = {
            "method": "publish",
            "params": {
                "channel": msg.topic,
                "data": msg.payload.decode()
            }
        }
        response = requests.post(CENTRIFUGO_API_ENDPOINT, headers=headers, data=json.dumps(data))

        if response.status_code != 200:
            print(f'Failed to post data to Centrifugo server. Status code: {response.status_code}')
    except Exception as e:
        print(f'Failed to post data to Centrifugo server: {e}')

client = mqtt.Client()
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except Exception as e:
        print(f'Connection failed, retrying in 5 seconds: {e}')
        time.sleep(5)
