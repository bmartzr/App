import requests
import json
from google.cloud import pubsub_v1
import os

credentials_path = 'ev3bigdatabmyh-2986d57c309e.json'
publisher = pubsub_v1.PublisherClient.from_service_account_json(credentials_path)

topic_name = 'tema-bmyh'
urls = [
    'http://api.weatherapi.com/v1/current.json?key=a9b1a00304864635ab024329231506&q=santiago&aqi=no',
    'http://api.weatherapi.com/v1/current.json?key=a9b1a00304864635ab024329231506&q=pucon&aqi=no',
    'http://api.weatherapi.com/v1/current.json?key=a9b1a00304864635ab024329231506&q=la%20serena&aqi=no'
]

def publish_message(message):
    publisher.publish(topic_name, data=message.encode('utf-8'))

def load_api_data(url):
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        for item in data:
            message = json.dumps(item)
            publish_message(message)
    else:
        print(f'Error al cargar la API. Código de estado: {response.status_code}')

for url in urls:
    load_api_data(url)
