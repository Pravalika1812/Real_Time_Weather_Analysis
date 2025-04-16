import json
import time
import requests
from kafka import KafkaProducer
import configparser
import os


# Dynamically load config.ini using relative path from this script
config = configparser.ConfigParser()
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))
config.read(config_path)

if 'API' not in config or 'key' not in config['API']:
    raise Exception("API key not found in config file")

api_key = config['API']['key']
bootstrap_servers = config['KAFKA']['bootstrap_servers']
topic = config['KAFKA']['topic']



# List of 10 cities
cities = ['New York', 'London', 'Tokyo', 'Delhi', 'Sydney',
          'Toronto', 'Berlin', 'Dubai', 'Paris', 'Sao Paulo', 'Baltimore']

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            'city': city,
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'description': data['weather'][0]['description'],
            'timestamp': data['dt']
        }
    else:
        print(f"Failed to fetch weather for {city}")
        return None

# Send weather data to Kafka every 10 seconds
while True:
    for city in cities:
        weather = get_weather(city)
        if weather:
            print(f"Sending data: {weather}")
            producer.send(topic, weather)
    time.sleep(10)
