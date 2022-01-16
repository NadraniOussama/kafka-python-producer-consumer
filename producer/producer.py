import json, requests
from kafka import KafkaProducer		
from kafka.errors import KafkaError
import time

# GET Station information as JSON from api 
url = "https://api.jcdecaux.com/vls/v1/stations?contract=marseille&apiKey=d5d56f9105fbbdfd8c1e53072eb3b2fba2bd41c4"

def on_send_success(record_metadata):
		print(record_metadata.topic)
		print(record_metadata.partition)
		print(record_metadata.offset)

def on_send_error(excp):
		log.error('I am an errback', exc_info=excp)
# Initialise Kafka Producer 
topicName='NewTopic'
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
	value_serializer=lambda m: json.dumps(m).encode('utf-8'))
# Send Json data to Consumer 
print("Start Producer using bootstrap server: localhost:9092 in topic NewTopic")
while True:
	r = requests.get(url)
	print(r.headers['content-type']) # print json file 
	data = r.json()
	for station in data:
		producer.send(topicName, station).add_callback(on_send_success).add_errback(on_send_error)
	# continue once a day 86400s == 1 day
	time.sleep(86400) 

