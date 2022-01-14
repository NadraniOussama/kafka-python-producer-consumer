import json, urllib, requests
# from kafka import KafkaProducer
# from kafka.errors import KafkaError


url = "https://api.jcdecaux.com/vls/v1/stations?contract=marseille&apiKey=d5d56f9105fbbdfd8c1e53072eb3b2fba2bd41c4"
r = requests.get(url)
print(r.headers['content-type'])
data = r.json()
for startion in data:
	print("statiion numero: ==>",startion)
# producer = KafkaProducer(bootstrap_servers=['broker1:1234'])

