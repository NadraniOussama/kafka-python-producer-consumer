from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
import json

topicName = 'NewTopic'
structurData={"number":0,
	"contract_name":"",
	"name":"",
	"address":"",
	"position":{"lat":0,"lng":0},
	"banking":False,
	"bonus":False,
	"bike_stands":0,
	"available_bike_stands":0,
	"available_bikes":0,
	"status":"OPEN",
	"last_update":1641522522000}
listData=[structurData]
def getIndexMessage(value):
	for i in range(len(listData)-1):
		if listData[i]['number']==value['number']:
			return i
	return -1

consumer = KafkaConsumer(topicName, bootstrap_servers=['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for message in consumer:
	# indique si un element a etait modifier ou ajouter dans la list
	isChanged=False 
	value = message.value
	# get indece ou la station est definie ou bien -1
	index =  getIndexMessage(value)
	if index!=-1:
		# verifie si il ya eu un changement depuit la dernier sauvgrade
		if listData[index]!=value:
			listData.insert(index,value)
			isChanged=True
			print("updating element")
	else :
		listData.append(value)
		isChanged=True
		print("inserting new elemnet")
	if isChanged: # affiche seulement les elements modifier dans la list
		print("number",value['number'],
		"\t available_bike_stands",value['available_bike_stands'],
		"\tavailable_bikes",value['available_bikes'],
		"\tname",value['name'])
