from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
import json

topicName = 'testTopic'
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
		# print("list",type(listData[i]))#,listData[i]['number']
		if listData[i]['number']==value['number']:
			return i
	return -1

consumer = KafkaConsumer(topicName, bootstrap_servers=['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('utf-8')))#,auto_offset_reset='earliest')# , group_id='my-group'


for message in consumer:
	# if isinstance(message.value,dict):
	# data = json.loads(message.value)
	isChanged=False
	value = message.value
	index =  getIndexMessage(value)
	if index!=-1:
		if listData[index]!=message.value:
			listData.insert(index,message.value)
			isChanged=True
			print("updating element")
		# else:
			
	else :
		listData.append(message.value)
		isChanged=True
		print("inserting new elemnet")
	if isChanged:
		print("number",message.value['number'],
		"\t available_bike_stands",message.value['available_bike_stands'],
		"\tavailable_bikes",message.value['available_bikes'],
		"\tname",message.value['name'])


