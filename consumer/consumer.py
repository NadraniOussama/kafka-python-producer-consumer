from kafka import KafkaConsumer
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
def getIndexMessage(message):
	for i in range(len(listData)-1):
		if isinstance(message,dict):
			if listData[i]['number']==message['number']:
				return i
		else: 
			print ("error")
	return -1

consumer = KafkaConsumer(topicName, bootstrap_servers=['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('ascii')))# , group_id='my-group'
for message in consumer:
	if isinstance(message.value,dict):
		
		print(type(message.value),message.value)
		
	else :
		print("thank god",message.value)
	index =  getIndexMessage(message.value)
	if index!=-1:
		if listData[index]!=message:
			listData.insert(index,message)
			print(message.value)
	else :
		listData.append(message)
		print(message.value)


