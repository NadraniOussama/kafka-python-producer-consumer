from kafka import KafkaConsumer
import json

topicName = 'testTopic'
listNumber=[]
listData=[]
dictData={"hello":"hey"}

consumer = KafkaConsumer(topicName, bootstrap_servers=['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('ascii')))# , group_id='my-group'
for message in consumer:
# message value and key are raw bytes -- decode if necessary!
# e.g., for unicode: `message.value.decode('utf-8')`
    # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, 
    # message.offset, message.key,message.value))
    # listData.append(message.value)

    # print(isinstance(message.value,dict))
    # print 
    # listData.append(message.value)
    print(message.value)

# print(consumer.topicName topicName )    
    # print(message.value['number']
    # print(type(message.value))
print(listData)
# conumer.flush()
# print(listData.)