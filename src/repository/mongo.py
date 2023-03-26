import os
import json
from pymongo import MongoClient
from pymongo.errors import InvalidName
from bson.json_util import dumps, loads
from pymongo.collection import ReturnDocument
from src.serializers.topic_serializers import topicEntity, topicListEntity

MONGODB_HOST = os.environ.get('MONGODB_HOST') or 'localhost'
MONGODB_PORT = os.environ.get('MOONGODB_PORT') or '27017'
URI = f'mongodb://{MONGODB_HOST}:{MONGODB_PORT}/'
    
class MongoDB():
    
    
    def __init__(self) -> None:
        pass
    
    def connect(self):
        print('mongo URI',URI)
        client = MongoClient(URI, username='root', password='root')
        try:
           db = client.kafka_topics
        except InvalidName:
            print('database does not exists')
        except Exception as e:
            print('database does not exists 2')
        print('Connected to MongoDB...')
        return client
    

    def get_topics(self):
        try:
            print("LLegué al repo")
            client = self.connect()
            db = client.kafka_topics
            # try:
            #     db = client.kafka_topics
            # except InvalidName    
            print("dsf")
            Topics = db.topics.find({}, {"_id":0, "price":0, "origin":0})
            #insert_one({ "name": "John", "address": "Highway 37"})
            print("collections1",  )
            lista=list(Topics)
            topics_list=[]
            for top in lista:
                print('foreach', top)
                topics_list.append(top['name'])
            
            client.close()
        except Exception as e:
            print('exception', e)
        
        print('respuesta', topics_list, "32", len(json.loads(dumps(topics_list))))    
        return (None if len(json.loads(dumps(topics_list)))==0 else {"topics": json.loads(dumps(topics_list))})
        

        