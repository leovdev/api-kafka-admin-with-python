import os
import json
from pymongo import MongoClient
from pymongo.errors import InvalidName
from bson.json_util import dumps, loads
from pymongo.collection import ReturnDocument
from src.serializers.topic_serializers import topicEntity, topicListEntity
from src.exception.mongo_connection_exception import MongoConnectionException
from src.exception.database_connection_exception import DatabaseConnectionException

MONGODB_HOST = os.environ.get('MONGODB_HOST') or 'localhost'
MONGODB_PORT = os.environ.get('MOONGODB_PORT') or '27017'
URI = f'mongodb://{MONGODB_HOST}:{MONGODB_PORT}/'
    
class MongoDB():
    
    
    def __init__(self) -> None:
        pass
    
    def connect(self):
        try:
            client =  MongoClient(URI, username='root', password='root')
        except Exception as e:
            print('exception', e)
            raise MongoConnectionException()
        return client
    

    def get_topics(self):
        try:
            client = self.connect()
            topics = client.kafka_topics.topics.find({}, {"_id":0, "price":0, "origin":0})
            topics = list(topics) #Para enviar json completo, enviarlo así tal cual
            topics_count = len(list(topics))
            
            if topics_count == 0:
                return None
            
            client.close()
            detailed_list = True
            if (detailed_list):
                return {"topics": topics}
            
            topics_names_list = self.extract_only_topic_names(topics)
           
        except InvalidName as e:
            print(e,"cause 0", e.cause)
        except MongoConnectionException as e:
            print(e,"cause 1", e.cause)
            raise MongoConnectionException()
        except Exception as e:
            print(e,"cause 2")
            return None

        return {"topics": topics_names_list}

    def extract_only_topic_names(self, topics_list, topics):
        topics_list = []
        for topic in topics:
            if 'name' in topic:
                print('foreach', topic )
                topics_list.append(topic['name'])
        return topics_list
    

    def insert_topic(self):
        try:
            client = self.connect()
            db = client.kafka_topics
            database_response = db.topics.find({"name":"orange"})
            count = len(list(database_response))
            exists= True if count>0 else False
            
            if (not exists):
                db.topics.insert_one({"name":"papay"})
                client.close()
                return True
        except InvalidName as e:
            print(e,"cause 0", e.cause)
        except MongoConnectionException as e:
            print(e,"cause 1", e.cause)
            raise MongoConnectionException()
        except Exception as e:
            print(e,"cause 2")
        
        return False
    
    def update_topic(self):
        try:
            client = self.connect()
            print("respuesta 1")
            db = client.kafka_topics
            print("respuesta 2")
            topics = db.topics.find_one_and_update(
                {'name':'apple'},
                {'$set': {'name':'apple1'}},
                upsert= True,
                return_document=ReturnDocument.AFTER
            )
            print("respuesta",dir(topics), "id ")
        except InvalidName as e:
            print(e,"cause 0", e.cause)
        except MongoConnectionException as e:
            print(e,"cause 1", e.cause)
            raise MongoConnectionException
        except Exception as e:
            print(e,"cause 2")
            topics_list=None
        client.close()
        return None

        