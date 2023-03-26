import subprocess
from src.repository.mongo import MongoDB

class IRepository():
   
    def get_topics(self):
        print("LLegué al repo")
        a = MongoDB()
        return a.get_topics()
