import pandas as pd
import pymongo


class MongoDbConnection:
    mongo_conn = pymongo.MongoClient('mongodb://localhost:27017')
    mongo_db = mongo_conn.academicworld

    def __int__(self):
        print('MongoDb Connected!')

    def execute_publication(self, pipeline):
        query = self.mongo_db.publications.aggregate(pipeline)
        return pd.DataFrame(list(query))

    def execute_faculty(self, pipeline):
        query = self.mongo_db.faculty.aggregate(pipeline)
        return pd.DataFrame(list(query))

    def close(self):
        self.mongo_conn.close()


