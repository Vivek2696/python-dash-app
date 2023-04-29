import neo4j
import pandas as pd


class Neo4jDatabase:
    neo_conn = neo4j.GraphDatabase.driver("bolt://localhost:7687", auth=('root', 'Viraj0458'))
    neo4j_db = neo_conn.session(database='academicworld')

    def __int__(self):
        print('Neo4j Connected!')

    def execute(self, query):
        result = self.neo4j_db.run(query).data()
        return pd.DataFrame(result)

    def close(self):
        self.neo_conn.close()

