import sqlalchemy
import pandas as pd


class MySqlDatabase:
    _mysql_conn = sqlalchemy.create_engine('mysql+pymysql://root:Viraj0458@localhost:3306/academicworld')

    def __int__(self):
        print('mysql connected!')

    def __enter__(self):
        return self

    def execute(self, query):
        return pd.read_sql(query, self._mysql_conn)

    def close(self):
        self._mysql_conn.dispose()


