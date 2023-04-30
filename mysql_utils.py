import sqlalchemy
import pandas as pd


class MySqlDatabase:
    _mysql_conn = sqlalchemy.create_engine('mysql+pymysql://root:Viraj0458@localhost:3306/academicworld')
    db_conn = _mysql_conn.connect()

    def __int__(self):
        print('mysql connected!')

    def __enter__(self):
        return self

    def execute(self, query):
        return pd.read_sql(query, self._mysql_conn)

    def execute_statement(self, query):
        return self.db_conn.execute(sqlalchemy.text(query))

    def commit_transaction(self):
        self.db_conn.commit()

    def close(self):
        self._mysql_conn.dispose()
