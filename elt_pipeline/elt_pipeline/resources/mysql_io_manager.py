import pandas as pd
from contextlib import contextmanager
import mysql.connector

class MySQLIOManager:
    def __init__(self, config) -> None:
        self._config = config

    @contextmanager
    def connect_mysql(self):
        conn = None
        try:
            conn = mysql.connector.connect(**self._config)
            yield conn
        finally:
            if conn:
                conn.close()

    def extract_data(self, sql: str) -> pd.DataFrame:
        df = pd.DataFrame()

        with self.connect_mysql() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            data = cursor.fetchall()

            columns = [i[0] for i in cursor.description]

            df = pd.DataFrame(data, columns=columns)

        return df