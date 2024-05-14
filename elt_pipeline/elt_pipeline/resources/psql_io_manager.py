from typing import Any
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine
from contextlib import contextmanager

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    @contextmanager
    def connect_psql(self):
        user = self._config["user"]
        password = self._config["password"]
        host = self._config["host"]
        port = self._config["port"]
        db = self._config["database"]

        connection_path = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        try:
            engine = create_engine(connection_path)
            conn = engine.connect()
            yield conn
        finally:
            conn.close()

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):

        table_name = context.asset_key.path[-1]
        table_name = table_name.split("_")[1:]
        table_name = "_".join(table_name)

        with self.connect_psql() as conn:
            obj.to_sql(table_name, conn, if_exists='replace', index=False)
    
    def load_input(self, context: InputContext) -> Any:
        return super().load_input(context)
