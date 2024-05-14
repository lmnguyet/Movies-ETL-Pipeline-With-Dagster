from typing import Any
from azure.storage.blob import BlobServiceClient, ContentSettings
from dagster import IOManager, OutputContext, InputContext
from contextlib import contextmanager
import pandas as pd
import os
import pyarrow.parquet as pq
import pyarrow

class ABSIOManager(IOManager):
    def __init__(self, config) -> None:
        self._config = config

    @contextmanager
    def connect_abs(self, container, blob):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self._config["connection_string"])
            blob_client = blob_service_client.get_blob_client(container=container, blob=blob)
            yield blob_client
        except Exception as e:
            return str(e)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        table_name = context.asset_key.path[-1]
        file_name = f"{table_name}.parquet"
        file_path = f"../tmp/{file_name}"

        obj.to_parquet(file_path, engine="pyarrow")

        container = context.asset_key.path[-2]

        try:
            with self.connect_abs(container=container, blob=file_name) as connection:
                if not connection.exists():
                    connection.create_append_blob(content_settings=ContentSettings(content_type="application/octet-stream"))
                with open(file_path, "rb") as data:
                    connection.append_block(data)
            
            context.log.info(f"Load {table_name} to container {container} successfully!")
            # os.remove(file_path)
        except Exception as e:
            context.log.error(str(e))
    
    def load_input(self, context: InputContext):
        table_name = context.asset_key.path[-1]
        
        file_name = f"{table_name}.parquet"
        
        file_path = f"../tmp/saved_{file_name}"

        container = context.asset_key.path[-2]
        
        # if context.upstream_output.metadata["batch_size"]:
        #     batch_size = context.upstream_output.metadata["batch_size"]
        # else:
        #     batch_size = None
        
        try:
            with self.connect_abs(container=container, blob=file_name) as connection:
                blob_content = connection.download_blob().readall()
            
            with open(file_path, "wb") as file:
                file.write(blob_content)
            
            # if batch_size:
            #     parquet_file = pq.ParquetFile(file_path)
            #     for batch in parquet_file.iter_batches(batch_size=batch_size):
            #         df = batch.to_pandas()
            #         yield df
            # else:
            df = pd.read_parquet(file_path)
            return df
        except Exception as e:
            context.log.error(str(e))
            return None
