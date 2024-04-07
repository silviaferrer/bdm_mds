import happybase
from hdfs import InsecureClient
import pyarrow.parquet as pq
import io
import time

# Configuration HBase
HBASE_COLUMN_FAMILY = 'data'
HBASE_METADATA_COLUMN_FAMILY = 'metadata'
HBASE_PORT = 16010
HOST = "10.4.41.48"

# Configuration HDFS
TMP_LANDING_DIR = "/temporal_landing"
HDFS_PORT = "9870"
HDFS_USER = "bdm"
HDFS_DIRECTORIES = ['income', 'elections', 'idealista']

def persist_to_hbase(table, row_key, data, schema):
    table.put(row_key, {HBASE_COLUMN_FAMILY: data, HBASE_METADATA_COLUMN_FAMILY: schema})

def read_from_hdfs(connection, directory):
    table = connection.table(directory)

    files = client.list(directory)
    
    for file in files:
        with client.read(f'{directory}/{file}') as reader:
            # read file
            file_content = reader.read()
            file_io = io.BytesIO(file_content)
            table = pq.read_table(file_io)
            
            schema_str = table.schema.to_string()
            data_dict = table.to_pandas().to_dict(orient='list')
            
            # key structure
            row_key = f"{directory.split('/')[2]}${file}${int(time.time())}"

            persist_to_hbase(table, row_key, data_dict, schema_str)

if __name__ == "__main__":
    # HBase connection
    connection = happybase.Connection(HOST, HBASE_PORT)

    # HDFS connection
    client = InsecureClient(f'http://{HOST}:{HDFS_PORT}', user=HDFS_USER)

    for directory in HDFS_DIRECTORIES:
        if directory.encode() not in connection.tables():
            connection.create_table(
                directory,
                {HBASE_COLUMN_FAMILY: dict()}
            )
        print(f"Table {directory} created in HBase.")

        read_from_hdfs(connection, TMP_LANDING_DIR + '/' + directory)