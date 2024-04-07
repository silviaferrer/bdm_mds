import io
import time
import pandas as pd
    
def read_from_temporal(client, tmp_landing_dir):
    try:
        files = client.list(tmp_landing_dir)

        dict_data = {}
        
        for file in files:
            with client.read(f'{tmp_landing_dir}/{file}') as reader:
                file_content = reader.read()
                data = pd.read_parquet(io.BytesIO(file_content))
                
                # key structure
                file_name = f"{tmp_landing_dir.split('/')[2]}${file.split('.')[0]}${int(time.time())}.parquet"

                dict_data[file_name] = data
    except Exception as e:
        print(f'Error {e} during the read of the directory {tmp_landing_dir}')

    return dict_data

def upload_to_persistent(client, per_landing_dir, file_name, content):
    try:        
        with io.BytesIO() as buffer:
            content.to_parquet(buffer, engine='pyarrow')
            buffer.seek(0)
            with client.write(f'{per_landing_dir}/{file_name}', overwrite=True) as writer:
                writer.write(buffer.getvalue())

        print(f"Uploaded correctly - {file_name}")

    except Exception as e:
        print(f"Error {e} during the upload of the file {file_name}")


def main(hdfs_client, tmp_landing_dir, per_landing_dir, hdfs_directories):
    # read and upload data per dataset
    for dir in hdfs_directories:
        # read data from temporal landing
        print(f'Starting {dir} dataset read')
        data_dict = read_from_temporal(hdfs_client, tmp_landing_dir + '/' + dir)

        # upload files to persistent
        for file_name, content in data_dict.items():
            upload_to_persistent(hdfs_client, per_landing_dir + '/' + dir, file_name, content) 
