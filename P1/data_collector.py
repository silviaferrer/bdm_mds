import requests
import os
import io
import pandas as pd
from hdfs import InsecureClient

def extract_open_data_bcn_income(data_folder, urls):
    if not os.path.exists(data_folder+'/income'):
        os.makedirs(data_folder+'/income')

    paths = []
    for year in urls.keys():
        df = extract_open_data_bcn_datasets(urls[year])
        path = 'income/'+year+'_Distribucio_territorial_renda_familiar.parquet'
        df.to_parquet(os.path.join(data_folder, path))
        paths.append(path)

    return paths

def extract_open_data_bcn_elections(data_folder, url):    
    if not os.path.exists(data_folder+'/elections'):
        os.makedirs(data_folder+'/elections')

    path = 'elections/2023_07_23_Eleccions_Congres_Diputats.parquet'

    df = extract_open_data_bcn_datasets(url)
    df.to_parquet(os.path.join(data_folder, path))

    return path

def extract_open_data_bcn_datasets(url):
    response = requests.get(url)
    df = pd.read_json(io.StringIO(response.content.decode('utf-8')))
    df_result = pd.DataFrame(df.loc['records','result'])

    return df_result

def extract_idealista(data_folder, source_dir):
    if not os.path.exists(data_folder+'/idealista'):
        os.makedirs(data_folder+'/idealista')

    paths = []
    for json_file in os.listdir(source_dir):
        df = pd.read_json(source_dir + json_file)
        file_name = json_file.split('.')[0]
        path = '/idealista/' + file_name + '.parquet'
        df.to_parquet(os.path.join('.' + data_folder + path))
        paths.append(path)

    return paths

def create_hdfs(hdfs_host, hdfs_port, hdfs_user, temp_landing_dir):
    try:
        client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user=hdfs_user)
        print(f"Connection to HDFS has been established successfully.")
        if client.status(temp_landing_dir, strict=False) is None:
            client.makedirs(temp_landing_dir)
        
        return client

    except Exception as e:
        client.close()
        print(e)
        return None

def upload_file_hdfs(client, temp_landing_dir, local_path, dataset):
    try:        
        str = file_path.split('/')
        file_name = str[len(str)-1]
        remote_path = client.upload(temp_landing_dir + dataset + file_name, local_path, overwrite=True)
        print(f"Uploaded correctly - {remote_path}")

    except Exception as e:
        print(f"Error {e} during the upload of the file {file_name}")

if __name__ == "__main__":
    # intialize variables
    local_data_folder = "/data"
    urls_income = {'2017': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=e7206797-e57b-4ded-8c6c-62e9b4cb54f7',
            '2016': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=1d9ff171-6f23-45c1-b02f-203b0589f08a',
            '2015': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=bb4de997-cdf9-43ad-98c6-cc3a3e4d4f07',
            '2014': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=a741ff54-968e-4fa9-adfc-3635bc84b692',
            '2013': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=4ed48990-3686-4f28-8b1d-6a4350d91218',
            '2012': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=336780a2-8a92-4356-9302-b88ed997e4a8',
            '2011': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=c4751d23-fb9f-429a-be8b-1e119646417a',
            '2010': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=1981aa37-2eba-4b55-948c-2264498a269f',
            '2009': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=cdd351d9-eb5c-443b-a7be-bdadaf724614',
            '2008': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=2c178800-917e-4b59-9e4e-29da232fb26f',
            '2007': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=935b8e2f-996f-4829-8586-c7ddfcb9ba18'}
    url_elections = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=e8fce35e-46b9-429e-a29f-945c33a3a8ef"

    # intialize hdfs variables
    temp_landing_dir = "/temporal_landing"
    hdfs_host = "10.4.41.48"
    hdfs_port = "9870"
    hdfs_user = "bdm"

    # extract data
    local_paths_income = extract_open_data_bcn_income(local_data_folder, urls_income)
    local_path_elections = extract_open_data_bcn_elections(local_data_folder, url_elections)
    local_path_idealista = extract_idealista(local_data_folder, '.' + local_data_folder + '/idealista_source/')

    # create hdfs client and makedir
    hdfs_client = create_hdfs(hdfs_host, hdfs_port, hdfs_user, temp_landing_dir)

    # upload files
    if hdfs_client is not None:
        for file_path in local_paths_income:
            upload_file_hdfs(hdfs_client, temp_landing_dir, os.path.join(local_data_folder + '/' + file_path), '/income/')

        upload_file_hdfs(hdfs_client, temp_landing_dir, os.path.join(local_data_folder + '/' + local_path_elections), '/elections/')

        for file_path in local_path_idealista:
            upload_file_hdfs(hdfs_client, temp_landing_dir, os.path.join(local_data_folder + '/' + file_path), '/idealista/')

