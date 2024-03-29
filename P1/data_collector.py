import requests
import os
import io
import pandas as pd


def extract_open_data_bcn_income(data_folder):
    urls = {'2017': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=e7206797-e57b-4ded-8c6c-62e9b4cb54f7',
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
    data_folder = data_folder + "/opendatabcn_income"

    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    for year in urls.keys():
        extract_open_data_bcn_datasets(urls[year]).to_csv(os.path.join(data_folder, year+'_Distribucio_territorial_renda_familiar.csv'))

def extract_open_data_bcn_elections(data_folder):
    url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=e8fce35e-46b9-429e-a29f-945c33a3a8ef"
    data_folder = data_folder + "/opendatabcn_elections"
    
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    extract_open_data_bcn_datasets(url).to_csv(os.path.join(data_folder, '2023_07_23_Eleccions_Congres_Diputats.csv'))

def extract_open_data_bcn_datasets(url):
    response = requests.get(url)
    df = pd.read_json(io.StringIO(response.content.decode('utf-8')))
    df_result = pd.DataFrame(df.loc['records','result'])
    # print(df_result.head())
    return df_result


if __name__ == "__main__":
    data_folder = r"C:\Users\Silvia\OneDrive - Universitat Politècnica de Catalunya\Escritorio\UPC\MASTER DS\1B\BDM\P1\data"

    extract_open_data_bcn_income(data_folder)
    extract_open_data_bcn_elections(data_folder)


