import argparse
from hdfs import InsecureClient
from src import data_collector as dc
from src import persistence_loader_hdfs as pl

# Configuration HDFS
HDFS_HOST = "10.4.41.48"
HDFS_PORT = "9870"
HDFS_USER = "bdm"
TMP_LANDING_DIR = "/temporal_landing"
PER_LANDING_DIR = "/persistent_landing"
HDFS_DIRECTORIES = ['income', 'elections', 'idealista']

def create_hdfs(hdfs_host, hdfs_port, hdfs_user, per_landing_dir):
    try:
        client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user=hdfs_user)
        print(f"Connection to HDFS has been established successfully.")
        if client.status(per_landing_dir, strict=False) is None:
            client.makedirs(per_landing_dir)
        
        return client

    except Exception as e:
        client.close()
        print(e)
        return None

def main():
    parser = argparse.ArgumentParser(description='Temporal Landing Zone')

    parser.add_argument('exec_mode', type=str, choices=['data-collector', 'persistence-loader'], help='Execution mode')

    args = parser.parse_args()
    exec_mode = args.exec_mode

    if exec_mode == 'data-collector':

        try:
            print('Starting data collector process')
            # create hdfs client and makedir
            hdfs_client = create_hdfs(HDFS_HOST, HDFS_PORT, HDFS_USER, TMP_LANDING_DIR)

            # run data collector
            dc.main(hdfs_client, TMP_LANDING_DIR)

            print('Finished succesfully data collector process')

        except Exception as e:
            print(f'Error occurred during data collection: {e}')

    elif exec_mode == 'persistence-loader':

        try:
            print('Starting persistence loader process')

            # connect to hdfs client and makedir persistent
            hdfs_client = create_hdfs(HDFS_HOST, HDFS_PORT, HDFS_USER, PER_LANDING_DIR)
            # run persistent loader
            pl.main(hdfs_client, TMP_LANDING_DIR, PER_LANDING_DIR, HDFS_DIRECTORIES)

            print('Finished succesfully persistence loader process')

        except Exception as e:
            print(f'Error occurred during persistence loading: {e}')

if __name__ == '__main__':
    main()