import os
import sys
import json
import logging
import requests
import pandas as pd
import reporting.utils as utl

config_path = utl.config_path


class AzuApi(object):
    base_url = 'https://{}.blob.core.windows.net/{}?{}'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.account_name = None
        self.container_name = None
        self.sas_token = None
        self.config_list = None
        self.df = pd.DataFrame()
        self.header = None
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Azure config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.account_name = self.config['storage_account_name']
        self.container_name = self.config['container_name']
        self.sas_token = self.config['sas_token']
        self.config_list = [self.account_name, self.container_name,
                            self.sas_token]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Azure config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def create_url(self, key):
        url = self.base_url.format(self.account_name, key, self.sas_token)
        return url

    @staticmethod
    def set_headers():
        headers = {
            'x-ms-blob-type': 'BlockBlob',
            'Content-Encoding': 'gzip',
            'Content-Type': 'application/octet-stream'
        }
        return headers

    def get_data(self, sd=None, ed=None, fields=None):
        return self.df

    def write_file(self, df, file_name='raw', default_format=True):
        buffer, zip_file = utl.write_df_to_buffer(
            df, file_name=file_name, default_format=default_format,
            base_folder=self.container_name)
        url = self.create_url(key=zip_file)
        headers = self.set_headers()
        data = buffer.getvalue()
        headers['Content-Length'] = str(len(data))
        r = requests.put(url=url, headers=headers, data=data)
        if r.status_code == 201:
            logging.info('File successfully uploaded to {}'.format(zip_file))
        else:
            logging.warning('failed to upload. Status Code: {}'
                            .format(r.status_code))
            logging.info('Response: {}'.format(r.text))
