import os
import io
import sys
import gzip
import json
import time
import logging
import requests
import pandas as pd
import reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path


class GcpApi(object):
    base_url = 'https://storage.googleapis.com/upload/storage/v1/b/'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.config_list = None
        self.client = None
        self.bucket_name = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            sys.exit('Config file name not in vendor matrix.  '
                     'Aborting.')
        logging.info('Loading GCP config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            sys.exit('{} not found.  Aborting.'.format(self.config_file))
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.access_token = self.config['access_token']
        self.refresh_token = self.config['refresh_token']
        self.refresh_url = self.config['refresh_url']
        self.bucket_name = self.config['bucket_name']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                sys.exit('{} not in GCP config file.'
                         'Aborting.'.format(item))

    def refresh_client_token(self, extra, attempt=1):
        try:
            token = self.client.refresh_token(self.refresh_url, **extra)
        except requests.exceptions.ConnectionError as e:
            attempt += 1
            if attempt > 100:
                logging.warning('Max retries exceeded: {}'.format(e))
                token = None
            else:
                logging.warning('Connection error retrying 60s: {}'.format(e))
                token = self.refresh_client_token(extra, attempt)
        return token

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.refresh_client_token(extra)
        self.client = OAuth2Session(self.client_id, token=token)

    def write_file(self, df, file_name='raw'):
        csv_file = '{}{}'.format(file_name, '.csv')
        buffer = io.BytesIO()
        with gzip.GzipFile(filename=csv_file, fileobj=buffer, mode="wb") as f:
            f.write(df.to_csv().encode())
        buffer.seek(0)
        url = '{}b/{}/o?uploadType=media&name={}'.format(
            self.base_url, self.bucket_name, file_name)
        headers = {
            'Authorization': 'Bearer {}'.format(self.config['access_token']),
            'Content-Type': 'text/csv'}
        self.make_request(url, method='POST', headers=headers)

    def get_data(self, sd=None, ed=None, fields=None):
        return self.df

    def make_request(self, url, method, body=None, params=None, attempt=1,
                     json_response=True):
        self.get_client()
        try:
            self.r = self.raw_request(url, method, body=body, params=params)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url=url, method=method, body=body,
                                       params=params, attempt=attempt,
                                       json_response=json_response)
        if json_response and 'error' in self.r.json():
            logging.warning('Request error.  Retrying {}'.format(self.r.json()))
            time.sleep(30)
            attempt += 1
            if attempt > 10:
                self.request_error()
            self.r = self.make_request(url=url, method=method, body=body,
                                       params=params, attempt=attempt,
                                       json_response=json_response)
        return self.r

    def raw_request(self, url, method, body=None, params=None):
        if method == 'get':
            if body:
                self.r = self.client.get(url, json=body, params=params)
            else:
                self.r = self.client.get(url, params=params)
        elif method == 'post':
            if body:
                self.r = self.client.post(url, json=body, params=params)
            else:
                self.r = self.client.post(url, params=params)
        return self.r

    def request_error(self):
        sys.exit('Unknown error: {}'.format(self.r.text))
