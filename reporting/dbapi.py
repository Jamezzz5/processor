import sys
import json
import time
import logging
import pandas as pd
import datetime as dt
import reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path
base_url = 'https://www.googleapis.com/doubleclickbidmanager/v1'


class DbApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.report_id = None
        self.config_list = None
        self.client = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  ' +
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading DB config file: {}'.format(config))
        self.config_file = config_path + config
        self.load_config()
        self.check_config()
        self.config_file = config_path + config

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.access_token = self.config['access_token']
        self.refresh_token = self.config['refresh_token']
        self.refresh_url = self.config['refresh_url']
        self.report_id = self.config['report_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in DB config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.client.refresh_token(self.refresh_url, **extra)
        self.client = OAuth2Session(self.client_id, token=token)

    def create_url(self):
        query_url = '/query/{}'.format(self.report_id)
        full_url = (base_url + query_url)
        return full_url

    def get_data(self, sd=None, ed=None, fields=None):
        self.get_client()
        self.get_raw_data()
        return self.df

    def get_raw_data(self):
        full_url = self.create_url()
        for x in range(1, 101):
            self.r = self.client.get(full_url)
            if 'metadata' in self.r.json().keys():
                break
            else:
                logging.warning('Rate limit exceeded. Pausing. '
                                'Response: {}'.format(self.r.json()))
        report_url = (self.r.json()['metadata']
                      ['googleCloudStoragePathForLatestReport'])
        self.df = pd.read_csv(report_url)
