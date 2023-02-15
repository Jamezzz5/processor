import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl


class NzAPI(object):
    config_path = utl.config_path
    base_url = "https://api.newzoo.com/public/engagement/games/data"

    def __init__(self):
        self.config = None
        self.config_file = None
        self.game_title = None
        self.api_key = None
        self.config_list = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Nz config file: {}'.format(config))
        self.config_file = os.path.join(self.config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.game_title = self.config['game_title']
        self.api_key = self.config['api_key']
        self.config_list = [self.config, self.api_key, self.game_title]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Nz config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        header = self.create_header()
        params = {
            'start_date': sd,
            'end_date': ed,
            'games': [self.game_title]
        }
        self.r = self.make_request('get', self.base_url, params=params,
                                   header=header)
        self.df = pd.DataFrame(self.r)
        return self.df

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today()
        if ed is None:
            ed = dt.datetime.today()
        return sd, ed

    def make_request(self, method, url, params=None, body=None, header=None):
        try:
            response = self.raw_request(method, url, params=params,
                                        body=body, header=header)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            response = self.make_request(method, url, params=params,
                                         body=body, header=header)
        return response

    @staticmethod
    def raw_request(method, url, params=None, body=None, header=None):
        if method == 'post':
            response = requests.post(url, json=body, headers=header)
        elif method == 'get':
            response = requests.get(url, params=params, headers=header)
        else:
            response = None
        return response

    def create_header(self):
        header = {
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + self.api_key,
        }
        return header
