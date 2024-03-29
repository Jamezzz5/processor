import os
import sys
import json
import time
import logging
import requests
import numpy as np
import pandas as pd
import datetime as dt
import reporting.utils as utl


class NzApi(object):
    config_path = utl.config_path
    base_url = 'https://api.newzoo.com/public'
    engagement_url = '/engagement'
    games_url = '/games/data'
    viewership_url = '/viewership'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.game_title = None
        self.api_key = None
        self.country_filter = None
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
        if 'country_filter' in self.config:
            self.country_filter = self.config['country_filter']
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
        request_list = self.parse_fields(sd, ed, fields)
        self.df = pd.DataFrame()
        for req in request_list:
            name, url, params = req
            self.r = self.make_request('get', url, params=params,
                                       header=header)
            df = self.get_df_from_response(self.r, name)
            if self.df.empty:
                self.df = df
            else:
                self.df = self.df.merge(df, how='outer')
        return self.df

    @staticmethod
    def get_df_from_response(response, name):
        df = pd.DataFrame()
        translation_dict = {}
        data = response.json()
        if 'message' in data:
            logging.error('Unexpected Error {}'.format(data))
            return df
        if name == 'viewership':
            data = response.json()['data']
            translation_dict = {'timestamp': 'date', 'rank': 'viewership_rank',
                                'rank_change': 'viewership_rank_change'}
        if data:
            list_cols = [col for col in data[0] if type(data[0][col]) == list]
            df = pd.DataFrame(data)
            for col in list_cols:
                df[col] = df[col].apply(sorted).apply(tuple)
            df = df.rename(columns=translation_dict)
            df.fillna(np.nan)
        logging.info('Successful response. Returning dataframe.')
        return df

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today()
        if ed is None:
            ed = dt.datetime.today()
        return sd, ed

    def parse_fields(self, sd, ed, fields=None):
        engage_url = self.engagement_url
        category_url = self.games_url
        name = 'default'
        default_params = {
            'start_date': sd.strftime("%Y-%m-%d"),
            'end_date': ed.strftime("%Y-%m-%d"),
            'games': self.game_title.split(',')
        }
        if self.country_filter:
            default_params['geo_type'] = 'markets'
            default_params['markets'] = self.country_filter.split(',')
        url = '{}{}{}'.format(self.base_url, engage_url, category_url)
        request_list = [(name, url, default_params)]
        if fields:
            for field in fields:
                if field == 'Viewership':
                    engage_url = self.viewership_url
                    name = 'viewership'
                else:
                    continue
                url = '{}{}{}'.format(self.base_url, engage_url, category_url)
                request_list.append((name, url, default_params))
        return request_list

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
        time.sleep(1)
        return response

    def create_header(self):
        header = {
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.api_key),
        }
        return header
