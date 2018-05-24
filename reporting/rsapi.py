import sys
import json
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl

config_path = utl.config_path
games_url = 'https://api.redshell.io/games'
stats_url = 'https://api.redshell.io/stats'

def_groups = ['campaign_id', 'date', 'aff_sub2', 'aff_sub3', 'aff_sub4',
              'aff_sub5']
def_fields = ['clicks', 'converted_users', 'conversion_rate', 'launches',
              'launches_per_user', 'average_user_retention']
nested_cols = ['retention', 'custom_events']
def_fields.extend(nested_cols)


class RsApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.api_key = None
        self.game_name = None
        self.game_id = None
        self.headers = None
        self.config_list = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  ' +
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading RS config file: {}'.format(config))
        self.config_file = config_path + config
        self.load_config()
        self.check_config()
        self.set_headers()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.api_key = self.config['api_key']
        self.game_name = self.config['game_name']
        self.config_list = [self.api_key, self.game_name]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in RS config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        self.headers = {'Authorization': self.api_key,
                        'Content-Type': 'application/json',
                        'X-Api-Version': '1.1'}

    def get_id(self):
        r = requests.get(games_url, headers=self.headers)
        self.game_id = [x['id'] for x in r.json()['games']
                        if x['name'] == self.game_name]
        self.game_id = int(self.game_id[0])

    def date_check(self, sd, ed):
        sd, ed = self.get_data_default_check(sd, ed)
        if sd > ed:
            logging.warning('Start date greater than end date.  Start data' +
                            'was set to end date.')
            sd = ed - dt.timedelta(days=1)
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        return sd, ed

    def get_request_data(self, sd, ed):
        r_data = {'groups': def_groups,
                  'fields': def_fields,
                  'game_id': self.game_id,
                  'filters': {'start_date': sd,
                              'end_date': ed}}
        return r_data

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.date_check(sd, ed)
        self.get_id()
        self.get_raw_data(sd, ed)
        return self.df

    def get_raw_data(self, sd, ed):
        r_data = self.get_request_data(sd, ed)
        self.r = requests.post(stats_url, headers=self.headers, json=r_data)
        self.df = self.data_to_df()

    def data_to_df(self):
        df = pd.DataFrame(self.r.json()['results'])
        for col in nested_cols:
            n_df = self.flatten_nested_cols(df, col)
            for n_col in n_df.columns:
                n2_df = self.flatten_nested_cols(n_df, n_col)
                df = df.join(n2_df)
        df = df.drop(nested_cols, axis=1)
        return df

    @staticmethod
    def flatten_nested_cols(df, col):
        n_df = df[col].apply(pd.Series)
        n_df.columns = ['{} - {}'.format(col, x) for x in n_df.columns]
        return n_df
