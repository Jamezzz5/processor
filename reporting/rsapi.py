import os
import sys
import json
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl

config_path = utl.config_path
games_url = 'https://api.marketing.gamesight.io/games'
games_version = '1.1.0'
stats_url = 'https://api.marketing.gamesight.io/stats'
stats_version = '2.0.0'

def_groups = ['network', 'clicked_at_date', 'campaign', 'ad_group', 'ad',
              'sub1', 'sub2', 'sub3', 'sub4', 'sub5']
def_fields = ['clicks', 'converted_users', 'conversion_rate', 'launches',
              'launches_per_user', 'average_user_retention', 'impressions']
nested_cols = ['goals']
def_fields.extend(nested_cols)


display_groups = ['network', 'clicked_at_date', 'campaign', 'ad_group', 'ad']
display_fields = ['impressions', 'clicks']
display_nested_cols = ['network_reported_performance', 'goals']
display_fields.extend(display_nested_cols)


class RsApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.api_key = None
        self.game_name = None
        self.campaign_filter = None
        self.game_id = None
        self.headers = None
        self.config_list = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading RS config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()
        self.set_headers(games_version)

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
        if 'campaign_filter' in self.config:
            self.campaign_filter = self.config['campaign_filter']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in RS config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self, version):
        self.headers = {'Authorization': self.api_key,
                        'Content-Type': 'application/json',
                        'X-Api-Version': version}

    def get_id(self):
        r = requests.get(games_url, headers=self.headers)
        self.game_id = [x['id'] for x in r.json()['games']
                        if x['name'] == self.game_name]
        self.game_id = int(self.game_id[0])

    def date_check(self, sd, ed):
        sd, ed = self.get_data_default_check(sd, ed)
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date '
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

    def get_request_data(self, sd, ed, fields):
        r_data = {'groups': def_groups,
                  'fields': def_fields,
                  'game_id': self.game_id,
                  'filters': {'start_date': sd,
                              'end_date': ed}}
        if fields:
            for field in fields:
                if field == 'Display':
                    r_data['groups'] = display_groups
                    r_data['fields'] = display_fields
        return r_data

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.date_check(sd, ed)
        self.get_id()
        self.get_raw_data(sd, ed, fields)
        return self.df

    def filter_df_on_campaign(self):
        logging.info('Filtering dataframe on {}'.format(self.campaign_filter))
        self.df = self.df[self.df['campaign'].fillna('0').str.contains(
            self.campaign_filter)].reset_index(drop=True)

    def get_raw_data(self, sd, ed, fields):
        logging.info('Getting data from {} to ed {}'.format(sd, ed))
        r_data = self.get_request_data(sd, ed, fields)
        self.set_headers(stats_version)
        self.r = requests.post(stats_url, headers=self.headers, json=r_data)
        self.df = self.data_to_df(fields)
        if self.campaign_filter:
            self.filter_df_on_campaign()
        logging.info('Data successfully retrieved returning dataframe.')

    def data_to_df(self, fields):
        try:
            json_data = self.r.json()['results']
        except ValueError:
            logging.warning('Could not load as json.  '
                            'Response: {}'.format(self.r.text))
            sys.exit(0)
        df = pd.DataFrame(json_data)
        if fields:
            for field in fields:
                if field == 'Display':
                    nested_cols.insert(0, 'network_reported_performance')
        for col in nested_cols:
            n_df = self.flatten_nested_cols(df, col)
            for n_col in n_df.columns:
                n2_df = self.flatten_nested_cols(n_df, n_col)
                for n2_col in n2_df.columns:
                    n3_df = self.flatten_nested_cols(n2_df, n2_col)
                    df = df.join(n3_df)
        df = df.drop(nested_cols, axis=1)
        return df

    @staticmethod
    def flatten_nested_cols(df, col):
        n_df = df[col].apply(pd.Series)
        n_df.columns = ['{} - {}'.format(col, x) for x in n_df.columns]
        return n_df
