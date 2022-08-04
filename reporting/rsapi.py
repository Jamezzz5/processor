import os
import sys
import time
import json
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl

config_path = utl.config_path


class RsApi(object):
    games_url = 'https://api.marketing.gamesight.io/games'
    games_version = '1.1.0'
    stats_url = 'https://api.marketing.gamesight.io/stats'
    stats_version = '3.0.0'
    def_groups = [
        "game_id", "team_id", "network", "campaign", "ad_group", "ad",
        "platform", "ad_type", "placement", "creative",
        "sub1", "sub2", "sub3", "sub4", "sub5", "sub6", "sub7", "sub8", "sub9",
        "sub10"]
    def_fields = [
        "impressions", "impressions_unique", "clicks", "clicks_unique",
        "click_through_rate", "cost_amount", "cost_currency",
        "network_impressions", "network_clicks", "network_click_through_rate",
        "network_cost_amount", "network_cost_currency", "display_impressions",
        "display_clicks", "display_click_through_rate", "display_cost_amount",
        "display_cost_currency", "goal_rate", "goal_revenue_amount",
        "goal_revenue_currency", "steam_total_visits", "steam_tracked_visits",
        "steam_wishlists", "steam_purchases", "steam_activations"]
    nested_cols = ['goals']
    def_fields.extend(nested_cols)
    date_trigger = ['triggered_at_date']
    date_click = ['clicked_at_date']
    def_groups.extend(date_click)

    display_groups = ['network', 'clicked_at_date', 'campaign', 'ad_group',
                      'ad']
    display_fields = ['impressions', 'clicks']
    display_nested_cols = ['network_reported_performance', 'goals']
    display_fields.extend(display_nested_cols)

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
            sys.exit('Config file name not in vendor matrix.  '
                     'Aborting.')
        logging.info('Loading RS config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()
        self.set_headers(self.games_version)

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            sys.exit('{} not found.  Aborting.'.format(self.config_file))
        self.api_key = self.config['api_key']
        self.game_name = self.config['game_name']
        self.config_list = [self.api_key, self.game_name]
        if 'campaign_filter' in self.config:
            self.campaign_filter = self.config['campaign_filter']
        if 'domain' in self.config and self.config['domain']:
            self.stats_url = self.config['domain']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                sys.exit('{} not in RS config file.  '
                         'Aborting.'.format(item))

    def set_headers(self, version):
        self.headers = {'Authorization': self.api_key,
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'X-Api-Version': version}
        return self.headers

    def get_id(self):
        r = requests.get(self.games_url, headers=self.headers)
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
        r_data = {'groups': self.def_groups,
                  'fields': self.def_fields,
                  'game_id': self.game_id,
                  'filters': {'start_date': sd,
                              'end_date': ed,
                              'include_unattributed':  False}}
        if fields:
            for field in fields:
                if field == 'Display':
                    r_data['groups'] = self.display_groups
                    r_data['fields'] = self.display_fields
                if field == 'DateTriggered':
                    group = [x for x in self.def_groups if
                             x not in self.date_click] + self.date_trigger
                    r_data['groups'] = group
                    r_data['filters'] = {'start_triggered_at_date': sd,
                                         'end_triggered_at_date': ed,
                                         'include_unattributed': False}
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

    def request_error(self):
        sys.exit('Unknown error: {}'.format(self.r.text))

    def raw_request(self, url, method, body=None, params=None, headers=None):
        kwargs = {}
        for kwarg in [(body, 'json'), (params, 'params'), (headers, 'headers')]:
            if kwarg[0]:
                kwargs[kwarg[1]] = kwarg[0]
        if method == 'POST':
            request_method = requests.post
        else:
            request_method = requests.get
        self.r = request_method(url, **kwargs)
        return self.r

    def make_request(self, url, method, body=None, params=None,
                     attempt=1, json_response=True):
        headers = self.set_headers(self.stats_version)
        try:
            self.r = self.raw_request(url, method, body=body, params=params,
                                      headers=headers)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url=url, method=method, body=body,
                                       params=params, attempt=attempt,
                                       json_response=json_response)
        if json_response:
            try:
                self.r.json()
            except ValueError:
                logging.warning(
                    'Request error.  Retrying {}'.format(self.r.text))
                time.sleep(30)
                attempt += 1
                if attempt > 10:
                    self.request_error()
                self.r = self.make_request(url=url, method=method, body=body,
                                           params=params, attempt=attempt,
                                           json_response=json_response)
        return self.r

    def get_raw_data(self, sd, ed, fields):
        logging.info('Getting data from {} to ed {}'.format(sd, ed))
        r_data = self.get_request_data(sd, ed, fields)
        self.r = self.make_request(self.stats_url, method='POST', body=r_data)
        self.df = self.data_to_df(fields)
        if self.campaign_filter:
            self.filter_df_on_campaign()
        logging.info('Data successfully retrieved returning dataframe.')

    def data_to_df(self, fields):
        try:
            json_data = self.r.json()['results']
        except ValueError:
            sys.exit('Could not load as json.  '
                     'Response: {}'.format(self.r.text))
        df = pd.DataFrame(json_data)
        if fields:
            for field in fields:
                if field == 'Display':
                    self.nested_cols.insert(0, 'network_reported_performance')
        for col in self.nested_cols:
            n_df = self.flatten_nested_cols(df, col)
            for n_col in n_df.columns:
                n2_df = self.flatten_nested_cols(n_df, n_col)
                for n2_col in n2_df.columns:
                    n3_df = self.flatten_nested_cols(n2_df, n2_col)
                    df = df.join(n3_df)
        df = df.drop(self.nested_cols, axis=1)
        return df

    @staticmethod
    def flatten_nested_cols(df, col):
        n_df = df[col].apply(pd.Series)
        n_df.columns = ['{} - {}'.format(col, x) for x in n_df.columns]
        return n_df
