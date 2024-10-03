import os
import sys
import json
import logging
import requests
import datetime as dt
import pandas as pd
import processor.reporting.utils as utl

config_path = utl.config_path

base_url = 'https://api.vk.com/method/'


class VkApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.act_id = None
        self.config_list = None
        self.client = None
        self.df = pd.DataFrame()
        self.r = None
        self.base_params = None
        self.adids = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  ' +
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading VK config file: {}'.format(config))
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
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.access_token = self.config['access_token']
        self.act_id = self.config['act_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.access_token, self.act_id]
        self.base_params = self.get_url_params()

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not found in config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    @staticmethod
    def date_check(sd, ed):
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date' +
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

    def get_url_params(self, stats=False, sd=None, ed=None, ids=None):
        params = {'v': '5.52',
                  'access_token': self.access_token,
                  'account_id': self.act_id}
        if stats:
            params['period'] = 'day'
            params['ids_type'] = 'ad'
            params['date_from'] = sd
            params['date_to'] = ed
            params['ids'] = ','.join(ids)
        return params

    @staticmethod
    def create_url(method):
        method_url = '{}'.format(method)
        full_url = base_url + method_url
        return full_url

    def get_dicts(self, method):
        url = self.create_url(method)
        r = requests.get(url, params=self.base_params)
        data = r.json()['response']
        return data

    def get_ads(self):
        method = 'ads.getAds'
        data = self.get_dicts(method)
        data = {x['id']: {'Ad Name': x['name'],
                          'campaign_id': x['campaign_id']} for x in data}
        return data

    def get_campaigns(self):
        method = 'ads.getCampaigns'
        data = self.get_dicts(method)
        data = {str(x['id']): {'Campaign': x['name']} for x in data}
        return data

    def get_data(self, sd=None, ed=None, fields=None):
        self.df = pd.DataFrame()
        sd, ed = self.get_data_default_check(sd, ed)
        sd, ed = self.date_check(sd, ed)
        self.adids = self.get_ads()
        self.get_raw_data(sd, ed, self.adids)
        self.add_names_to_df()
        return self.df

    def get_raw_data(self, sd, ed, adids):
        adids = list(adids.keys())
        params = self.get_url_params(True, sd, ed, adids)
        url = self.create_url('ads.getStatistics')
        self.r = requests.get(url, params=params)
        if self.r.status_code == 200:
            self.df = self.data_to_df(self.r)
        else:
            self.request_error()

    def request_error(self):
        logging.warning('Unknown error: {}'.format(self.r.text))
        sys.exit(0)

    @staticmethod
    def data_to_df(r):
        df = pd.DataFrame()
        data = pd.DataFrame(r.json()['response'])
        stats = data['stats'].apply(pd.Series)
        ids = pd.DataFrame(data['id'])
        for col in stats.columns:
            tdf = stats[col].apply(pd.Series)
            tdf = tdf.drop(0, axis=1)
            tdf = tdf.dropna(how='all')
            df = pd.concat([df, tdf])
        df = df.join(ids)
        df = df.reset_index(drop=True)
        return df

    def add_names_to_df(self):
        self.df = self.dict_to_cols(self.df, 'id',
                                    self.adids)  # type: pd.DataFrame
        cids = self.get_campaigns()
        self.df = self.dict_to_cols(self.df, 'campaign_id',
                                    cids)  # type: pd.DataFrame
        self.df = utl.col_removal(self.df, 'API_VK', ['id', 'campaign_id'])

    @staticmethod
    def dict_to_cols(df, col, map_dict):
        df[col] = df[col].astype('U').map(map_dict).fillna('None')
        df = pd.concat([df, df[col].apply(pd.Series)], axis=1)
        return df
