import os
import sys
import json
import time
import pytz
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path

access_token_url = 'https://accounts.snapchat.com/login/oauth2/access_token'
base_url = 'https://adsapi.snapchat.com/v1/'

def_fields = ['impressions', 'swipes', 'view_time_millis', 'quartile_1',
              'quartile_2', 'quartile_3', 'view_completion', 'spend',
              'video_views', 'shares', 'saves']


class ScApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.ad_account_id = None
        self.config_list = None
        self.client = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading SC config file: {}'.format(config))
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
        self.refresh_token = self.config['refresh_token']
        self.ad_account_id = self.config['ad_account_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.access_token, self.ad_account_id,
                            self.refresh_token]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in SC config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 1800,
                 'expires_at': 1521743516.5867176}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.client.refresh_token(access_token_url, **extra)
        self.client = OAuth2Session(self.client_id, token=token)

    def date_check(self, sd, ed):
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date '
                            'was set to end date.')
            sd = ed - dt.timedelta(days=1)
        timezone = self.get_account_timezone()
        sd = pytz.timezone(timezone).localize(sd).isoformat()
        ed = pytz.timezone(timezone).localize(ed).isoformat()
        return sd, ed

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        return sd, ed

    @staticmethod
    def create_url(cid):
        camp_url = 'campaigns/{}/stats?'.format(cid)
        break_url = 'breakdown=ad'
        field_url = '&fields={}'.format(','.join(def_fields))
        gran_url = '&granularity=DAY'
        full_url = (base_url + camp_url + break_url + field_url + gran_url)
        return full_url

    def make_request(self, add_url):
        act_url = base_url + add_url
        self.get_client()
        try:
            r = self.client.get(act_url)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            r = self.make_request(add_url)
        return r

    def get_account_timezone(self):
        act_url = 'adaccounts/{}'.format(self.ad_account_id)
        r = self.make_request(act_url)
        timezone = r.json()['adaccounts'][0]['adaccount']['timezone']
        return timezone

    def get_campaigns(self):
        act_url = 'adaccounts/{}/campaigns'.format(self.ad_account_id)
        r = self.make_request(act_url)
        cids = {x['campaign']['id']: x['campaign']['name']
                for x in r.json()['campaigns']}
        return cids

    def get_adsquads(self):
        act_url = 'adaccounts/{}/adsquads'.format(self.ad_account_id)
        r = self.make_request(act_url)
        asids = {x['adsquad']['id']: x['adsquad']['name']
                 for x in r.json()['adsquads']}
        return asids

    def get_ads(self):
        act_url = 'adaccounts/{}/ads'.format(self.ad_account_id)
        r = self.make_request(act_url)
        adids = {x['ad']['id']: {'name': x['ad']['name'],
                                 'ad_squad_id': x['ad']['ad_squad_id']}
                 for x in r.json()['ads']}
        return adids

    def get_data(self, sd=None, ed=None, fields=None):
        self.df = pd.DataFrame()
        sd, ed = self.get_data_default_check(sd, ed)
        sd, ed = self.date_check(sd, ed)
        cids = self.get_campaigns()
        for cid in cids:
            self.get_raw_data(sd, ed, cid)
        self.df['Campaign Name'] = self.df['Campaign Name'].map(cids)
        self.df = self.add_names_to_df()
        return self.df

    def get_raw_data(self, sd, ed, cid):
        full_url = self.create_url(cid)
        self.r = self.client.get(full_url,
                                 params={'start_time': sd, 'end_time': ed})
        if self.r.status_code == 200:
            tdf = self.data_to_df(self.r)
            tdf['Campaign Name'] = cid
            self.df = self.df.append(tdf)
        else:
            self.request_error(sd, ed, cid)

    def request_error(self, sd, ed, cid):
        limit_error = 'Limit reached for'
        if self.r.status_code == 403 and self.r.text[:17] == limit_error:
            logging.warning('Limit reached pausing for 120 seconds.')
            time.sleep(120)
            self.get_raw_data(sd, ed, cid)
        else:
            logging.warning('Unknown error: ' + str(self.r.text))
            sys.exit(0)

    @staticmethod
    def data_to_df(r):
        df = pd.DataFrame()
        data = (r.json()['timeseries_stats'][0]['timeseries_stat']
                        ['breakdown_stats']['ad'])
        for ad_data in data:
            tdf = pd.DataFrame(ad_data['timeseries'])  # type: pd.DataFrame
            tdf = pd.concat([tdf, tdf['stats'].apply(pd.Series)], axis=1)
            tdf['id'] = ad_data['id']  # type: pd.DataFrame
            df = df.append(tdf)
        if 'spend' in df.columns:
            df['spend'] = df['spend'] / 1000000
        df = df.reset_index()
        return df

    def add_names_to_df(self):
        if self.df.empty:
            logging.warning('No data for date range, returning empty df.')
            return self.df
        adids = self.get_ads()
        self.df = self.dict_to_cols(self.df, 'id', adids)  # type: pd.DataFrame
        asids = self.get_adsquads()
        self.df = self.dict_to_cols(self.df, 'ad_squad_id', asids)  # type: pd.DataFrame
        self.df = self.df.rename(columns={'ad_squad_id': 'Ad Set Name',
                                          'name': 'Ad Name'})
        self.df = utl.col_removal(self.df, 'API_Snapchat',
                                  [0, 'id', 'stats', 'index'])
        return self.df

    @staticmethod
    def dict_to_cols(df, col, map_dict):
        df[col] = df[col].map(map_dict).fillna('None')
        df = pd.concat([df, df[col].apply(pd.Series)], axis=1)
        return df
