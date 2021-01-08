import io
import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl

config_path = utl.config_path


class AdkApi(object):
    auth_url = 'https://public-api.adikteev.com/token'
    base_url = 'https://public-api.adikteev.com'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.username = None
        self.password = None
        self.api_key = None
        self.campaign_filter = None
        self.config_list = None
        self.company_id = None
        self.headers = None
        self.cid_dict = {}
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Adikteev config file: {}'.format(config))
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
        self.username = self.config['username']
        self.password = self.config['password']
        self.api_key = self.config['api_key']
        self.campaign_filter = self.config['campaign_filter']
        self.company_id = self.config['company_id']
        self.config_list = [self.config, self.username, self.password,
                            self.api_key, self.company_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Adikteev config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        self.headers = {'authorization': self.api_key}
        data = {'username': self.username, 'password': self.password,
                'grant_type': 'password'}
        r = self.make_request(self.auth_url, method='POST', data=data,
                              headers=self.headers)
        access_token = r.json()['access_token']
        self.headers['authorization'] = 'Bearer {}'.format(access_token)

    def make_request(self, url, method, headers=None, json_body=None, data=None,
                     params=None, attempt=1, json_response=False):
        if not json_body:
            json_body = {}
        if not headers:
            headers = {}
        if not data:
            data = {}
        if not params:
            params = {}
        if method == 'POST':
            request_method = requests.post
        else:
            request_method = requests.get
        try:
            r = request_method(url, headers=headers, json=json_body, data=data,
                               params=params)
        except requests.exceptions.ConnectionError as e:
            attempt += 1
            if attempt > 100:
                logging.warning('Could not connect with error: {}'.format(e))
                r = None
            else:
                logging.warning('Connection error, pausing for 60s '
                                'and retrying: {}'.format(e))
                time.sleep(60)
                r = self.make_request(url, method, headers, json_body, attempt,
                                      json_response)
        if json_response:
            try:
                r.json()
            except:
                attempt += 1
                if attempt > 100:
                    logging.warning(
                        'Could not connect with error: {}'.format(e))
                    r = None
                else:
                    logging.warning('No json in response retrying: {}')
                    r = self.make_request(url, method, headers, json_body,
                                          attempt, json_response)
        return r

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    def get_data_default_check(self, sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() + dt.timedelta(days=1)
        if dt.datetime.today().date() == ed.date():
            ed += dt.timedelta(days=1)
        sd, ed = self.date_check(sd, ed)
        return sd, ed

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        app_ids = self.get_app_ids()
        self.get_campaign_ids(app_ids)
        self.df = self.request_and_get_data(sd, ed)
        self.df = self.clean_df(self.df)
        return self.df

    def get_app_ids(self):
        self.set_headers()
        url = self.base_url + '/apps'
        r = self.make_request(url, method='GET', headers=self.headers,
                              params={'company_id': int(self.company_id)})
        if self.campaign_filter:
            app_ids = [x['id'] for x in r.json()['items']
                       if self.campaign_filter in x['name']]
        else:
            app_ids = [x['id'] for x in r.json()['items']]
        return app_ids

    def get_campaign_ids(self, app_ids):
        url = self.base_url + '/client_campaigns'
        for app_id in app_ids:
            r = self.make_request(url, method='GET', headers=self.headers,
                                  params={'app_id': int(app_id)})
            cid_dict = {x['id']: x['name'] for x in r.json()['items']}
            self.cid_dict.update(cid_dict)

    def request_and_get_data(self, sd, ed):
        logging.info('Getting data from {} to {}'.format(sd, ed))
        cids = ','.join([str(x) for x in self.cid_dict.keys()])
        url = self.base_url + '/report/company/creative/daily/client'
        params = {'campaign_ids': cids, 'start_date': sd, 'end_date': ed}
        r = self.make_request(url, method='GET', headers=self.headers,
                              params=params, json_response=True)
        df = pd.DataFrame(r.json()['data'])
        logging.info('Data retrieved.')
        return df

    def clean_df(self, df):
        df['campaign_name'] = df['campaign_id'].map(self.cid_dict)
        df = df.rename(columns={'client_cost': 'spend'})
        df['date'] = pd.to_datetime(df['date']).dt.date
        return df
