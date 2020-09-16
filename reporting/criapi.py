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


class CriApi(object):
    base_url = 'https://rm-api.criteo.com'
    auth_url = '{}/oauth2/token'.format(base_url)

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.advertiser_id = None
        self.campaign_id = None
        self.ad_id_list = []
        self.config_list = None
        self.headers = None
        self.version = '2'
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Cri config file: {}'.format(config))
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
        self.advertiser_id = self.config['advertiser_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.advertiser_id]
        if 'campaign_id' in self.config:
            self.campaign_id = self.config['campaign_id']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Cri config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        data = {'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'client_credentials'}
        r = self.make_request(self.auth_url, method='POST', data=data)
        token = r.json()['access_token']
        self.headers = {'Authorization': 'Bearer {}'.format(token)}

    def make_request(self, url, method, headers=None, json_body=None, data=None,
                     params=None, attempt=1):
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
                logging.warning('Could not connection with error: {}'.format(e))
                r = None
            else:
                logging.warning('Connection error, pausing for 60s '
                                'and retrying: {}'.format(e))
                time.sleep(60)
                r = self.make_request(url, method, headers, json_body, attempt)
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
        self.df = self.request_and_get_data(sd, ed)
        return self.df

    def request_and_get_data(self, sd, ed):
        logging.info('Getting data form {} to {}'.format(sd, ed))
        self.set_headers()
        url = '{}/v1/account/{}/campaign-activity-by-day'.format(
            self.base_url, self.advertiser_id)
        params = {'dateFrom': sd, 'dateTo': ed, 'timezone': 'EDT'}
        r = self.make_request(url, method='GET', headers=self.headers,
                              params=params)
        if 'data' not in r.json():
            logging.warning('data was not in response returning blank df: '
                            '{}'.format(r.json()))
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(r.json()['data'], columns=r.json()['columns'])
            logging.info('Data successfully retrieved returning df.')
        return df
