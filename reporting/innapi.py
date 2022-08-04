import io
import os
import sys
import json
import time
import base64
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl

config_path = utl.config_path


class InnApi(object):
    auth_url = 'https://papi.innovid.com/v3/authenticate'
    base_url = 'https://papi.innovid.com/v3'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.username = None
        self.password = None
        self.advertiser = None
        self.campaign_filter = None
        self.config_list = None
        self.headers = None
        self.response_tokens = []
        self.aid_dict = {}
        self.cid_dict = {}
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            sys.exit('Config file name not in vendor matrix.  '
                     'Aborting.')
        logging.info('Loading Innovid config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            sys.exit('{} not found.  Aborting.'.format(self.config_file))
        self.username = self.config['username']
        self.password = self.config['password']
        self.advertiser = self.config['advertiser']
        self.campaign_filter = self.config['campaign_filter']
        self.config_list = [self.config, self.username, self.password,
                            self.advertiser]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                sys.exit('{} not in Innovid config file.  '
                         'Aborting.'.format(item))

    def set_headers(self):
        user_pass = base64.b64encode(bytes('{}:{}'.format(
            self.username, self.password), 'utf-8')).decode('utf-8')
        self.headers = {'Authorization': 'Basic {}'.format(user_pass)}
        self.make_request(self.auth_url, method='GET', headers=self.headers)

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
    def format_dates(sd, ed):
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
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
        full_date_list = self.list_dates(sd, ed)
        client_id, advertiser_id = self.get_advertiser_id()
        for date_list in full_date_list:
            sd, ed = self.format_dates(date_list[0], date_list[1])
            self.request_reports(sd, ed, client_id, advertiser_id)
        for idx, resp_token in enumerate(self.response_tokens):
            logging.info('Attempting to get report {} of {}'
                         ''.format(idx + 1, len(self.response_tokens)))
            df = self.check_and_get_report(resp_token)
            self.df = self.df.append(df, ignore_index=True)
            self.df = self.filter_df_on_campaign(self.df)
        return self.df

    def filter_df_on_campaign(self, df):
        if self.campaign_filter:
            df = df[df['Campaign Name'].str.contains(self.campaign_filter)]
        return df

    def get_advertiser_id(self):
        self.set_headers()
        url = self.base_url + '/advertisers'
        r = self.make_request(url, method='GET', headers=self.headers)
        ad_id_dict = r.json()['data']['clients'][0]
        client_id = ad_id_dict['id']
        id_dict = [x for x in ad_id_dict['advertisers']
                   if x['name'] == self.advertiser]
        advertiser_id = id_dict[0]['id']
        return client_id, advertiser_id

    def request_reports(self, sd, ed, client_id, advertiser_id):
        logging.info('Requesting data from {} to {}'.format(sd, ed))
        url = (self.base_url + '/clients/{}/advertisers/{}/reports/delivery/'
               'dateFrom/{}/dateTo/{}'.format(client_id, advertiser_id, sd, ed))
        r = self.make_request(url, method='GET', headers=self.headers)
        if 'data' in r.json() and 'reportStatusToken' in r.json()['data']:
            resp_token = r.json()['data']['reportStatusToken']
            self.response_tokens.append(resp_token)
        else:
            logging.warning('Could not get status token: {}.'.format(r.json()))

    def check_and_get_report(self, resp_token):
        df = pd.DataFrame()
        for x in range(500):
            logging.info('Checking for report attempt: {}'.format(x + 1))
            url = self.base_url + '/reports/{}/status'.format(resp_token)
            r = self.make_request(url, method='GET', headers=self.headers)
            report_url = r.json()['data']['reportUrl']
            if report_url:
                r = self.make_request(report_url, method='GET')
                df = pd.read_csv(io.BytesIO(r.content), compression='zip')
                logging.info('Data retrieved.')
                break
            time.sleep(30)
        if df.empty:
            logging.warning('Could not get report - returning blank df')
        return df

    @staticmethod
    def list_dates(sd, ed):
        dates = []
        while sd <= ed:
            cur_end = sd + dt.timedelta(days=4)
            if cur_end > dt.datetime.today():
                cur_end = pd.Timestamp((dt.datetime.today() +
                                        dt.timedelta(days=1)).date())
            dates.append((sd, cur_end))
            sd = sd + dt.timedelta(days=5)
        return dates
