import os
import sys
import jwt
import time
import json
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl

config_path = utl.config_path


class YvApi(object):
    b2b_url = 'https://id.b2b.yahooinc.com'
    token_url = "{}/identity/oauth2/access_token".format(b2b_url)
    aud = "{}/identity/oauth2/access_token?realm=dsp".format(b2b_url)
    schedule_url = 'http://api-sched-v3.admanagerplus.yahoo.com'
    report_url = '{}/yamplus_api/extreport/'.format(schedule_url)
    start_time_str = "T00:00:00-08:00"
    end_time_str = "T23:59:59-08:00"

    def __init__(self):
        self.config = None
        self.config_file = None
        self.username = None
        self.password = None
        self.client_id = None
        self.client_secret = None
        self.advertiser = None
        self.campaign_filter = None
        self.config_list = None
        self.header = None
        self.act_id = None
        self.access_token = None
        self.response_tokens = []
        self.aid_dict = {}
        self.cid_dict = {}
        self.df = pd.DataFrame()
        self.r = None
        self.jwt_token = None
        self.report_id = None
        self.url = None

    def input_config(self, config):
        if str(config) == 'nan':
            sys.exit('Config file name not in vendor matrix.  '
                     'Aborting.')
        logging.info(
            'Loading Yahoo DSP - Verizon config file: {}'.format(config))
        self.config_file = os.path.join(utl.config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            sys.exit('{} not found.  Aborting.'.format(self.config_file))
        self.client_id = self.config["client_id"]
        self.client_secret = self.config["client_secret"]
        self.advertiser = int(self.config["advertiser"])
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.advertiser]
        if 'campaign' in self.config:
            self.campaign_filter = self.config['campaign']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                sys.exit('{} not in Yahoo DSP - Verizon config file.  '
                         'Aborting.'.format(item))

    def set_header(self):
        token = self.get_token()
        self.access_token = token['access_token']
        self.header = {"X-Auth-Method": "OAuth2",
                       "X-Auth-Token": self.access_token}
        logging.info('Header set with access token')

    def get_token(self):
        logging.info('Retrieving access token')
        token_header = {"Content-Type": "application/x-www-form-urlencoded",
                        "Accept": "application/json"}
        assertion = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
        jwt_header = {
            "alg": "HS256",
            "typ": "jwt"
        }
        jwt_payload = {
            "aud": self.aud,
            "iss": self.client_id,
            "sub": self.client_id,
            "exp": time.time() + 600,
            "iat": time.time(),
            "jti": "f8799df2-254e-11ec-9621-0242ac130002"
        }
        jwt_token = jwt.encode(payload=jwt_payload, key=self.client_secret,
                               headers=jwt_header)
        self.jwt_token = jwt_token
        params = {
            "grant_type": "client_credentials",
            "client_assertion_type": assertion,
            "client_assertion": jwt_token,
            "scope": "dsp-api-access",
            "realm": "dsp"}
        r = requests.post(self.token_url, data=params, headers=token_header)
        try:
            token = r.json()
        except json.decoder.JSONDecodeError as e:
            sys.exit('Response not json, exiting. \nError: {}\n Response'
                     .format(e, r.text))
        logging.info('Access token retrieved')
        self.access_token = token
        return token

    @staticmethod
    def format_dates(sd, ed):
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.'
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

    def check_file(self, download_url, attempt=1):
        r = requests.get(download_url, headers=self.header)
        if 'status' in r.json() and r.json()['status'] == 'Success':
            return True, r
        else:
            logging.info('Report unavailable.  Attempt {}.  '
                         'Response: {}'.format(attempt, r.json()))
            time.sleep(30)
            return False, r

    def get_report(self, download_url):
        report_status = False
        r = None
        for x in range(1, 101):
            report_status, r = self.check_file(download_url, attempt=x)
            if report_status:
                break
        if not report_status:
            logging.warning('Report could not download returning blank df.')
            return None
        logging.info('Report available.  Downloading.')
        if 'url' in r.json():
            df = pd.read_csv(r.json()['url'])
        else:
            logging.warning('Url not in response likely no rows, '
                            'returning blank df.')
            df = pd.DataFrame()
        return df

    def request_report(self, sd, ed):
        sd_time = '{}{}'.format(sd, self.start_time_str)
        ed_time = '{}{}'.format(ed, self.end_time_str)
        payload = {
            "reportOption": {
                "timezone": "America/Los_Angeles",
                "currency": 4,
                "dimensionTypeIds": [4, 5, 8, 34, 39],
                "metricTypeIds": [1, 2, 23, 25, 26, 27, 28,
                                  29, 44, 109, 110, 138],
                "accountIds": [self.advertiser]
            },
            "intervalTypeId": 2,
            "dateTypeId": 11,
            "startDate": sd_time,
            "endDate": ed_time
        }
        self.header['Content-Type'] = 'application/json'
        r = requests.post(self.report_url, headers=self.header, json=payload)
        report_id = r.json()['customerReportId']
        download_url = "{}{}".format(self.report_url, report_id)
        return download_url

    def filter_df_on_campaign(self, df):
        logging.info('Filtering dataframe on {}'.format(self.campaign_filter))
        df = df[df['Campaign Name'].fillna('0').str.contains(
            self.campaign_filter)].reset_index(drop=True)
        return df

    def get_data(self, sd=None, ed=None, fields=None):
        self.set_header()
        sd, ed = self.get_data_default_check(sd, ed)
        sd, ed = self.format_dates(sd, ed)
        logging.info('Getting data from {} to {}'.format(sd, ed))
        download_url = self.request_report(sd, ed)
        df = self.get_report(download_url)
        logging.info('Data downloaded.')
        if self.campaign_filter:
            df = self.filter_df_on_campaign(df)
        logging.info('Returning df.')
        return df
