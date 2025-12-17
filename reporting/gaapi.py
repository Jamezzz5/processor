import os
import sys
import json
import logging
import time

import pandas as pd
import datetime as dt

import requests.exceptions

import reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path


class GaApi(object):
    base_url = 'https://analyticsdata.googleapis.com/v1beta/properties/'
    def_metrics = ['totalUsers', 'eventCount', 'sessions', 'engagedSessions',
                   'newUsers', 'averageSessionDuration',
                   'userEngagementDuration']
    def_dims = ['date', 'country', 'sessionManualCampaignName',
                'sessionManualMedium', 'sessionManualSource',
                'sessionManualTerm', 'sessionManualAdContent', 'eventName']
    dcm_dims = ['dcmClickSitePlacement']
    default_config_file_name = 'gaapi.json'
    regex_filter_type = "FULL_REGEXP"
    paid_media_filter = "paidmedia"

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.ga_id = None
        self.config_list = None
        self.client = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading GA config file:{}'.format(config))
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
        self.refresh_url = self.config['refresh_url']
        self.ga_id = self.config['ga_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url, self.ga_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in GA config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.client.refresh_token(self.refresh_url, **extra)
        self.client = OAuth2Session(self.client_id, token=token)

    def get_data_default_check(self, sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        fields = self.parse_fields(fields)
        sd, ed = self.date_check(sd, ed)
        return sd, ed, fields

    def parse_fields(self, fields):
        parsed_fields = []
        if fields:
            for field in fields:
                if field == 'DCM':
                    self.def_dims = self.def_dims + self.dcm_dims
                if field == 'paidmedia':
                    parsed_fields.append(self.paid_media_filter)
        return parsed_fields

    @staticmethod
    def date_check(sd, ed):
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date '
                            'was set to end date.')
            sd = ed
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    def create_url(self):
        full_url = '{}{}:runReport'.format(self.base_url, self.ga_id)
        return full_url

    def create_body(self, sd, ed, fields):
        body = {
            "dateRanges": [
                {"startDate": sd, "endDate": ed}
            ],
            "metrics": [{"name": m} for m in self.def_metrics],
            "dimensions": [{"name": d} for d in self.def_dims],
            "limit": "250000"
        }
        if 'paidmedia' in fields:
            body["dimensionFilter"] = {
                "filter": {
                    "fieldName": "sessionManualMedium",
                    "stringFilter": {
                        "matchType": self.regex_filter_type,
                        "value": self.paid_media_filter
                    }
                }
            }
        return body

    def get_data(self, sd=None, ed=None, fields=None, retries=3, delay=2):
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        logging.info('Getting df from {} to {}'.format(sd, ed))
        self.get_client()
        url = self.create_url()
        body = self.create_body(sd, ed, fields)
        df = pd.DataFrame()
        for attempt in range(1, retries +1):
            r = self.client.post(url, json=body)
            try:
                df = self.data_to_df(r)
                break
            except requests.exceptions.JSONDecodeError as e:
                logging.warning('JSON decode failed on attempt: {}'.format(
                    attempt))
                if attempt < retries:
                    time.sleep(delay)
                    continue
                logging.warning('Failed aget {} attempts: {}'.format(
                    retries, r.text))
        return df

    @staticmethod
    def data_to_df(r):
        dimension_header = 'dimensionHeaders'
        if dimension_header not in r.json():
            logging.warning('Column headers not in response: \n{}'
                            ''.format(r.json()))
            return pd.DataFrame()
        cols = [x['name'] for x in r.json()[dimension_header]]
        cols += [header['name'] for header in r.json()['metricHeaders']]
        if 'rows' not in r.json():
            logging.warning('Rows not in response: \n{}'
                            ''.format(r.json()))
            return pd.DataFrame(columns=cols)
        raw_data = r.json()['rows']
        parsed_data = []
        for row in raw_data:
            dimensions = row['dimensionValues']
            metrics = row['metricValues']
            new_data = [d['value'] for d in dimensions]
            new_data += [m['value'] for m in metrics]
            parsed_data.append(new_data)
        df = pd.DataFrame(parsed_data, columns=cols)
        return df
