import os
import sys
import json
import logging
import requests
import datetime as dt
import pandas as pd
import urllib.parse as parse
import reporting.utils as utl

config_path = utl.config_path

token_url = 'https://api.revcontent.io/oauth/token'
boosts_url = 'https://api.revcontent.io/stats/api/v1.0/boosts/content'


class RcApi(object):
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
        self.headers = None
        self.adids = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading RC config file: {}'.format(config))
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
        self.config_list = [self.config, self.client_id, self.client_secret]
        self.base_params = self.get_url_params()

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not found in config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    @staticmethod
    def date_check(sd, ed):
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date'
                            'was set to end date.')
            sd = ed - dt.timedelta(days=1)
        return sd, ed

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        return sd, ed

    def get_token(self):
        r = requests.post(token_url, data=self.base_params)
        token = r.json()
        return token

    def set_header(self):
        token = self.get_token()
        at = token['access_token']
        self.headers = {"Authorization": "Bearer {}".format(at),
                        "Content-Type": "application/json",
                        "Cache-Control": "no-cache"}

    def get_url_params(self):
        params = {'grant_type': 'client_credentials',
                  'client_id': self.client_id,
                  'client_secret': self.client_secret}
        return params

    def get_data(self, sd=None, ed=None, fields=None):
        self.df = pd.DataFrame()
        sd, ed = self.get_data_default_check(sd, ed)
        sd, ed = self.date_check(sd, ed)
        self.get_raw_data(sd, ed)
        return self.df

    def get_raw_data(self, sd, ed):
        self.set_header()
        self.df = pd.DataFrame()
        date_list = self.list_dates(sd, ed)
        for date in date_list:
            date = dt.datetime.strftime(date, '%Y-%m-%d')
            logging.info('Getting RC data for {}'.format(date))
            params = {'date_from': date, 'date_to': date}
            r = requests.get(boosts_url, params=params, headers=self.headers)
            tdf = pd.DataFrame(r.json()['data'])
            tdf = tdf[tdf['impressions'] != '0']
            tdf['Date'] = date

            self.df = self.df.append(tdf, ignore_index=True)

    def split_utms(self):
        tdf = pd.DataFrame(list(self.df['utm_codes'].apply(
             lambda x: parse.parse_qs(parse.urlsplit(x).path))))


    def request_error(self):
        logging.warning('Unknown error: {}'.format(self.r.text))
        sys.exit(0)

    @staticmethod
    def list_dates(sd, ed):
        dates = []
        while sd <= ed:
            dates.append(sd)
            sd = sd + dt.timedelta(days=1)
        return dates
