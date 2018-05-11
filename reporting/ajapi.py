import sys
import json
import logging
import requests
import datetime as dt
import pandas as pd
from io import StringIO
import reporting.utils as utl

config_path = utl.config_path

base_url = 'http://api.adjust.com/kpis/v1/'

def_fields = ['sessions', 'installs', 'revenue', 'daus', 'waus', 'maus',
              'events']
def_groupings = ['days', 'countries', 'campaigns', 'adgroups', 'creatives',
                 'events']


class AjApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.app_token = None
        self.tracker_token = None
        self.api_token = None
        self.config_list = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  ' +
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading AJ config file: ' + str(config))
        self.config_file = config_path + config
        self.load_config()
        self.check_config()
        self.config_file = config_path + config

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(self.config_file + ' not found.  Aborting.')
            sys.exit(0)
        self.app_token = self.config['app_token']
        self.tracker_token = self.config['tracker_token']
        self.api_token = self.config['api_token']
        self.config_list = [self.config, self.app_token, self.tracker_token,
                            self.api_token]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning(item + 'not in AJ config file.  Aborting.')
                sys.exit(0)

    @staticmethod
    def date_check(sd, ed):
        if sd > ed:
            logging.warning('Start date greater than end date.  Start data' +
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

    def create_url(self, sd, ed):
        app_url = '{}?'.format(self.app_token)
        token_url = 'user_token={}'.format(self.api_token)
        kpi_url = '&kpis={}'.format(','.join(def_fields))
        sded_url = '&start_date={}&end_date={}'.format(sd, ed)
        group_url = '&grouping={}'.format(','.join(def_groupings))
        full_url = (base_url + app_url + token_url + kpi_url + sded_url +
                    group_url)
        return full_url

    def get_data(self, sd=None, ed=None, fields=None):
        self.df = pd.DataFrame()
        sd, ed = self.get_data_default_check(sd, ed)
        sd, ed = self.date_check(sd, ed)
        self.get_raw_data(sd, ed)
        return self.df

    def get_raw_data(self, sd, ed):
        full_url = self.create_url(sd, ed)
        headers = {'Accept': 'text/csv'}
        self.r = requests.get(full_url, headers=headers)
        if self.r.status_code == 200:
            tdf = self.data_to_df(self.r)
            self.df = self.df.append(tdf)
        else:
            self.request_error()

    def request_error(self):
        logging.warning('Unknown error: ' + str(self.r.text))
        sys.exit(0)

    @staticmethod
    def data_to_df(r):
        df = pd.read_csv(StringIO(r.text))
        return df
