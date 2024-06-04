import os
import sys
import json
import logging
import requests
import datetime as dt
import pandas as pd
from io import StringIO
import reporting.utils as utl

config_path = utl.config_path


class AjApi(object):
    base_url = 'https://dash.adjust.com/control-center/reports-service/'
    def_fields = ['sessions', 'installs', 'revenue', 'daus', 'waus', 'maus',
                  'events', 'clicks', 'impressions', 'cost', 'network_installs',
                  'organic_installs', 'skad_installs']
    def_groupings = ['day', 'country', 'os_name',
                     'partner_name', 'campaign', 'adgroup', 'creative']

    def __init__(self):
        self.config = None
        self.config_file = None
        self.app_token = None
        self.tracker_token = None
        self.api_token = None
        self.config_list = None
        self.df = pd.DataFrame()
        self.r = None
        self.attribution_type = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading AJ config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

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
        self.config_list = [('app_token', self.app_token),
                            ('api_token', self.api_token)]

    def check_config(self):
        for item in self.config_list:
            if item[1] == '':
                logging.warning('{} not in AJ config file.  Aborting.'.format(
                    item[0]))
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

    def create_url(self, sd, ed, fields):
        full_url = '{}csv_report'.format(self.base_url)
        params = {'app_token__in': self.app_token,
                  'metrics': ','.join(self.def_fields),
                  'date_period': '{}:{}'.format(sd, ed),
                  'dimensions': ','.join(self.def_groupings)}
        if self.tracker_token:
            params['tracker_filter'] = self.tracker_token
        if fields:
            for field in fields:
                if 'attribution' in field:
                    if 'impression' in field:
                        val = 'impression'
                    elif 'all' in field:
                        val = 'all'
                    else:
                        val = 'click'
                    params['attribution_type'] = val
        return full_url, params

    def get_data(self, sd=None, ed=None, fields=None):
        self.df = pd.DataFrame()
        sd, ed = self.get_data_default_check(sd, ed)
        sd, ed = self.date_check(sd, ed)
        self.get_raw_data(sd, ed, fields)
        return self.df

    def get_raw_data(self, sd, ed, fields):
        logging.info('Getting data from {} to {}'.format(sd, ed))
        full_url, params = self.create_url(sd, ed, fields)
        headers = {'Authorization': 'Bearer {}'.format(self.api_token)}
        self.r = requests.get(full_url, headers=headers, params=params)
        if self.r.status_code == 200:
            tdf = self.data_to_df(self.r)
            self.df = pd.concat([self.df, tdf])
            logging.info('Data downloaded, returning df.')
        else:
            self.request_error()

    def request_error(self):
        logging.warning('Unknown error: ' + str(self.r.text))
        sys.exit(0)

    @staticmethod
    def data_to_df(r):
        df = pd.read_csv(StringIO(r.text))
        df.columns = ['date' if 'date' in x else x for x in df.columns]
        return df
