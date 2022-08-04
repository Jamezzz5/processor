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

base_url = 'https://hq.appsflyer.com/export/'

report_types = ['geo_by_date_report', 'partners_by_date_report',
                'installs_report', 'in_app_events_report', 'geo_report',
                'daily_report', 'partners_report']


class AfApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.api_token = None
        self.app_id = None
        self.config_list = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            sys.exit('Config file name not in vendor matrix.  '
                     'Aborting.')
        logging.info('Loading AF config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            sys.exit('{} not found.  Aborting.'.format(self.config_file))
        self.api_token = self.config['api_token']
        self.app_id = self.config['app_id']
        self.config_list = [self.config, self.api_token, self.app_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                sys.exit('{} not in AF config file.  '
                         'Aborting.'.format(item))

    @staticmethod
    def parse_fields(items):
        sources = []
        category = None
        field = None
        for item in items:
            if item in report_types:
                field = item
            elif item == 'standard':
                category = 'standard'
            else:
                sources.append(item)
        if not field:
            field = 'partners_by_date_report'
        return field, sources, category

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        return sd, ed

    def create_url(self, sd, ed, field, sources, category, retarget=False):
        report_url = '/{}/v5?'.format(field)
        token_url = 'api_token={}'.format(self.api_token)
        sded_url = '&from={}&to={}'.format(sd, ed)
        tz_url = '&timezone=America/Los_Angeles'
        full_url = (base_url + self.app_id + report_url + token_url +
                    sded_url + tz_url)
        if sources:
            source_url = '&media_source={}'.format(','.join(sources))
            full_url += source_url
        if category:
            cat_url = '&category={}'.format(category)
            full_url += cat_url
        if retarget:
            full_url += '&reattr=true'
        return full_url

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date '
                            'was set to end date.')
            sd = ed
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        field, sources, category = self.parse_fields(fields)
        self.df = pd.DataFrame()
        for rt in [True, False]:
            tdf = self.get_raw_data(sd, ed, field, sources, category, rt)
            self.df = self.df.append(tdf, ignore_index=True, sort=True)
            time.sleep(60)
        return self.df

    def get_raw_data(self, sd, ed, field, sources, category, retarget=False,
                     attempts=0):
        logging.info('Getting {} report from {} to {} with source: {}, '
                     'category: {}, retarget: {}'
                     ''.format(field, sd, ed, sources, category, retarget))
        full_url = self.create_url(sd, ed, field, sources, category, retarget)
        try:
            self.r = requests.get(full_url, timeout=1200)
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as err:
            logging.warning('Request exceeded 1200 seconds, remaking request: '
                            '\n {}'.format(err))
        if self.r and self.r.status_code == 200:
            df = self.data_to_df(self.r)
        else:
            attempts = self.request_error(attempts)
            df = self.get_raw_data(sd, ed, field, sources, category, retarget,
                                   attempts=attempts)
        return df

    def request_error(self, attempts=0):
        limit_error = 'Limit reached for'
        wrong_error = 'Something went wrong.'
        attempts += 1
        if attempts > 10:
            sys.exit('Max attempts exceeded: {}'.format(self.r.text))
        if not self.r:
            pass
        if self.r.status_code == 403 and self.r.text[:17] == limit_error:
            logging.warning('Limit reached pausing for 60 seconds.  '
                            'Response: {}'.format(self.r.text))
        elif (self.r.status_code == 504 or self.r.status_code == 502 or
                self.r.text[:21] == wrong_error):
            logging.warning('Gateway timeout.  Pausing for 60 seconds.  '
                            'Response: {}'.format(self.r.text))
        else:
            logging.warning('Unknown error.  Pausing for 60 seconds.  '
                            'Response: {}'.format(self.r.text))
        time.sleep(60)
        return attempts

    @staticmethod
    def data_to_df(r):
        df = pd.read_csv(io.StringIO(r.text))
        return df
