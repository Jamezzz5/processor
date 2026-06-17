import io
import os
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
import processor.reporting.vmcolumns as vmc

config_path = utl.config_path

agg_base_url = 'https://hq1.appsflyer.com/api/agg-data/export/app/'
raw_base_url = 'https://hq1.appsflyer.com/api/raw-data/export/app/'

raw_report_types = ['installs_report', 'in_app_events_report']

report_types = ['geo_by_date_report', 'partners_by_date_report',
                'installs_report', 'in_app_events_report', 'geo_report',
                'daily_report', 'partners_report']


class AfApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.api_token = None
        self.app_id = None
        self.source = []
        self.config_list = None
        self.default_config_file_name = 'afconfig.json'
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Skipping AppsFlyer.')
            return
        logging.info('Loading AF config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        if self.load_config():
            self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Skipping AppsFlyer.'.format(
                self.config_file))
            return False
        self.api_token = self.config.get('api_token', '')
        self.app_id = self.config.get('app_id', '')
        if 'source' in self.config:
            self.source = self.remove_space(self.config.get('source', []))
            self.source = self.source.split(',') if self.source else []
        self.config_list = [('api_token', self.api_token),
                            ('app_id', self.app_id)]
        return True

    @staticmethod
    def remove_space(val):
        return str(val).replace(', ', ',')

    def check_config(self):
        for name, item in self.config_list:
            if not item:
                logging.warning('{} not in AF config file.  AppsFlyer '
                                'pull may not return data.'.format(name))

    def get_headers(self):
        return {'Authorization': 'Bearer {}'.format(self.api_token),
                'accept': 'text/csv'}

    @staticmethod
    def get_base_url(field):
        if field in raw_report_types:
            return raw_base_url
        return agg_base_url

    @staticmethod
    def parse_fields(items):
        sources = []
        category = None
        field = None
        if items:
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
        base_url = self.get_base_url(field)
        report_url = '/{}/v5?'.format(field)
        sded_url = 'from={}&to={}'.format(sd, ed)
        tz_url = '&timezone=America/Los_Angeles'
        full_url = base_url + self.app_id + report_url + sded_url + tz_url
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
        filter_col = 'Media Source (pid)'
        if not self.api_token or not self.app_id:
            logging.warning('AppsFlyer not configured (missing api_token '
                            'or app_id).  Returning empty df.')
            return pd.DataFrame()
        sd, ed = self.get_data_default_check(sd, ed)
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date '
                            'was set to end date.')
            sd = ed
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        field, sources, category = self.parse_fields(fields)
        self.df = pd.DataFrame()
        dfs = []
        for rt in [True, False]:
            dfs.append(self.get_raw_data(sd, ed, field, sources, category, rt))
        self.df = pd.concat(dfs, ignore_index=True, sort=True)
        if self.source and not self.df.empty:
            if filter_col in self.df.columns:
                self.df = self.df[
                    self.df[filter_col].isin(self.source)]
        return self.df

    def get_raw_data(self, sd, ed, field, sources, category, retarget=False,
                     attempts=0):
        logging.info('Getting {} report from {} to {} with source: {}, '
                     'category: {}, retarget: {}'
                     ''.format(field, sd, ed, sources, category, retarget))
        full_url = self.create_url(sd, ed, field, sources, category, retarget)
        try:
            self.r = requests.get(full_url, headers=self.get_headers(),
                                  timeout=1200)
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as err:
            logging.warning('Request failed (timeout or connection error): '
                            '{}'.format(err))
            self.r = None
        if self.r is not None and self.r.status_code == 200:
            return self.data_to_df(self.r)
        attempts, fatal = self.request_error(attempts)
        if fatal:
            return pd.DataFrame()
        return self.get_raw_data(sd, ed, field, sources, category, retarget,
                                 attempts=attempts)

    def request_error(self, attempts=0):
        limit_error = 'Limit reached for'
        wrong_error = 'Something went wrong.'
        attempts += 1
        if attempts > 10:
            msg = (self.r.text if self.r is not None
                   else 'No response from server.')
            logging.warning('Max attempts exceeded, skipping AppsFlyer '
                            'pull: {}'.format(msg))
            return attempts, True
        if self.r is None:
            logging.warning('No response from server (timeout or '
                            'connection error).  Pausing for 5 seconds.')
            time.sleep(5)
            return attempts, False
        if self.r.status_code == 401:
            logging.error('Unauthorized (401).  The AF config api_token '
                          'must be a valid V2 token.  Skipping AppsFlyer '
                          'pull.  Response: {}'.format(self.r.text))
            return attempts, True
        if self.r.status_code == 404:
            logging.error('Not found (404).  Check the AF config app_id.  '
                          'Skipping AppsFlyer pull.  Response: {}'.format(
                              self.r.text))
            return attempts, True
        if (self.r.status_code == 429 or
                (self.r.status_code == 403 and
                 self.r.text[:17] == limit_error)):
            logging.warning(
                'Limit reached ({}).  Pausing for 5 seconds.  '
                'Response: {}'.format(self.r.status_code, self.r.text))
        elif (self.r.status_code == 504 or self.r.status_code == 502 or
                self.r.text[:21] == wrong_error):
            logging.warning(
                'Gateway timeout ({}).  Pausing for 5 seconds.  '
                'Response: {}'.format(self.r.status_code, self.r.text))
        else:
            logging.warning(
                'Unknown error ({}).  Pausing for 5 seconds.  '
                'Response: {}'.format(self.r.status_code, self.r.text))
        time.sleep(5)
        return attempts, False

    @staticmethod
    def data_to_df(r):
        df = pd.read_csv(io.StringIO(r.text))
        return df

    def test_connection(self, acc_col, camp_col, acc_pre):
        results = []
        sd = dt.datetime.today() - dt.timedelta(days=2)
        ed = dt.datetime.today()
        df = self.get_data(sd=sd, ed=ed)
        if df.empty:
            row = [acc_col, 'Failure, double check app_id and api_token', False]
            results.append(row)
        else:
            row = [acc_col, 'Success', True]
            results.append(row)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
