import os
import sys
import json
import logging
import pandas as pd
import datetime as dt
import processor.reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path


class GaApi(object):
    base_url = 'https://www.googleapis.com/analytics/v3/data/ga'
    def_metrics = ['goal1Completions', 'goal2Completions', 'users',
                   'newUsers', 'bounces', 'pageviews', 'totalEvents',
                   'uniqueEvents', 'timeOnPage']
    def_dims = ['date', 'campaign', 'source', 'medium', 'keyword', 'country']
    dcm_dims = ['dcmClickSitePlacement']

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
                else:
                    field = field.split('::')
                    parsed_fields.append(
                        ','.join('ga:{}=={}'.format(field[0], x)
                                 for x in field[1].split(',')))
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

    def create_url(self, sd, ed, fields, start_index, metric):
        ids_url = '?ids=ga:{}'.format(self.ga_id)
        sd_url = '&start-date={}'.format(sd)
        ed_url = '&end-date={}'.format(ed)
        dim_url = '&dimensions={}'.format(','.join('ga:' + x
                                                   for x in self.def_dims))
        metrics_url = '&metrics={}'.format(','.join('ga:' + x
                                                    for x in metric))
        start_index_url = '&start-index={}'.format(start_index)
        max_results_url = '&max-results=10000'
        full_url = (self.base_url + ids_url + sd_url + ed_url + dim_url +
                    metrics_url + start_index_url + max_results_url)
        if fields:
            filter_url = '&filters={}'.format(';'.join(fields))
            full_url += filter_url
        return full_url

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        logging.info('Getting df from {} to {}'.format(sd, ed))
        self.get_client()
        start_index = 1
        metrics = [self.def_metrics[x:x + 10]
                   for x in range(0, len(self.def_metrics), 10)]
        self.get_df_for_all_metrics(sd, ed, fields, start_index, metrics)
        start_indices = self.get_start_indices()
        for start_index in start_indices:
            self.get_df_for_all_metrics(sd, ed, fields, start_index, metrics)
        return self.df

    def get_start_indices(self):
        if 'totalResults' not in self.r.json():
            logging.warning('Results not in response: \n{}'
                            ''.format(self.r.json()))
            return []
        total_results = self.r.json()['totalResults']
        total_pages = range(total_results // 10000 +
                            ((total_results % 10000) > 0))[1:]
        start_indices = [(10000 * x) + 1 for x in total_pages]
        return start_indices

    def get_df_for_all_metrics(self, sd, ed, fields, start_index, metrics):
        for metric in metrics:
            self.get_raw_data(sd, ed, fields, start_index, metric)

    def get_raw_data(self, sd, ed, fields, start_index, metric):
        full_url = self.create_url(sd, ed, fields, start_index, metric)
        self.r = self.client.get(full_url)
        tdf = self.data_to_df(self.r)
        self.df = pd.concat([self.df, tdf])

    def data_to_df(self, r):
        if 'columnHeaders' not in self.r.json():
            logging.warning('Column headers not in response: \n{}'
                            ''.format(self.r.json()))
            return pd.DataFrame()
        cols = [x['name'][3:] for x in r.json()['columnHeaders']]
        if 'rows' not in r.json():
            logging.warning('Rows not in response: \n{}'
                            ''.format(self.r.json()))
            return pd.DataFrame()
        raw_data = r.json()['rows']
        df = pd.DataFrame(raw_data, columns=cols)
        return df
