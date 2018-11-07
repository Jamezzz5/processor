import os
import sys
import json
import logging
import pandas as pd
import datetime as dt
import reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path
base_url = 'https://www.googleapis.com/analytics/v3/data/ga'
def_metrics = ['sessions', 'goal1Completions', 'goal2Completions', 'users',
               'newUsers', 'bounces', 'pageviews', 'totalEvents',
               'uniqueEvents', 'timeOnPage']
def_dims = ['date', 'campaign', 'source', 'medium', 'keyword',
            'adContent', 'country']


class GaApi(object):
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

    @staticmethod
    def get_data_default_check(sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields is None:
            fields = def_dims
        return sd, ed, fields

    def create_url(self, sd, ed, start_index, metric):
        ids_url = '?ids=ga:{}'.format(self.ga_id)
        sd_url = '&start-date={}'.format(sd)
        ed_url = '&end-date={}'.format(ed)
        dim_url = '&dimensions={}'.format(','.join('ga:' + x
                                                   for x in def_dims))
        metrics_url = '&metrics={}'.format(','.join('ga:' + x
                                                    for x in metric))
        start_index_url = '&start-index={}'.format(start_index)
        max_results_url = '&max-results=10000'
        full_url = (base_url + ids_url + sd_url + ed_url + dim_url +
                    metrics_url + start_index_url + max_results_url)
        return full_url

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date '
                            'was set to end date.')
            sd = ed
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        self.get_client()
        start_index = 1
        metrics = [def_metrics[x:x + 10]
                   for x in range(0, len(def_metrics), 10)]
        for metric in metrics:
            self.get_raw_data(sd, ed, start_index, metric)
        total_results = self.r.json()['totalResults']
        total_pages = range(total_results // 10000 +
                            ((total_results % 10000) > 0))[1:]
        start_indices = [(10000 * x) + 1 for x in total_pages]
        for start_index in start_indices:
            for metric in metrics:
                self.get_raw_data(sd, ed, start_index, metric)
        return self.df

    def get_raw_data(self, sd, ed, start_index, metric):
        full_url = self.create_url(sd, ed, start_index, metric)
        self.r = self.client.get(full_url)
        tdf = self.data_to_df(self.r)
        self.df = self.df.append(tdf)

    @staticmethod
    def data_to_df(r):
        cols = [x['name'][3:] for x in r.json()['columnHeaders']]
        raw_data = r.json()['rows']
        df = pd.DataFrame(raw_data, columns=cols)
        return df
