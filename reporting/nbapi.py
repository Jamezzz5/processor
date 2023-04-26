import sys
import json
import time
import logging
import datetime as dt
import pandas as pd
import reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path

auth_url = 'https://api.netbase.com/cb/oauth/authorize?'
metric_url = 'https://api.netbase.com:443/cb/insight-api/2/metricValues?'

def_metrics = ['TotalBuzz', 'TotalBuzzPost', 'TotalReplies', 'TotalReposts',
               'OriginalPosts', 'Impressions', 'PositiveSentiment',
               'NegativeSentiment', 'NeutralSentiment', 'NetSentiment',
               'Passion', 'UniqueAuthor', 'StrongEmotion', 'WeakEmotion',
               'EngagementLikes', 'EngagementComments', 'EngagementShares',
               'EngagementViews', 'EngagementDislikes','EngagementTotal',
               'EngagementRatio']

metric_col_name_dic = {x: 'Social Metrics - ' + x for x in def_metrics}


class NbApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.topic_id = None
        self.config_list = None
        self.client = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  ' +
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading NB config file: ' + str(config))
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
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.access_token = self.config['access_token']
        self.refresh_token = self.config['refresh_token']
        self.refresh_url = self.config['refresh_url']
        self.topic_id = self.config['topic_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url,
                            self.topic_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning(item + 'not in NB config file.  Aborting.')
                sys.exit(0)

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'bearer',
                 'expires_in': 43199}
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
            ed = dt.datetime.today()
        if fields is None:
            fields = None
        return sd, ed, fields

    def create_url(self, sd, ed):
        ids_url = 'topicIds={}'.format(self.topic_id)
        met_url = ''.join('&metricSeries={}'.format(x) for x in def_metrics)
        date_url = '&datetimeISO=true&timeUnits=Day&smoothing=false'
        sded_url = '&publishedDate={}&publishedDate={}'.format(sd, ed)
        full_url = (metric_url + ids_url + met_url + date_url + sded_url)
        return full_url

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date' +
                            'was set to end date.')
            sd = ed
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        self.make_request(sd, ed)
        self.get_raw_data()
        return self.df

    def make_request(self, sd, ed):
        self.get_client()
        full_url = self.create_url(sd, ed)
        self.r = self.client.get(full_url)
        if 'error' in self.r.json():
            self.request_error()
            self.make_request(sd, ed)

    def request_error(self):
        if self.r.json()['error'] == 'Rate limit exceeded':
            logging.warning('Rate limit exceeded. Retrying after 120s pause.')
            time.sleep(120)
            return True
        else:
            logging.warning('Unkown error occured: ' + str(self.r.json()))
            sys.exit(0)

    def get_raw_data(self):
        self.df = pd.DataFrame()
        tdf = self.data_to_df(self.r)
        self.df = pd.concat([self.df, tdf])
        self.df = self.df.rename(columns=metric_col_name_dic)
        self.df['Topic ID'] = 'Netbase Topic: ' + str(self.topic_id)

    @staticmethod
    def data_to_df(r):
        df = pd.DataFrame()
        df['Date'] = r.json()['metrics'][0]['columns']
        for x in r.json()['metrics'][0]['dataset']:
            df[x['seriesName']] = x['set']
        df = utl.data_to_type(df, date_col=['Date'])
        df['Date'] = df['Date'].dt.date
        return df
