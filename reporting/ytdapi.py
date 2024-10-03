import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import processor.reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path


class YtdApi(object):
    base_url = 'https://www.googleapis.com/youtube/v3/'
    search_url = 'search?part=snippet'
    video_url = 'videos?part=snippet,statistics'
    channel_url = 'channels?part=snippet,statistics'
    def_fields = ['id', 'snippet/title', 'snippet/publishedAt',
                  'statistics/viewCount']
    video_fields = ['snippet/channelId', 'snippet/channelTitle',
                    'statistics/likeCount', 'statistics/favoriteCount',
                    'statistics/commentCount']
    channel_fields = ['snippet/customUrl', 'snippet/country',
                      'statistics/viewCount', 'statistics/subscriberCount',
                      'statistics/videoCount']

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.query = None
        self.config_list = None
        self.client = None
        self.query_type = None
        self.df = pd.DataFrame()

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  ' +
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading YT config file: {}'.format(config))
        self.config_file = config_path + config
        self.load_config()
        self.check_config()
        self.config_file = config_path + config

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
        self.query = self.config['query']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in YT config file.'
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
        token = self.get_token(token, extra)
        self.client = OAuth2Session(self.client_id, token=token)

    def get_token(self, token, extra):
        try:
            token = self.client.refresh_token(token_url=self.refresh_url,
                                              **extra)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            token = self.get_token(token, extra)
        return token

    def make_request(self, url, method='GET', attempt=1, **kwargs):
        self.get_client()
        r = None
        try:
            r = self.client.request(method=method, url=url, **kwargs)
        except requests.exceptions.ConnectionError as e:
            if attempt > 3:
                logging.warning('Unable to resolve Connection Error. {}'
                                .format(e))
                return None
            else:
                logging.warning('Connection Error. Retrying {}'
                                .format(e))
                attempt += 1
                time.sleep(30)
                r = self.make_request(url, method, attempt=attempt, **kwargs)
        return r

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today()
        if ed == sd:
            logging.warning('Start date and end date can not be '
                            'equal. Setting end date to following '
                            'day.')
            ed = ed + dt.timedelta(days=1)
        return sd, ed

    def parse_fields(self, fields):
        self.query_type = 'video'
        report_fields = self.def_fields + self.video_fields
        if fields:
            for field in fields:
                if field == 'channel':
                    self.query_type = 'channel'
                    report_fields = self.def_fields + self.channel_fields
        return report_fields

    def create_search_url(self, query, sd, ed):
        max_url = '&maxResults=50'
        type_url = '&type={}'.format(self.query_type)
        query_url = '&q={}'.format(query)
        full_url = "{0}{1}{2}{3}{4}".format(self.base_url, self.search_url,
                                            max_url, type_url, query_url)
        if self.query_type != 'channel':
            if sd:
                sd_url = '&publishedAfter={}'.format(
                    sd.isoformat() + 'Z')
                full_url += sd_url
            if ed:
                ed_url = '&publishedBefore={}'.format(
                    ed.isoformat() + 'Z')
                full_url += ed_url
        return full_url

    def create_yt_url(self, ids, fields=None):
        fields_url = ''
        if fields:
            fields_url = '&fields=items({})'.format(','.join(fields))
        id_url = '&id={}'.format(','.join(ids))
        full_url = "{0}{1}{2}{3}".format(self.base_url, self.video_url,
                                         fields_url, id_url)
        if self.query_type == 'channel':
            full_url = "{0}{1}{2}{3}".format(self.base_url, self.channel_url,
                                             fields_url, id_url)
        return full_url

    def data_to_df(self, response):
        df = pd.DataFrame()
        date_col = 'snippet.publishedAt'
        if not response:
            return df
        json_response = response.json()
        if 'error' in json_response:
            logging.warning('Request error. {}'.format(json_response))
        else:
            df = pd.json_normalize(json_response['items'])
            if date_col in df.columns:
                df = utl.data_to_type(df, date_col=[date_col])
                df[date_col] = df[date_col].dt.date
                if self.query_type == 'channel':
                    df['Date'] = dt.date.today()
                else:
                    df = df.rename(columns={date_col: 'Date'})
        return df

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        fields = self.parse_fields(fields)
        ids = self.get_search_ids(self.query, sd, ed)
        if not ids:
            logging.warning('No results found for query {}. Returning empty '
                            'DataFrame.'.format(self.query))
            self.df = pd.DataFrame()
            return self.df
        if self.query_type == 'channel':
            url = self.create_yt_url([ids[0]], fields=fields)
        else:
            url = self.create_yt_url(ids, fields=fields)
        r = self.make_request(url)
        self.df = self.data_to_df(r)
        return self.df

    def get_search_ids(self, query, sd, ed):
        id_col = 'id.{}Id'.format(self.query_type)
        full_url = self.create_search_url(query, sd, ed)
        r = self.make_request(full_url)
        df = self.data_to_df(r)
        ids = []
        if id_col in df.columns:
            ids = df[id_col].to_list()
        return ids
