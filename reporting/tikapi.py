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


class TikApi(object):
    version = 'v1.3'
    base_url = 'https://business-api.tiktok.com/open_api/'
    ad_url = '/ad/get/'
    ad_report_url = '/report/integrated/get/'
    new_date = 'stat_time_day'
    old_date = 'stat_datetime'
    dimensions = ['stat_time_day', 'ad_id']
    metrics = {'clicks': 'click_cnt',
               'cost_per_conversion': 'conversion_cost',
               'conversion_rate': 'conversion_rate',
               'conversion': 'convert_cnt',
               'ctr': 'ctr',
               'impressions': 'show_cnt',
               'spend': 'stat_cost',
               'video_watched_2s': 'play_duration_2s',
               'video_watched_6s': 'play_duration_6s',
               'video_views_p100': 'play_over',
               'video_views_p75': 'play_third_quartile',
               'video_views_p50': 'play_midpoint',
               'video_views_p25': 'play_first_quartile',
               'video_play_actions': 'total_play',
               'comments': 'ad_comment',
               'likes': 'ad_like',
               'shares': 'ad_share',
               'frequency': 'frequency',
               'real_time_app_install': 'real_time_app_install',
               'app_install': 'app_install',
               'registration': 'registration',
               'purchase': 'purchase',
               'checkout': 'checkout',
               'view_content': 'view_content',
               'offline_shopping_events': 'offline_shopping_events'}

    def __init__(self):
        self.config = None
        self.config_file = None
        self.access_token = None
        self.advertiser_id = None
        self.campaign_id = None
        self.ad_id_list = []
        self.config_list = None
        self.headers = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Tik config file: {}'.format(config))
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
        self.access_token = self.config['access_token']
        self.config_list = [self.config, self.access_token]
        if 'advertiser_id' in self.config:
            self.advertiser_id = self.config['advertiser_id']
        if 'campaign_id' in self.config:
            self.campaign_id = self.config['campaign_id']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Tik config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        self.headers = {'Access-Token': self.access_token,
                        'Content-Type': 'application/json'}

    def make_request(self, url, method, headers=None, json_body=None, data=None,
                     params=None, attempt=1):
        if not json_body:
            json_body = {}
        if not headers:
            headers = {}
        if not data:
            data = {}
        if not params:
            params = {}
        if method == 'POST':
            request_method = requests.post
        else:
            request_method = requests.get
        try:
            r = request_method(url, headers=headers, json=json_body, data=data,
                               params=params)
        except requests.exceptions.ConnectionError as e:
            attempt += 1
            if attempt > 100:
                logging.warning('Could not connection with error: {}'.format(e))
                r = None
            else:
                logging.warning('Connection error, pausing for 60s '
                                'and retrying: {}'.format(e))
                time.sleep(60)
                r = self.make_request(url, method, headers, json_body, attempt)
        return r

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    def get_data_default_check(self, sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() + dt.timedelta(days=1)
        if dt.datetime.today().date() == ed.date():
            ed += dt.timedelta(days=1)
        sd, ed = self.date_check(sd, ed)
        return sd, ed

    def get_ad_ids(self):
        logging.info('Getting ad ids.')
        self.set_headers()
        url = self.base_url + self.version + self.ad_url
        params = {'advertiser_id': self.advertiser_id,
                  'page': 1,
                  'page_size': 1000,
                  'filtering': json.dumps(
                      {'primary_status': 'STATUS_ALL',
                       'status': 'AD_STATUS_ALL'})}
        ad_ids, r = self.request_ad_id(url, params, [])
        if r and ad_ids:
            total_pages = r.json()['data']['page_info']['total_page']
            for x in range(total_pages - 1):
                page_num = x + 2
                params['page'] = page_num
                if page_num % 10 == 0:
                    logging.info('Pulling ad_ids page #{} of {}'.format(
                        page_num, total_pages))
                ad_ids, r = self.request_ad_id(url, params, ad_ids)
        ad_ids = [ad_ids[x:x + 100] for x in range(0, len(ad_ids), 100)]
        logging.info('Returning all ad ids.')
        return ad_ids

    def request_ad_id(self, url, params, ad_ids):
        r = self.make_request(url, method='GET', headers=self.headers,
                              params=params)
        response_data = r.json()['data']
        if 'list' not in response_data:
            logging.warning(
                'No list in response please make sure accounts '
                'have been given access:\n {}'.format(r.json()))
            return ad_ids, r
        ad_list = response_data['list']
        self.ad_id_list.extend(ad_list)
        ad_ids.extend([x['ad_id'] for x in ad_list])
        return ad_ids, r

    def request_and_get_data(self, sd, ed):
        url = self.base_url + self.version + self.ad_report_url
        params = {'advertiser_id': self.advertiser_id,
                  'report_type': 'BASIC',
                  'data_level': 'AUCTION_AD',
                  'start_date': sd,
                  'end_date': ed,
                  'metrics': json.dumps(list(self.metrics.keys())),
                  'dimensions': json.dumps(self.dimensions),
                  'page_size': 1000}
        for x in range(1, 1000):
            logging.info('Getting data from {} to {}.  Page #{}.'
                         ''.format(sd, ed, x))
            params['page'] = x
            r = self.make_request(url=url, method='GET', headers=self.headers,
                                  params=params)
            if ('data' not in r.json() or 'list' not in r.json()['data'] or
                    not r.json()['data']['list']):
                logging.warning('Data not in response as follows:\n'
                                '{}'.format(r.json()))
                return self.df
            df = pd.DataFrame(r.json()['data']['list'])
            for col in ['dimensions', 'metrics']:
                tdf = pd.DataFrame(df[col].to_list())
                df = df.join(tdf)
            self.df = self.df.append(df, ignore_index=True)
            page_rem = r.json()['data']['page_info']['total_page']
            if x >= page_rem:
                break
            logging.info('Data retrieved {} pages remaining'
                         ''.format(page_rem - x))
        self.df = self.df.merge(pd.DataFrame(self.ad_id_list), on='ad_id',
                                how='left')
        cols = self.metrics.copy()
        cols[self.new_date] = self.old_date
        self.df = self.df.rename(columns=cols)
        logging.info('Data successfully pulled.  Returning df.')
        return self.df

    def filter_df_on_campaign(self, df):
        campaign_col = 'campaign_name'
        if self.campaign_id:
            df = utl.filter_df_on_col(df, campaign_col, self.campaign_id)
        return df

    def reset_params(self):
        self.df = pd.DataFrame()
        self.ad_id_list = []

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        self.reset_params()
        self.get_ad_ids()
        self.df = self.request_and_get_data(sd, ed)
        self.df = self.filter_df_on_campaign(self.df)
        return self.df
