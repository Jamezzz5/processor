import json
import logging
import time
import sys
import ast
import datetime as dt
import pandas as pd
from facebookads.api import FacebookAdsApi
from facebookads import objects
from facebookads.adobjects.adsinsights import AdsInsights
from facebookads.exceptions import FacebookRequestError

def_fields = ['campaign_name', 'adset_name', 'ad_name', 'impressions',
              'inline_link_clicks', 'spend', 'video_10_sec_watched_actions',
              'video_p25_watched_actions', 'video_p50_watched_actions',
              'video_p75_watched_actions', 'video_p100_watched_actions']

nested_col = ['video_10_sec_watched_actions', 'video_p100_watched_actions',
              'video_p50_watched_actions', 'video_p25_watched_actions',
              'video_p75_watched_actions']

col_name_dic = {'date_start': 'Reporting Starts',
                'date_stop': 'Reporting Ends',
                'campaign_name': 'Campaign', 'adset_name': 'Ad Set',
                'ad_name': 'Ad Name', 'impressions': 'Impressions',
                'inline_link_clicks': 'Link Clicks',
                'spend': 'Amount Spent (USD)',
                'video_10_sec_watched_actions': '3-Second Video Views',
                'video_p25_watched_actions': 'Video Watches at 25%',
                'video_p50_watched_actions': 'Video Watches at 50%',
                'video_p75_watched_actions': 'Video Watches at 75%',
                'video_p100_watched_actions': 'Video Watches at 100%'}

config_path = 'Config/'


class FbApi(object):
    def __init__(self):
        self.df = pd.DataFrame()
        self.configfile = None
        self.config = None
        self.account = None
        self.app_id = None
        self.app_secret = None
        self.access_token = None
        self.act_id = None
        self.config_list = []

    def input_config(self, config):
        logging.info('Loading Facebook config file: ' + str(config))
        self.configfile = config_path + config
        self.load_config()
        self.check_config()
        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
        self.account = objects.AdAccount(self.config['act_id'])

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(self.configfile + ' not found.  Aborting.')
            sys.exit(0)
        self.app_id = self.config['app_id']
        self.app_secret = self.config['app_secret']
        self.access_token = self.config['access_token']
        self.act_id = self.config['act_id']
        self.config_list = [self.app_id, self.app_secret, self.access_token,
                            self.act_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warn(item + 'not in FB config file.  Aborting.')
                sys.exit(0)

    def get_data(self, sd=(dt.datetime.today() - dt.timedelta(days=1)),
                 ed=(dt.datetime.today() - dt.timedelta(days=1)),
                 fields=def_fields):
        sd = sd.date()
        ed = ed.date()
        if sd > ed:
            logging.warn('Start date greater than end date.  Start data was' +
                         'set to end date.')
            sd = ed
        full_date_list = self.list_dates(sd, ed)
        date_lists = map(None, *(iter(full_date_list),) * 7)
        for date_list in date_lists:
            date_list = filter(None, date_list)
            sd = date_list[0]
            ed = date_list[-1]
            logging.info('Getting FB data for ' + str(sd) + ' to ' + str(ed))
            try:
                insights = list(self.account.get_insights(
                    fields=fields,
                    params={'level': AdsInsights.Level.ad,
                            'time_range': {'since': str(sd),
                                           'until': str(ed), },
                            'time_increment': 1, }))
            except FacebookRequestError as e:
                if e._api_error_code == 190:
                    logging.error('Facebook Access Token invalid.  Aborting.')
                    sys.exit(0)
                elif e._api_error_code == 17:
                    logging.warn('Facebook rate limit reached.  Pausing for ' +
                                 '300 seconds.')
                    time.sleep(300)
                    date_lists.append(date_list)
                    continue
                elif e._api_error_code == 1:
                    logging.warn('Too much data queried.  Reducing time scale')
                    fh = date_list[:len(date_list)/2]
                    bh = date_list[len(date_list)/2:]
                    date_lists.append(fh)
                    date_lists.append(bh)
                    continue
                else:
                    logging.error('Aborting as the Facebook API call resulted '
                                  'in the following error: ' + str(e))
                    sys.exit(0)
            if not insights:
                continue
            self.df = self.df.append(insights, ignore_index=True)
        for col in nested_col:
            try:
                self.df[col] = self.df[col].apply(lambda x: self.clean_data(x))
            except KeyError:
                continue
        self.df = self.rename_cols()
        return self.df

    @staticmethod
    def clean_data(x):
        if str(x) == str('nan'):
            return 0
        x = str(x).strip('[]')
        return ast.literal_eval(x)['value']

    def rename_cols(self):
        self.df = self.df.rename(columns=col_name_dic)
        return self.df

    @staticmethod
    def list_dates(sd, ed):
        dates = []
        while sd <= ed:
            dates.append(sd)
            sd = sd + dt.timedelta(days=1)
        return dates
