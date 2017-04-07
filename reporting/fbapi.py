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

def_params = ['campaign_name', 'adset_name', 'ad_name']

def_metrics = ['impressions', 'inline_link_clicks', 'spend',
               'video_10_sec_watched_actions', 'video_p25_watched_actions',
               'video_p50_watched_actions', 'video_p75_watched_actions',
               'video_p100_watched_actions']

def_fields = def_params + def_metrics

nested_col = ['video_10_sec_watched_actions', 'video_p100_watched_actions',
              'video_p50_watched_actions', 'video_p25_watched_actions',
              'video_p75_watched_actions']

col_name_dic = {'date_start': 'Reporting Starts',
                'date_stop': 'Reporting Ends',
                'campaign_name': 'Campaign Name', 'adset_name': 'Ad Set Name',
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
        self.date_lists = None
        self.field_lists = None

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
        self.date_lists = map(None, *(iter(full_date_list),) * 7)
        for date_list in self.date_lists:
            date_list = filter(None, date_list)
            sd = date_list[0]
            ed = date_list[-1]
            self.field_lists = [fields]
            for field_list in self.field_lists:
                logging.info('Getting FB data for ' + str(sd) + ' to ' +
                             str(ed))
                try:
                    insights = list(self.account.get_insights(
                        fields=field_list,
                        params={'level': AdsInsights.Level.ad,
                                'time_range': {'since': str(sd),
                                               'until': str(ed), },
                                'time_increment': 1, }))
                except FacebookRequestError as e:
                    self.request_error(e, date_list, field_list)
                    continue
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

    def request_error(self, e, date_list, field_list):
        if e._api_error_code == 190:
            logging.error('Facebook Access Token invalid.  Aborting.')
            sys.exit(0)
        elif e._api_error_code == 17:
            logging.warn('Facebook rate limit reached.  Pausing for ' +
                         '300 seconds.')
            time.sleep(300)
            self.date_lists.append(date_list)
            return True
        elif e._api_error_code == 1:
            if date_list[0] == date_list[-1]:
                logging.warn('Already daily.  Reducing requested fields.')
                metrics = [x for x in field_list if x not in def_params]
                fh, bh = self.split_list(metrics)
                self.field_lists.append(def_params + fh)
                self.field_lists.append(def_params + bh)
                return True
            logging.warn('Too much data queried.  Reducing time scale')
            fh, bh = self.split_list(date_list)
            self.date_lists.append(fh)
            self.date_lists.append(bh)
            return True
        else:
            logging.error('Aborting as the Facebook API call resulted '
                          'in the following error: ' + str(e))
            sys.exit(0)

    @staticmethod
    def split_list(x):
        first_half = x[:len(x)/2]
        back_half = x[len(x)/2:]
        return first_half, back_half

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
