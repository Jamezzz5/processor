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

fields_actions = ['actions', 'action_values']

def_fields = def_params + def_metrics

nested_col = ['video_10_sec_watched_actions', 'video_p100_watched_actions',
              'video_p50_watched_actions', 'video_p25_watched_actions',
              'video_p75_watched_actions']

nested_dict_col = ['actions']

col_name_dic = {'date_start': 'Reporting Starts',
                'date_stop': 'Reporting Ends',
                'campaign_name': 'Campaign Name', 'adset_name': 'Ad Set Name',
                'ad_name': 'Ad Name', 'impressions': 'Impressions',
                'inline_link_clicks': 'Link Clicks',
                'spend': 'Amount Spent (USD)',
                'video_10_sec_watched_actions': '10-Second Video Views',
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

    @staticmethod
    def parse_fields(fields):
        for field in fields:
            if field == 'Actions':
                fields = def_fields + fields_actions
        return fields

    @staticmethod
    def get_data_default_check(sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields is None:
            fields = def_fields
        return sd, ed, fields

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        fields = self.parse_fields(fields)
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
                    deleted = list(self.account.get_insights(
                        fields=field_list,
                        params={'level': AdsInsights.Level.ad,
                                'time_range': {'since': str(sd),
                                               'until': str(ed), },
                                'time_increment': 1,
                                'filtering': [{'field': 'ad.effective_status',
                                               'operator': 'IN',
                                               'value': ['DELETED',
                                                         'ARCHIVED']}]}))
                except FacebookRequestError as e:
                    self.request_error(e, date_list, field_list)
                    continue
                if insights:
                    self.df = self.df.append(insights, ignore_index=True)
                if deleted:
                    self.df = self.df.append(deleted, ignore_index=True)
        for col in nested_col:
            try:
                self.df[col] = self.df[col].apply(lambda x: self.clean_data(x))
            except KeyError:
                continue
        for col in nested_dict_col:
            if col in self.df.columns:
                self.nested_dicts_to_cols(col)
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
                if fh and bh:
                    self.field_lists.append(def_params + fh)
                    self.field_lists.append(def_params + bh)
                else:
                    self.field_lists.append(field_list)
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

    @staticmethod
    def convert_dictionary(x):
        if str(x) == str('nan'):
            return 0
        x = str(x).strip('[]')
        return ast.literal_eval(x)

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

    def nested_dicts_to_cols(self, nd_col):
        self.df[nd_col] = (self.df[nd_col]
                               .apply(lambda x: self.convert_dictionary(x)))
        dict_df = self.df[nd_col].apply(pd.Series).fillna(0)
        column_list = dict_df.columns.values.tolist()
        column_list = [l for l in column_list if
                       l not in ['action_type', 'value']]
        clean_df = pd.DataFrame()
        if 'action_type' in dict_df.columns:
            clean_df = self.clean_nested_df(dict_df, clean_df)
        for col in column_list:
            dirty_df = dict_df[col].apply(pd.Series).fillna(0)
            clean_df = self.clean_nested_df(dirty_df, clean_df)
        self.df = pd.concat([clean_df, self.df], axis=1)
        self.df = self.df.drop(nested_dict_col, axis=1)

    @staticmethod
    def clean_nested_df(dirty_df, clean_df):
        dirty_df = (dirty_df.pivot(columns='action_type', values='value')
                            .drop(0.0, axis=1).fillna(0))
        dirty_df = dirty_df.apply(pd.to_numeric)
        clean_df = pd.concat([clean_df, dirty_df], axis=1)
        clean_df = clean_df.groupby(clean_df.columns, axis=1).sum()
        return clean_df
