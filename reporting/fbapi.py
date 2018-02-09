import json
import logging
import time
import sys
import ast
import datetime as dt
import pandas as pd
import reporting.utils as utl
from facebookads.api import FacebookAdsApi
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.adsinsights import AdsInsights
from facebookads.exceptions import FacebookRequestError
from facebookads.adobjects.adreportrun import AdReportRun


def_params = ['campaign_name', 'adset_name', 'ad_name']

def_metrics = ['impressions', 'inline_link_clicks', 'spend',
               'video_10_sec_watched_actions', 'video_p25_watched_actions',
               'video_p50_watched_actions', 'video_p75_watched_actions',
               'video_p100_watched_actions', 'reach', 'frequency']

fields_actions = ['actions', 'action_values']

def_fields = def_params + def_metrics

nested_col = ['video_10_sec_watched_actions', 'video_p100_watched_actions',
              'video_p50_watched_actions', 'video_p25_watched_actions',
              'video_p75_watched_actions']

nested_dict_col = ['actions']

breakdown_age = ['age']
breakdown_gender = ['gender']
breakdown_placement = ['publisher_platform', 'platform_position',
                       'impression_device']
breakdown_country = ['country']


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
                'video_p100_watched_actions': 'Video Watches at 100%',
                'reach': 'Reach',
                'frequency': 'Frequency'}

config_path = utl.config_path


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
        self.async_requests = []

    def input_config(self, config):
        logging.info('Loading Facebook config file: ' + str(config))
        self.configfile = config_path + config
        self.load_config()
        self.check_config()
        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
        self.account = AdAccount(self.config['act_id'])

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
                logging.warning(item + 'not in FB config file.  Aborting.')
                sys.exit(0)

    @staticmethod
    def parse_fields(items):
        breakdowns = []
        fields = def_fields
        for item in items:
            if item == 'Actions':
                fields.extend(fields_actions)
            if item == 'Age':
                breakdowns.extend(breakdown_age)
            if item == 'Gender':
                breakdowns.extend(breakdown_gender)
            if item == 'Placement':
                breakdowns.extend(breakdown_placement)
            if item == 'Country':
                breakdowns.extend(breakdown_country)
        return fields, breakdowns

    @staticmethod
    def get_data_default_check(sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields is None:
            fields = def_fields
        return sd, ed, fields

    @staticmethod
    def date_check(sd, ed):
        sd = sd.date()
        ed = ed.date()
        if sd > ed:
            logging.warning('Start date greater than end date.  Start data' +
                            'was set to end date.')
            sd = ed
        return sd, ed

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        fields, breakdowns = self.parse_fields(fields)
        sd, ed = self.date_check(sd, ed)
        self.date_lists = self.set_full_date_lists(sd, ed)
        self.make_all_requests(fields, breakdowns)
        self.check_and_get_async_jobs(self.async_requests)
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

    def make_all_requests(self, fields, breakdowns):
        for date_list in self.date_lists:
            sd = date_list[0]
            ed = date_list[-1]
            self.field_lists = [fields]
            for field_list in self.field_lists:
                self.make_request(sd, ed, date_list, field_list, breakdowns)

    def make_request(self, sd, ed, date_list, field_list, breakdowns):
        logging.info('Making FB request for ' + str(sd) + ' to ' + str(ed))
        try:
            insights = self.account.get_insights(
                fields=field_list,
                params={'level': AdsInsights.Level.ad,
                        'breakdowns': breakdowns,
                        'time_range': {'since': str(sd), 'until': str(ed), },
                        'time_increment': 1, },
                async=True)
            deleted = self.account.get_insights(
                fields=field_list,
                params={'level': AdsInsights.Level.ad,
                        'breakdowns': breakdowns,
                        'time_range': {'since': str(sd), 'until': str(ed), },
                        'time_increment': 1,
                        'filtering': [{'field': 'ad.effective_status',
                                       'operator': 'IN',
                                       'value': ['DELETED', 'ARCHIVED']}]},
                async=True)
        except FacebookRequestError as e:
            self.request_error(e, date_list, field_list)
            return True
        self.async_requests.append(insights)
        self.async_requests.append(deleted)

    def check_and_get_async_jobs(self, async_jobs):
        self.async_requests = []
        for job in async_jobs:
            ar = AdReportRun(job['id'])
            report = ar.remote_read()
            percent = report['async_percent_completion']
            logging.info('FB async_job #' + str(job['id']) +
                         ' percent done: ' + str(percent) + '%')
            if percent == 100 and (report['async_status'] == 'Job Completed'):
                try:
                    complete_job = list(ar.get_result())
                except FacebookRequestError as e:
                    self.request_error(e)
                    self.async_requests.append(job)
                    complete_job = None
                if complete_job:
                    self.df = self.df.append(complete_job, ignore_index=True)
            else:
                self.async_requests.append(job)
        if self.async_requests:
            time.sleep(30)
            self.check_and_get_async_jobs(self.async_requests)

    def get_all_async_jobs(self, async_jobs):
        for async_job in async_jobs:
            percent = self.get_async_job_percent(async_job)
            while percent < 100 and type(percent) is int:
                percent = self.get_async_job_percent(async_job)
            complete_job = list(async_job.get_result())
            if complete_job:
                self.df = self.df.append(complete_job, ignore_index=True)

    @staticmethod
    def get_async_job_percent(async_job):
        job = async_job.remote_read()
        percent = job['async_percent_completion']
        logging.info('FB async_job #' + str(async_job['id']) +
                     ' percent done: ' + str(percent) + '%')
        time.sleep(30)
        return percent

    def request_error(self, e, date_list=None, field_list=None):
        if e._api_error_code == 190:
            logging.error('Facebook Access Token invalid.  Aborting.')
            sys.exit(0)
        elif e._api_error_code == 2:
            logging.warning('An unexpected error occurred.  Retrying request later.')
            return True
        elif e._api_error_code == 17:
            logging.warning('Facebook rate limit reached.  Pausing for ' +
                            '300 seconds.')
            time.sleep(300)
            self.date_lists.append(date_list)
            return True
        elif e._api_error_code == 1:
            if date_list[0] == date_list[-1]:
                logging.warning('Already daily.  Reducing requested fields.' +
                                'Error as follows: ' + str(e))
                metrics = [x for x in field_list if x not in def_params]
                fh, bh = self.split_list(metrics)
                if fh and bh:
                    self.field_lists.append(def_params + fh)
                    self.field_lists.append(def_params + bh)
                else:
                    self.field_lists.append(field_list)
                return True
            logging.warning('Too much data queried.  Reducing time scale')
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
    def set_full_date_lists(sd, ed):
        dates = []
        while sd <= ed:
            dates.append(sd)
            sd = sd + dt.timedelta(days=1)
        date_lists = [dates[i:i + 7] for i in range(0, len(dates), 7)]
        return date_lists

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
                    .fillna(0))
        for col in [x for x in [0.0, 'action_type'] if x in dirty_df.columns]:
            dirty_df = dirty_df.drop(col, axis=1)
        dirty_df = dirty_df.apply(pd.to_numeric)
        clean_df = pd.concat([clean_df, dirty_df], axis=1)
        clean_df = clean_df.groupby(clean_df.columns, axis=1).sum()
        return clean_df
