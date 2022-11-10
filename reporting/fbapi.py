import os
import ast
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError,\
    FacebookBadObjectError
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun


def_params = ['campaign_name', 'adset_name', 'ad_name', 'ad_id']

def_metrics = ['impressions', 'inline_link_clicks', 'spend',
               'video_thruplay_watched_actions', 'video_p25_watched_actions',
               'video_p50_watched_actions', 'video_p75_watched_actions',
               'video_p100_watched_actions', 'reach', 'frequency',
               'video_play_actions', 'estimated_ad_recallers', 'clicks']

fields_actions = ['actions', 'action_values']

ab_device = ['action_device', 'action_type']

def_fields = def_params + def_metrics

nested_col = ['video_thruplay_watched_actions', 'video_p100_watched_actions',
              'video_p50_watched_actions', 'video_p25_watched_actions',
              'video_p75_watched_actions', 'video_play_actions']

nested_dict_col = ['actions']

breakdown_age = ['age']
breakdown_gender = ['gender']
breakdown_publisher = ['publisher_platform']
breakdown_placement = ['publisher_platform', 'platform_position',
                       'impression_device']
breakdown_impdevice = ['impression_device']
breakdown_country = ['country']
breakdown_device = ['device_platform']
breakdown_product = ['product_id']

ad_status_enabled = ['ACTIVE', 'PAUSED', 'PENDING_REVIEW', 'DISAPPROVED',
                     'PREAPPROVED', 'CAMPAIGN_PAUSED', 'ADSET_PAUSED',
                     'PENDING_BILLING_INFO', 'IN_PROCESS', 'WITH_ISSUES']
ad_status_disabled = ['DELETED', 'ARCHIVED']


col_name_dic = {'date_start': 'Reporting Starts',
                'date_stop': 'Reporting Ends',
                'campaign_name': 'Campaign Name', 'adset_name': 'Ad Set Name',
                'ad_name': 'Ad Name', 'impressions': 'Impressions',
                'inline_link_clicks': 'Link Clicks',
                'spend': 'Amount Spent (USD)',
                'video_thruplay_watched_actions': '10-Second Video Views',
                'video_p25_watched_actions': 'Video Watches at 25%',
                'video_p50_watched_actions': 'Video Watches at 50%',
                'video_p75_watched_actions': 'Video Watches at 75%',
                'video_p100_watched_actions': 'Video Watches at 100%',
                'reach': 'Reach',
                'frequency': 'Frequency',
                'ad_id': 'ad_id',
                'adset_id': 'adset_id',
                'campaign_id': 'campaign_id',
                'estimated_ad_recallers': 'estimated_ad_recallers'}

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
        self.campaign_filter = None
        self.config_list = []
        self.date_lists = None
        self.field_lists = None
        self.async_requests = []

    def input_config(self, config):
        logging.info('Loading Facebook config file: {}'.format(config))
        self.configfile = os.path.join(config_path, config)
        self.load_config()
        self.check_config()
        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
        self.account = AdAccount(self.config['act_id'])

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.configfile))
            sys.exit(0)
        self.app_id = self.config['app_id']
        self.app_secret = self.config['app_secret']
        self.access_token = self.config['access_token']
        self.act_id = self.config['act_id']
        self.config_list = [self.app_id, self.app_secret, self.access_token,
                            self.act_id]
        if 'campaign_filter' in self.config:
            self.campaign_filter = self.config['campaign_filter']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in FB config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    @staticmethod
    def parse_fields(items):
        breakdowns = []
        action_breakdowns = []
        attribution_window = []
        fields = def_fields
        time_breakdown = 1
        level = AdsInsights.Level.ad
        for item in items:
            if item == 'Actions':
                fields.extend(fields_actions)
            if item == 'Age':
                breakdowns.extend(breakdown_age)
            if item == 'Gender':
                breakdowns.extend(breakdown_gender)
            if item == 'Placement':
                breakdowns.extend(breakdown_placement)
            if item == 'Publisher':
                breakdowns.extend(breakdown_publisher)
            if item == 'Country':
                breakdowns.extend(breakdown_country)
            if item == 'Impression Device':
                breakdowns.extend(breakdown_impdevice)
            if item == 'Device':
                breakdowns.extend(breakdown_device)
            if item == 'Product':
                breakdowns.extend(breakdown_product)
            if item == 'Action Device':
                action_breakdowns.extend(ab_device)
            if 'Attribution' in item:
                item = item.split('::')
                attribution_window = ['{}d_click'.format(item[1]),
                                      '{}d_view'.format(item[2])]
            if item == 'Total':
                time_breakdown = 'all_days'
            if item == 'Adset':
                level = AdsInsights.Level.adset
            if item == 'Campaign':
                level = AdsInsights.Level.campaign
        return fields, breakdowns, action_breakdowns, attribution_window,\
            time_breakdown, level

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
            logging.warning('Start date greater than end date.  Start date '
                            'was set to end date.')
            sd = ed
        return sd, ed

    def get_data(self, sd=None, ed=None, fields=None):
        self.df = pd.DataFrame()
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        fields, breakdowns, action_breakdowns, attr, time_breakdown, level = \
            self.parse_fields(fields)
        sd, ed = self.date_check(sd, ed)
        self.date_lists = self.set_full_date_lists(sd, ed)
        self.make_all_requests(fields, breakdowns, action_breakdowns, attr,
                               time_breakdown, level)
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

    def make_all_requests(self, fields, breakdowns, action_breakdowns, attr,
                          time_breakdown, level):
        self.field_lists = [fields]
        if time_breakdown == 'all_days':
            sd = self.date_lists[0][0]
            ed = self.date_lists[-1][0]
            self.request_for_fields(sd, ed, self.date_lists, fields,
                                    breakdowns, action_breakdowns, attr,
                                    time_breakdown, level)
        else:
            for date_list in self.date_lists:
                sd = date_list[0]
                ed = date_list[-1]
                self.request_for_fields(sd, ed, date_list, fields, breakdowns,
                                        action_breakdowns, attr,
                                        time_breakdown, level)

    def request_for_fields(self, sd, ed, date_list, fields, breakdowns,
                           action_breakdowns, attr, time_breakdown, level):
        self.field_lists = [fields]
        for field_list in self.field_lists:
            for ad_status in [ad_status_enabled, ad_status_disabled]:
                for x in range(10):
                    success = self.make_request(
                        sd, ed, date_list, field_list, breakdowns,
                        action_breakdowns, attr, ad_status,
                        time_breakdown, level)
                    if success:
                        break
                    else:
                        logging.warning('Retrying, attempt #{}'.format(x))

    def get_insights(self, field_list, params, date_list):
        insights = None
        try:
            insights = self.account.get_insights(
                fields=field_list,
                params=params,
                is_async=True)
        except FacebookRequestError as e:
            self.request_error(e, date_list, field_list)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
        except requests.exceptions.ConnectionError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
        return insights

    def make_request(self, sd, ed, date_list, field_list, breakdowns,
                     action_breakdowns, attribution_window, ad_status,
                     time_breakdown=1, level=AdsInsights.Level.ad,
                     times_requested=1):
        logging.info('Making FB request for {} to {}'.format(sd, ed))
        params = {'level': level,
                  'breakdowns': breakdowns,
                  'time_range': {'since': str(sd), 'until': str(ed), },
                  'time_increment': time_breakdown,
                  'filtering': [{'field': 'ad.effective_status',
                                 'operator': 'IN',
                                 'value': ad_status}]
                  }
        if action_breakdowns:
            params['action_breakdowns'] = action_breakdowns
        if attribution_window:
            params['action_attribution_windows'] = attribution_window
        if self.campaign_filter:
            params['filtering'].append({'field': 'campaign.name',
                                        'operator': 'CONTAIN',
                                        'value': self.campaign_filter})
        insights = None
        for x in range(10):
            insights = self.get_insights(field_list, params, date_list)
            if insights:
                break
        if insights:
            init_dict = {
                'sd': sd, 'ed': ed, 'date_list': date_list,
                'field_list': field_list, 'breakdowns': breakdowns,
                'action_breakdowns': action_breakdowns,
                'attribution_window': attribution_window,
                'ad_status': ad_status,
                'time_breakdown': time_breakdown, 'level': level,
                'insights': insights, 'times_requested': times_requested}
            fb_request = FacebookRequest(init_dict=init_dict)
            self.async_requests.append(fb_request)
            return True
        else:
            logging.warning('Could not get insights attempting again.')
            return False

    def get_report(self, ar):
        try:
            report = ar.api_get()
        except FacebookRequestError as e:
            self.request_error(e)
            report = self.get_report(ar)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            report = self.get_report(ar)
        except requests.exceptions.ConnectionError as e:
            logging.warning('Warning ConnectionError as follows {}'.format(e))
            time.sleep(30)
            report = self.get_report(ar)
        return report

    def reset_report_request(self, fb_request):
        self.make_request(
            fb_request.sd, fb_request.ed, fb_request.date_list,
            fb_request.field_list, fb_request.breakdowns,
            fb_request.action_breakdowns,
            fb_request.attribution_window, fb_request.ad_status,
            fb_request.time_breakdown, fb_request.level,
            fb_request.times_requested + 1)

    def check_and_get_async_jobs(self, async_jobs):
        self.async_requests = []
        for fb_request in async_jobs:
            try:
                job = fb_request.insights
            except AttributeError as e:
                logging.warning(
                    'A FB async_job does not contain insights and will '
                    'be requested again.  This is request #{} Error: {}'.format(
                        fb_request.times_requested, e))
                self.reset_report_request(fb_request)
                continue
            ar = AdReportRun(job['id'])
            report = self.get_report(ar)
            percent = report['async_percent_completion']
            need_reset = fb_request.check_last_percent(percent)
            if need_reset:
                logging.warning(
                    'FB async_job #{} has been stuck for {} attempts and will '
                    'be requested again.  This is request #{}'.format(
                        job['id'], fb_request.times_requested * 10,
                        fb_request.times_requested))
                self.reset_report_request(fb_request)
                continue
            logging.info('FB async_job #{} percent done '
                         '{}%'.format(job['id'], percent))
            if percent == 100 and (report['async_status'] == 'Job Completed'):
                try:
                    complete_job = list(ar.get_result())
                except FacebookRequestError as e:
                    self.request_error(e)
                    self.async_requests.append(fb_request)
                    complete_job = None
                except FacebookBadObjectError as e:
                    logging.warning('Facebook Bad Object Error: {}'.format(e))
                    self.async_requests.append(fb_request)
                    complete_job = None
                except requests.exceptions.SSLError as e:
                    logging.warning('Warning SSLError as follows {}'.format(e))
                    self.async_requests.append(fb_request)
                    complete_job = None
                    time.sleep(30)
                if complete_job:
                    self.df = self.df.append(complete_job, ignore_index=True)
                    fb_request.complete = True
            else:
                self.async_requests.append(fb_request)
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
        job = async_job.api_get()
        percent = job['async_percent_completion']
        logging.info('FB async_job #{}'' percent done:'
                     '{}%'.format(async_job['id'], percent))
        time.sleep(30)
        return percent

    def request_error(self, e, date_list=None, field_list=None):
        if e._api_error_code == 190:
            logging.info(e)
            logging.error('Facebook Access Token invalid.  Aborting.')
            sys.exit(0)
        elif e._api_error_code == 2 or e._api_error_code == 100:
            logging.warning('An unexpected error occurred.  '
                            'Retrying request later. {}'.format(e))
            return True
        elif e._api_error_code == 17:
            logging.warning('Facebook rate limit reached.  Pausing for '
                            '300 seconds.')
            time.sleep(300)
            self.date_lists.append(date_list)
            return True
        elif e._api_error_code == 1 and date_list is not None:
            if date_list[0] == date_list[-1]:
                logging.warning('Already daily.  Reducing requested fields.'
                                'Error as follows: {}'.format(e))
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
        elif e._api_error_code == 1:
            logging.warning('Unknown FB error has occurred. Retrying.')
            return True
        else:
            logging.error('Aborting as the Facebook API call resulted '
                          'in the following error: {}'.format(e))
            if e._api_error_code:
                logging.error('Api error subcode: {}'.format(e._api_error_code))
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
            dates.append([sd])
            sd = sd + dt.timedelta(days=1)
        return dates

    def nested_dicts_to_cols(self, nd_col):
        self.df[nd_col] = (self.df[nd_col]
                               .apply(lambda x: self.convert_dictionary(x)))
        dict_df = self.df[nd_col].apply(pd.Series).fillna(0)
        column_list = dict_df.columns.values.tolist()
        column_list = [x for x in column_list if
                       x not in ['action_type', 'value']]
        clean_df = pd.DataFrame()
        if 'action_type' in dict_df.columns:
            column_list += ['action_type']
        for col in column_list:
            dirty_df = dict_df[col].apply(pd.Series).fillna(0)
            if 'action_type' in dirty_df.columns:
                dirty_df = utl.data_to_type(dirty_df, str_col=['action_type'])
                clean_df = self.clean_nested_df(dirty_df, clean_df)
        self.df = pd.concat([clean_df, self.df], axis=1)  # type: pd.DataFrame
        self.df = self.df.drop(nested_dict_col, axis=1)  # type: pd.DataFrame

    @staticmethod
    def clean_nested_df(dirty_df, clean_df):
        values = [x for x in dirty_df.columns if x != 'action_type']
        dirty_df = utl.data_to_type(dirty_df, float_col=values)
        dirty_df = pd.pivot_table(dirty_df, columns='action_type',
                                  values=values, index=dirty_df.index,
                                  aggfunc='sum', fill_value=0)
        if type(dirty_df.columns) == pd.MultiIndex:
            dirty_df.columns = [' - '.join([str(y) for y in x]) if x[0] !=
                                'value' else x[1] for x in dirty_df.columns]
        for col in [x for x in [0.0, 'action_type'] if x in dirty_df.columns]:
            dirty_df = dirty_df.drop(col, axis=1)
        dirty_df = dirty_df.apply(pd.to_numeric)
        clean_df = pd.concat([clean_df, dirty_df], axis=1)
        clean_df = clean_df.groupby(clean_df.columns, axis=1).sum()  # type: pd.DataFrame
        return clean_df


class FacebookRequest(object):
    def __init__(self, init_dict=None):
        self.sd = None
        self.ed = None
        self.date_list = None
        self.field_list = None
        self.breakdowns = None
        self.action_breakdowns = None
        self.attribution_window = None
        self.ad_status = None
        self.time_breakdown = None
        self.times_requested = 1
        self.insights = None
        self.level = None
        self.complete = False
        self.last_percent = 0
        self.consecutive_same_percent = 0
        self.init_dict = init_dict
        if self.init_dict:
            self.set_from_init_dict()

    def set_from_init_dict(self):
        for k, v in self.init_dict.items():
            setattr(self, k, v)

    def check_last_percent(self, new_percent):
        if new_percent == self.last_percent:
            self.consecutive_same_percent += 1
        self.last_percent = new_percent
        if self.consecutive_same_percent > 10 * self.times_requested:
            return True
        else:
            return False
