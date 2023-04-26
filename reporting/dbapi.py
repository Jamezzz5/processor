import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path
base_url = 'https://www.googleapis.com/doubleclickbidmanager/v1.1'


class DbApi(object):
    default_groups = [
        'FILTER_ADVERTISER', 'FILTER_ADVERTISER_NAME',
        'FILTER_ADVERTISER_CURRENCY',
        'FILTER_INSERTION_ORDER', 'FILTER_INSERTION_ORDER_NAME',
        'FILTER_LINE_ITEM', 'FILTER_LINE_ITEM_NAME',
        'FILTER_DATE',
        'FILTER_LINE_ITEM_TYPE',
        'FILTER_MEDIA_PLAN', 'FILTER_MEDIA_PLAN_NAME',
        'FILTER_CREATIVE_ID', 'FILTER_CREATIVE']
    default_metrics = [
        'METRIC_IMPRESSIONS', 'METRIC_BILLABLE_IMPRESSIONS', 'METRIC_CLICKS',
        'METRIC_CTR', 'METRIC_TOTAL_CONVERSIONS', 'METRIC_LAST_CLICKS',
        'METRIC_LAST_IMPRESSIONS', 'METRIC_REVENUE_ADVERTISER',
        'METRIC_MEDIA_COST_ADVERTISER', 'METRIC_BILLABLE_COST_ADVERTISER',
        'METRIC_TOTAL_MEDIA_COST_ADVERTISER', 'METRIC_FEE16_ADVERTISER',
        'METRIC_RICH_MEDIA_VIDEO_FIRST_QUARTILE_COMPLETES',
        'METRIC_RICH_MEDIA_VIDEO_MIDPOINTS',
        'METRIC_RICH_MEDIA_VIDEO_THIRD_QUARTILE_COMPLETES',
        'METRIC_RICH_MEDIA_VIDEO_COMPLETIONS', 'METRIC_RICH_MEDIA_VIDEO_PLAYS']

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.advertiser_id = None
        self.campaign_id = None
        self.report_id = None
        self.config_list = None
        self.client = None
        self.start_time = None
        self.end_time = None
        self.df = pd.DataFrame()
        self.r = None
        self.v = 1

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading DB config file: {}'.format(config))
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
        self.report_id = self.config['report_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url]
        if 'advertiser_id' in self.config:
            self.advertiser_id = self.config['advertiser_id']
        if 'campaign_id' in self.config:
            self.campaign_id = self.config['campaign_id']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in DB config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    @staticmethod
    def get_data_default_check(sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        return sd, ed, fields

    def parse_fields(self, sd, ed, fields):
        self.start_time = round((sd - dt.datetime.utcfromtimestamp(0))
                                .total_seconds() * 1000)
        self.end_time = round((ed - dt.datetime.utcfromtimestamp(0))
                              .total_seconds() * 1000)
        if fields and fields != ['nan']:
            self.default_metrics += [
                'METRIC_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS',
                'METRIC_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS',
                'METRIC_ACTIVE_VIEW_UNVIEWABLE_IMPRESSIONS'
            ]

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
    def create_query_url():
        query_url = '{}/query'.format(base_url)
        return query_url

    def create_url(self):
        query_url = self.create_query_url()
        full_url = '{}/{}'.format(query_url, self.report_id)
        return full_url

    def get_data(self, sd=None, ed=None, fields=None):
        report_created = self.create_report(sd, ed, fields)
        if not report_created:
            logging.warning('Report was not created, check for errors.')
            return pd.DataFrame()
        self.get_raw_data()
        self.check_empty_df()
        self.remove_footer()
        return self.df

    def check_empty_df(self):
        if self.df.iloc[0, 0] == 'No data returned by the reporting service.':
            logging.warning('No data in response, returning empty df.')
            self.df = pd.DataFrame()

    def remove_footer(self):
        self.df = self.df[~self.df.isnull().any(axis=1)]

    def create_report(self, sd, ed, fields):
        if self.report_id:
            return True
        logging.info('No report specified, creating.')
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        self.parse_fields(sd, ed, fields)
        query_url = self.create_query_url()
        params = self.create_report_params()
        metadata = self.create_report_metadata()
        body = {
            'kind': 'doubleclickbidmanager#query',
            'metadata': metadata,
            'params': params,
            'schedule': {
                'frequency': 'ONE_TIME'},
            "reportDataStartTimeMs": self.start_time,
            "reportDataEndTimeMs": self.end_time,
            "timezoneCode": 'America/Los_Angeles'
        }
        self.r = self.make_request(query_url, method='post', body=body)
        if 'queryId' not in self.r.json():
            logging.warning('queryId not in response:{}'.format(self.r.json()))
            return False
        self.report_id = self.r.json()['queryId']
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f)
        logging.info(
            'Report created -- ID: {}. Pausing for 30s.'.format(self.report_id))
        time.sleep(30)
        return True

    def make_request(self, url, method, body=None):
        self.get_client()
        try:
            self.r = self.raw_request(url, method, body=body)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url, method, body=body)
        return self.r

    def raw_request(self, url, method, body=None):
        if method == 'get':
            if body:
                self.r = self.client.get(url, json=body)
            else:
                self.r = self.client.get(url)
        elif method == 'post':
            if body:
                self.r = self.client.post(url, json=body)
            else:
                self.r = self.client.post(url)
        return self.r

    def get_raw_data(self):
        full_url = self.create_url()
        self.r = self.make_request(full_url, method='post')
        for x in range(1, 101):
            self.r = self.make_request(full_url, method='get')
            if 'metadata' in self.r.json().keys():
                break
            else:
                logging.warning('Rate limit exceeded. Pausing. '
                                'Response: {}'.format(self.r.json()))
                time.sleep(60)
        report_url = (self.r.json()['metadata']
                      ['googleCloudStoragePathForLatestReport'])
        if report_url:
            logging.info('Found report url, downloading.')
            self.df = utl.import_read_csv(report_url, file_check=False,
                                          error_bad='warn')
        else:
            logging.warning('Report does not exist.  Create it.')
            sys.exit(0)

    def create_report_params(self):
        params = {
            'filters': [{'type': 'FILTER_ADVERTISER',
                         'value': self.advertiser_id}],
            'groupBys': self.default_groups,
            'includeInviteData': True,
            'metrics': self.default_metrics,
            'type': 'TYPE_GENERAL'}
        if self.campaign_id:
            campaign_filters = [
                {'type': 'FILTER_MEDIA_PLAN',
                 'value': x} for x in str(self.campaign_id).split(',')]
            params['filters'].extend(campaign_filters)
        return params

    def create_report_metadata(self):
        report_name = '{}_{}_report'.format(
            self.advertiser_id, self.campaign_id)
        metadata = {
            'dataRange': 'CUSTOM_DATES',
            'format': 'CSV',
            'title': report_name}
        return metadata
