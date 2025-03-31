import os
import re
import sys
import json
import time
import logging
import requests
import pandas as pd
from io import StringIO
import reporting.utils as utl
import reporting.vendormatrix as matrix
import reporting.vmcolumns as vmc
from requests_oauthlib import OAuth2Session

config_path = utl.config_path
base_url = 'https://www.googleapis.com/dfareporting'


class DcApi(object):
    default_fields = [
        'campaign', 'campaignId', 'site', 'placement',
        'date', 'placementId', 'creative', 'ad', 'creativeId', 'adId',
        'packageRoadblock', 'contentCategory', 'creativeType']
    pos = 'positionInContent'
    default_metrics = [
        'impressions', 'clicks', 'clickRate',
        'activeViewViewableImpressions',
        'activeViewMeasurableImpressions',
        'activeViewEligibleImpressions', 'totalConversions',
        'mediaCost', 'dv360Cost', 'dv360CostUsd',
        'totalConversionsRevenue', 'richMediaVideoViews',
        'richMediaTrueViewViews', 'richMediaVideoCompletions',
        'richMediaVideoThirdQuartileCompletes',
        'richMediaVideoMidpoints',
        'richMediaVideoFirstQuartileCompletes', 'richMediaVideoPlays']
    default_conversion_metrics = [
        'activityClickThroughConversions', 'totalConversions',
        'totalConversionsRevenue', 'activityViewThroughConversions',
        'activityViewThroughRevenue', 'activityClickThroughRevenue']
    col_rename_dict = {
        'Site (CM360)': 'Site (DCM)', 'DV360 Cost USD': 'DBM Cost USD',
        'DV360 Cost (Account Currency)': 'DBM Cost (Account Currency)'
    }
    reach_metrics = ['impressionsCoviewed',
                     'uniqueReachAverageImpressionFrequency',
                     'uniqueReachAverageImpressionFrequencyCoviewed',
                     'uniqueReachImpressionReach',
                     'uniqueReachImpressionReachCoviewed',
                     'uniqueReachIncrementalClickReach',
                     'uniqueReachIncrementalImpressionReach',
                     'uniqueReachIncrementalTotalReach',
                     'uniqueReachIncrementalViewableImpressionReach',
                     'uniqueReachTotalReachCoviewed',
                     'uniqueReachViewableImpressionReach']
    report_path = 'reports/'
    ad_path = 'advertisers/'
    camp_path = 'campaigns/'
    default_config_file_name = 'dcapi.json'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.usr_id = None
        self.advertiser_id = None
        self.campaign_id = None
        self.original_report_id = None
        self.report_ids = []
        self.config_list = None
        self.client = None
        self.date_range = None
        self.version = '4'
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading DC config file: {}'.format(config))
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
        self.usr_id = self.config['usr_id']
        self.original_report_id = self.config['report_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url, self.usr_id]
        if 'advertiser_id' in self.config:
            self.advertiser_id = self.remove_space(self.config['advertiser_id'])
        if 'campaign_id' in self.config:
            self.campaign_id = self.remove_space(self.config['campaign_id'])

    @staticmethod
    def remove_space(val):
        return str(val).replace(' ', '')

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in DC config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def refresh_client_token(self, extra, attempt=1):
        try:
            token = self.client.refresh_token(self.refresh_url, **extra)
        except requests.exceptions.ConnectionError as e:
            attempt += 1
            if attempt > 100:
                logging.warning('Max retries exceeded: {}'.format(e))
                token = None
            else:
                logging.warning('Connection error retrying 60s: {}'.format(e))
                token = self.refresh_client_token(extra, attempt)
        return token

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.refresh_client_token(extra)
        self.client = OAuth2Session(self.client_id, token=token)

    def create_user_url(self):
        vers_url = '/v{}'.format(self.version)
        usr_url = '/userprofiles/{}/'.format(self.usr_id)
        user_url = '{}{}{}'.format(base_url, vers_url, usr_url)
        return user_url

    def create_url(self, arg, path=report_path):
        user_url = self.create_user_url()
        full_url = (user_url + path + str(arg))
        return full_url

    def parse_fields(self, sd, ed, fields):
        sd, ed = utl.date_check(sd, ed)
        self.date_range = {
            'startDate': sd.strftime('%Y-%m-%d'),
            'endDate': ed.strftime('%Y-%m-%d')
        }
        if fields and fields != ['nan']:
            self.date_range = {'kind': 'dfareporting#dateRange',
                               'relativeDateRange': 'LAST_365_DAYS'}
            for field in fields:
                if field == 'platformType':
                    self.default_fields.append('platformType')
                if field == '60':
                    self.date_range['relativeDateRange'] = 'LAST_60_DAYS'
                elif field == '30':
                    self.date_range['relativeDateRange'] = 'LAST_30_DAYS'

    def get_data(self, sd=None, ed=None, fields=None):
        df = pd.DataFrame()
        self.parse_fields(sd, ed, fields)
        if self.original_report_id:
            self.report_ids.append(self.original_report_id)
        for x in [False, True]:
            report_created = self.create_report(reach_report=x)
        if not report_created:
            logging.warning('Report not created returning blank df.')
            return df
        for report_id in self.report_ids:
            logging.info('Getting report id: {}'.format(report_id))
            self.df = pd.DataFrame()
            files_url = self.get_files_url(report_id)
            if not files_url:
                logging.warning('Report not created returning blank df.')
                continue
            self.r = self.get_report(files_url)
            if not self.r:
                continue
            self.get_df_from_response()
            tdf = self.rename_cols()
            tdf = utl.first_last_adj(tdf, first_row=1, last_row=1)
            df = pd.concat([df, tdf], ignore_index=True)
        return df

    def find_first_line(self):
        for idx, x in enumerate(self.r.text.splitlines()):
            if idx > 1000:
                logging.warning('Could not find first line, returning empty df')
                return None
            if x == 'Report Fields':
                return idx

    def get_df_from_response(self):
        first_line = self.find_first_line()
        if first_line:
            self.df = pd.read_csv(StringIO(self.r.text), skiprows=first_line)
            self.df = self.df.reset_index()
        else:
            self.df = pd.DataFrame()
        return self.df

    def get_report(self, files_url):
        report_status = False
        for x in range(1, 401):
            report_status = self.check_file(files_url, attempt=x)
            if report_status:
                break
        if not report_status:
            logging.warning('Report could not download returning blank df.')
            return None
        logging.info('Report available.  Downloading.')
        report_url = '{}?alt=media'.format(files_url)
        self.r = self.make_request(report_url, 'get', json_response=False)
        return self.r

    def check_file(self, files_url, attempt=1):
        r = self.make_request(files_url, 'get')
        if 'status' in r.json() and r.json()['status'] == 'REPORT_AVAILABLE':
            return True
        else:
            logging.info('Report unavailable.  Attempt {}.  '
                         'Response: {}'.format(attempt, r.json()))
            time.sleep(30)
            return False

    def get_files_url(self, report_id):
        full_url = self.create_url(report_id)
        self.r = self.make_request('{}/run'.format(full_url), 'post')
        if not self.r:
            logging.warning('No files URL returning.')
            return None
        file_id = self.r.json()['id']
        files_url = '{}/files/{}'.format(full_url, file_id)
        return files_url

    def make_request(self, url, method, body=None, params=None, attempt=1,
                     json_response=True):
        self.get_client()
        try:
            self.r = self.raw_request(url, method, body=body, params=params)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url=url, method=method, body=body,
                                       params=params, attempt=attempt,
                                       json_response=json_response)
        if json_response and 'error' in self.r.json():
            logging.warning('Request error.  Retrying {}'.format(self.r.json()))
            time.sleep(10)
            attempt += 1
            if attempt > 10:
                self.request_error()
                return None
            self.r = self.make_request(url=url, method=method, body=body,
                                       params=params, attempt=attempt,
                                       json_response=json_response)
        return self.r

    def raw_request(self, url, method, body=None, params=None):
        if method == 'get':
            if body:
                self.r = self.client.get(url, json=body, params=params)
            else:
                self.r = self.client.get(url, params=params)
        elif method == 'post':
            if body:
                self.r = self.client.post(url, json=body, params=params)
            else:
                self.r = self.client.post(url, params=params)
        return self.r

    def request_error(self):
        logging.warning('Unknown error: {}'.format(self.r.text))

    def create_report(self, reach_report=False):
        if self.original_report_id:
            return True
        report = self.create_report_params(reach_report)
        full_url = self.create_url('')
        self.r = self.make_request(full_url, method='post', body=report)
        if not self.r:
            return False
        self.report_ids.append(self.r.json()['id'])
        return True

    def get_floodlight_tag_ids(self, fl_ids=None, next_page=None):
        if not fl_ids:
            fl_ids = []
        fl_url = self.create_user_url()
        params = {'advertiserId': self.advertiser_id}
        fl_url = '{}floodlightActivities'.format(fl_url)
        if next_page:
            params['pageToken'] = next_page
        self.r = self.make_request(fl_url, method='get', params=params)
        if not self.r:
            logging.warning('Could not make floodlights returning blank list.')
            return fl_ids
        if 'floodlightActivities' not in self.r.json():
            logging.warning('floodlightActivities not in response as follows: '
                            '\n{}'.format(self.r.json()))
        fl_ids.extend([x['id'] for x in self.r.json()['floodlightActivities']])
        if 'nextPageToken' in self.r.json() and self.r.json()['nextPageToken']:
            fl_ids = self.get_floodlight_tag_ids(
                fl_ids, next_page=self.r.json()['nextPageToken'])
        return fl_ids

    def create_report_params(self, reach_report=False):
        report_name = ''
        for name in [self.advertiser_id, self.campaign_id]:
            name = re.sub(r'\W+', '', name)
            report_name += '{}_'.format(name)
        report_name += 'standard_report'
        report = {
            'name': report_name,
            'type': 'STANDARD',
            'fileName': report_name,
            'format': 'CSV'
        }
        if reach_report:
            report['type'] = 'REACH'
            report['name'] = '{}_reach'.format(report['name'])
        criteria = self.create_report_criteria(reach_report)
        criteria_str = 'criteria'
        if reach_report:
            criteria_str = 'reachCriteria'
        report[criteria_str] = criteria
        return report

    def create_report_criteria(self, reach_report=False):
        metrics = self.default_metrics
        dimensions = self.default_fields
        if reach_report:
            metrics = self.reach_metrics
            dimensions = [x for x in dimensions
                          if 'creative' not in x and 'ad' not in x]
        criteria = {
            'dateRange': self.date_range,
            'dimensions': [{'kind': 'dfareporting#sortedDimension', 'name': x}
                           for x in dimensions],
            'metricNames': metrics,
            'dimensionFilters': [{
                'dimensionName': 'advertiser',
                'id': self.advertiser_id,
                'kind': 'dfareporting#dimensionValue'}]
        }
        if not reach_report:
            criteria['activities'] = {
                'metricNames': self.default_conversion_metrics,
                'kind': 'dfareporting#activities',
                'filters': []
            }
            fl_ids = self.get_floodlight_tag_ids()
            if fl_ids:
                criteria['activities']['filters'] = [
                    {'dimensionName': 'activity',
                     'id': x,
                     'kind': 'dfareporting#dimensionValue'} for x in fl_ids]
            else:
                logging.warning('No floodlight conversions found.')
                criteria.pop('activities')
        if self.campaign_id:
            campaign_filters = [
                {'dimensionName': 'campaign',
                 'id': x,
                 'kind': 'dfareporting#dimensionValue'}
                for x in self.campaign_id.split(',')]
            criteria['dimensionFilters'].extend(campaign_filters)
        return criteria

    def rename_cols(self):
        self.df.iloc[0] = self.df.iloc[0].replace(self.col_rename_dict)
        return self.df

    def test_connection(self, acc_col, camp_col, pre_col):
        success_msg = 'SUCCESS -- ID:'
        failure_msg = 'FAILURE:'
        self.get_client()
        if self.campaign_id is None:
            self.campaign_id = ''
        campaign_ids = self.campaign_id.split(',')
        results = []
        query_url = self.create_url(self.advertiser_id, self.ad_path)
        r = self.client.get(url=query_url)
        if r.status_code == 200:
            row = [acc_col, ' '.join([success_msg, str(self.advertiser_id)]),
                   True]
            results.append(row)
        else:
            r = r.json()
            row = [acc_col, ' '.join([failure_msg, r['error']['message']]),
                   False]
            results.append(row)
            return pd.DataFrame(data=results, columns=vmc.r_cols)
        for campaign in campaign_ids:
            query_url = self.create_url(campaign, self.camp_path)
            r = self.client.get(url=query_url)
            if r.status_code == 200:
                row = [camp_col, ' '.join([success_msg, str(campaign)]), True]
                results.append(row)
            else:
                r = r.json()
                row = [camp_col, ' '.join([failure_msg, r['error']['message']]),
                       False]
                results.append(row)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
