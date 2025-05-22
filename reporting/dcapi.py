import os
import re
import sys
import json
import time
import logging
import requests
import numpy as np
import pandas as pd
from io import StringIO
import reporting.utils as utl
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
    col_rename_dict = {}
    reach_metrics = {
        'impressionsCoviewed':  'Impressions (Co-Viewed)',
        'uniqueReachAverageImpressionFrequency':
            'Unique Reach: Average Impression Frequency',
        'uniqueReachAverageImpressionFrequencyCoviewed':
            'Unique Reach: Average Impression Frequency (Co-Viewed)',
        'uniqueReachImpressionReach': 'Unique Reach: Impression Reach',
        'uniqueReachImpressionReachCoviewed':
            'Unique Reach: Impression Reach (Co-Viewed)',
        'uniqueReachIncrementalClickReach':
            'Unique Reach: Incremental Click Reach',
        'uniqueReachIncrementalImpressionReach':
            'Unique Reach: Incremental Impression Reach',
        'uniqueReachIncrementalTotalReach':
            'Unique Reach: Incremental Total Reach',
        'uniqueReachIncrementalViewableImpressionReach':
            'Unique Reach: Incremental Viewable Impression Reach',
        'uniqueReachTotalReachCoviewed':
            'Unique Reach: Total Reach (Co-Viewed)',
        'uniqueReachViewableImpressionReach':
            'Unique Reach: Viewable Impression Reach'}
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
        self.report_id_dict = {}
        self.vendor_dict = {}
        self.campaign_dict = {}

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
        report_created = False
        report_types = [(False, False, False, False),
                        (True, False, False, False),
                        (True, True, False, False),
                        (True, True, True, False),
                        (True, True, False, True)]
        if self.original_report_id:
            self.report_ids.append(self.original_report_id)
            report_types = []
            report_created = True
        for report_type in report_types:
            reach_report = report_type[0]
            no_date = report_type[1]
            campaign_report = report_type[2]
            vendor_report = report_type[3]
            single_report_created = self.create_report(
                reach_report=reach_report, no_date=no_date,
                campaign_report=campaign_report, vendor_report=vendor_report)
            if single_report_created:
                report_created = True
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
            if report_id not in self.report_id_dict:
                self.report_id_dict[report_id] = {}
            self.report_id_dict[report_id]['url'] = files_url
        for report_id, report_param in self.report_id_dict.items():
            files_url = report_param['url']
            self.r = self.get_report(files_url)
            if not self.r:
                continue
            tdf = self.get_df_from_response()
            last_row = 1
            if 'A dash' in str(tdf.iloc[-1, 0]):
                last_row = 2
            tdf = utl.first_last_adj(tdf, first_row=1, last_row=last_row)
            tdf = self.rename_cols(tdf, report_param)
            if 'reach_report' in report_param:
                if not report_param['reach_report']:
                    self.check_for_campaign_vendor_dicts(tdf)
                else:
                    tdf = self.add_placement_name(tdf)
            df = pd.concat([df, tdf], ignore_index=True)
        return df

    def check_for_campaign_vendor_dicts(self, tdf):
        temp_col = 'PlacementPart'
        cols = [('Site (CM360)', 1, self.vendor_dict),
                ('Campaign', 15, self.campaign_dict)]
        for col_name, place_idx, storage_dict in cols:
            second_parts = tdf['Placement'].str.split('_').str[place_idx]
            pair_df = tdf[[col_name]].copy()
            pair_df[temp_col] = second_parts
            unique_pairs = pair_df.drop_duplicates(subset=[col_name])
            res = unique_pairs.set_index(col_name)[temp_col].to_dict()
            storage_dict.update(res)

    def add_placement_name(self, tdf):
        place_col = 'Placement'
        if place_col in tdf.columns:
            return tdf
        else:
            tdf[place_col] = ''
        is_blank = tdf[place_col].isna() | (
                    tdf[place_col].str.strip() == '')
        placement_parts = [[''] * 16 for _ in range(len(tdf))]
        campaign_parts = tdf['Campaign'].map(self.campaign_dict)
        for i, val in enumerate(campaign_parts):
            placement_parts[i][15] = val
        if 'Site (CM360)' in tdf.columns:
            vendor_parts = tdf['Site (CM360)'].map(self.vendor_dict)
            for i, val in enumerate(vendor_parts):
                placement_parts[i][1] = val
        new_placements = ['_'.join(map(str, parts))
                          for parts in placement_parts]
        tdf[place_col] = np.where(is_blank, new_placements, tdf[place_col])
        return tdf

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
            self.df = self.df.reset_index().copy()
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
            time.sleep(15)
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
            reach_error_str = "reach reports can't span more than 93 days."
            if reach_error_str in self.r.json()['error']['message']:
                attempt = 100
            else:
                logging.warning(
                    'Request error.  Retrying {}'.format(self.r.json()))
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

    def create_report(self, reach_report=False, no_date=False,
                      campaign_report=False, vendor_report=False):
        if self.original_report_id:
            return True
        report = self.create_report_params(reach_report, no_date,
                                           campaign_report, vendor_report)
        full_url = self.create_url('')
        self.r = self.make_request(full_url, method='post', body=report)
        if not self.r:
            return False
        report_id = self.r.json()['id']
        self.report_ids.append(report_id)
        report_params = {'reach_report': reach_report,
                         'no_date': no_date,
                         'campaign_report': campaign_report,
                         'vendor_report': vendor_report}
        self.report_id_dict[report_id] = report_params
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

    def create_report_params(self, reach_report=False, no_date=False,
                             campaign_report=False, vendor_report=False):
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
        if no_date:
            report['name'] = '{}_no_date'.format(report['name'])
        if campaign_report:
            report['name'] = '{}_campaign'.format(report['name'])
        if vendor_report:
            report['name'] = '{}_vendor'.format(report['name'])
        criteria = self.create_report_criteria(reach_report, no_date,
                                               campaign_report, vendor_report)
        criteria_str = 'criteria'
        if reach_report:
            criteria_str = 'reachCriteria'
        report[criteria_str] = criteria
        return report

    def create_report_criteria(self, reach_report=False, no_date=False,
                               campaign_report=False, vendor_report=False):
        metrics = self.default_metrics
        dimensions = self.default_fields
        if reach_report:
            metrics = list(self.reach_metrics.keys())
            dimensions = [x for x in dimensions
                          if 'creative' not in x and 'ad' not in x]
        if no_date:
            dimensions = [x for x in dimensions if 'date' not in x]
        if campaign_report:
            dimensions = [x for x in dimensions if x == 'campaign']
        if vendor_report:
            dimensions = [x for x in dimensions if x in ['campaign', 'site']]
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

    def rename_cols(self, tdf, report_param):
        rename_dict = self.col_rename_dict.copy()
        if 'reach_report' in report_param and report_param['reach_report']:
            col_str = ''
            for report_type in ['no_date', 'campaign_report', 'vendor_report']:
                if report_param[report_type]:
                    col_str = '{} - {}'.format(col_str, report_type)
            if col_str:
                for k, v in self.reach_metrics.items():
                    rename_dict[v] = '{}{}'.format(v, col_str)
        tdf.rename(columns=rename_dict, inplace=True)
        return tdf

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
