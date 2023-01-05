import io
import os
import sys
import json
import time
import logging
import requests
import oauthlib
import pandas as pd
import datetime as dt
import reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path


class AmzApi(object):
    base_url = 'https://advertising-api.amazon.com'
    eu_url = 'https://advertising-api-eu.amazon.com'
    fe_url = 'https://advertising-api-fe.amazon.com'
    refresh_url = 'https://api.amazon.com/auth/o2/token'
    def_metrics = [
        'campaignName', 'adGroupName', 'impressions', 'clicks', 'cost',
        'attributedConversions14d',
        'attributedConversions14dSameSKU', 'attributedUnitsOrdered14d',
        'attributedSales14d', 'attributedSales14dSameSKU']

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.advertiser_id = None
        self.campaign_id = None
        self.profile_id = None
        self.report_ids = []
        self.report_types = ['sp', 'hsa']
        self.config_list = None
        self.client = None
        self.headers = None
        self.version = '2'
        self.amazon_dsp = False
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading AMZ config file: {}'.format(config))
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
        self.advertiser_id = self.config['advertiser_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token]
        if 'campaign_id' in self.config:
            self.campaign_id = self.config['campaign_id']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in AMZ config file.'
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

    def get_client(self, errors=0):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        try:
            token = self.refresh_client_token(extra)
        except oauthlib.oauth2.rfc6749.errors.CustomOAuth2Error as e:
            logging.warning('Could not get token attempting again. '
                            'Oauth error as follows {}'.format(e))
            time.sleep(30)
            errors += 1
            if errors > 10:
                logging.warning('Could not get token exiting.')
                sys.exit(0)
            self.get_client(errors=errors + 1)
        self.client = OAuth2Session(self.client_id, token=token)

    def set_headers(self):
        self.headers = {'Amazon-Advertising-API-ClientId': self.client_id}
        if self.profile_id:
            self.headers['Amazon-Advertising-API-Scope'] = str(self.profile_id)

    def get_profiles(self):
        self.set_headers()
        for endpoint in [self.base_url, self.eu_url, self.fe_url]:
            url = '{}/v{}/profiles'.format(endpoint, self.version)
            r = self.make_request(url, method='GET', headers=self.headers)
            profile = [x for x in r.json() if
                       self.advertiser_id[1:] in x['accountInfo']['id']]
            if profile:
                self.profile_id = profile[0]['profileId']
                self.set_headers()
                self.base_url = endpoint
                return True
            dsp_profile = [x for x in r.json() if 'agency'
                           in x['accountInfo']['type']]
            if dsp_profile:
                dsp_id = str(dsp_profile[0]['profileId'])
                self.headers['Amazon-Advertising-API-Scope'] = dsp_id
                url = '{}/dsp/advertisers'.format(endpoint)
                r = self.make_request(url, method='GET', headers=self.headers)
                profile = [x for x in r.json()['response'] if
                           self.advertiser_id in x['advertiserId']]
                if profile:
                    self.profile_id = profile[0]['advertiserId']
                    self.set_headers()
                    self.base_url = endpoint
                    self.amazon_dsp = True
                    return True
        logging.warning('Could not find the specified profile, check that '
                        'the provided account ID {} is correct and API has '
                        'access.'.format(self.advertiser_id))
        return False

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
        return sd, ed

    def set_fields(self, fields):
        if fields:
            for field in fields:
                if field == 'nan':
                    self.report_types = ['sp', 'hsa']
                elif field == 'sp':
                    self.report_types.append('sp')
                elif field == 'hsa':
                    self.report_types.append('hsa')

    def get_data_default_check(self, sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() + dt.timedelta(days=1)
        if dt.datetime.today().date() == ed.date():
            ed += dt.timedelta(days=1)
        if self.amazon_dsp:
            if ed.date() > dt.datetime.today().date():
                ed = dt.datetime.today()
        sd, ed = self.date_check(sd, ed)
        self.set_fields(fields)
        return sd, ed

    def create_url(self, report_type='sp', version=True, record_type='adGroups',
                   report_id=False):
        url = self.base_url
        if version:
            url = '{}/v{}'.format(url, self.version)
        if report_id:
            url = '{}/reports/{}'.format(url, report_id)
        else:
            url = '{}/{}/{}/report'.format(url, report_type, record_type)
        return url

    def get_data(self, sd=None, ed=None, fields=None):
        self.report_ids = []
        self.df = pd.DataFrame()
        profile_found = self.get_profiles()
        if not profile_found:
            return self.df
        sd, ed = self.get_data_default_check(sd, ed, fields)
        if self.amazon_dsp:
            report_id = self.request_dsp_report(sd, ed)
            self.get_dsp_report(report_id)
        else:
            date_list = self.list_dates(sd, ed)
            report_made = self.request_reports_for_all_dates(date_list)
            if not report_made:
                logging.warning('Report not made returning blank df.')
                return self.df
            self.check_and_get_all_reports(self.report_ids)
        logging.info('All reports downloaded - returning dataframe.')
        self.df = self.filter_df_on_campaign(self.df)
        return self.df

    def filter_df_on_campaign(self, df):
        if self.campaign_id and 'campaignName' in df.columns:
            df = df[df['campaignName'].str.contains(self.campaign_id)]
        return df

    def request_dsp_report(self, sd, ed):
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        logging.info('Requesting DSP report for dates: {} to {}'.format(sd, ed))
        body = {
            "endDate": ed,
            "format": "JSON",
            "metrics": ['totalCost', 'impressions', 'clickThroughs',
                        'videoStart', 'videoFirstQuartile', 'videoMidpoint',
                        'videoThirdQuartile', 'videoComplete'],
            "type": "CAMPAIGN",
            "startDate": sd,
            "dimensions": ["ORDER", "LINE_ITEM", "CREATIVE"],
            "timeUnit": "DAILY"
        }
        self.headers['Accept'] = 'application/vnd.dspcreatereports.v3+json'
        url = '{}/accounts/{}/dsp/reports'.format(
            self.base_url, self.profile_id)
        for x in range(5):
            r = self.make_request(url, method='POST', body=body,
                                  headers=self.headers)
            if 'reportId' in r.json():
                report_id = r.json()['reportId']
                return report_id
            elif ('message' in r.json() and r.json()['message'] ==
                    'Too Many Requests'):
                logging.warning(
                    'Too many requests pausing.  Attempt: {}.  '
                    'Response: {}'.format((x + 1), r.json()))
                time.sleep(30)
            else:
                logging.warning('Error in request as follows: '
                                '{}'.format(r.json()))
                sys.exit(0)
        logging.warning('Could not generate report, exiting')
        sys.exit(0)

    def get_dsp_report(self, report_id):
        self.headers['Accept'] = 'application/vnd.dspgetreports.v3+json'
        url = '{}/accounts/{}/dsp/reports/{}'.format(
            self.base_url, self.profile_id, report_id)
        for attempt in range(100):
            logging.info(
                'Checking for report {} attempt {}'.format(
                    report_id, attempt + 1))
            r = self.make_request(url, method='GET', headers=self.headers)
            if 'status' in r.json():
                if r.json()['status'] == 'SUCCESS':
                    report_url = r.json()['location']
                    r = requests.get(report_url)
                    df = pd.DataFrame(r.json())
                    if df.empty:
                        logging.warning('Dataframe empty, likely no data  - '
                                        'returning empty dataframe')
                    else:
                        df['date'] = df['date'].apply(
                            lambda x: dt.datetime.fromtimestamp(
                                x / 1000).date())
                    self.df = df
                    break
                else:
                    time.sleep(15)
            else:
                logging.warning(
                    'No status in response as follows: {}'.format(r.json()))
                sys.exit(0)

    def request_reports_for_all_dates(self, date_list):
        for report_date in date_list:
            report_made = self.request_reports_for_date(report_date)
            if not report_made:
                return False
        return True

    def request_reports_for_date(self, report_date):
        for report_type in self.report_types:
            if report_type == 'hsa':
                has_video = [True, False]
            else:
                has_video = [False]
            for vid in has_video:
                report_made = self.make_report_request(report_date, report_type,
                                                       vid)
                if not report_made:
                    return False
        return True

    def make_report_request(self, report_date, report_type, vid):
        report_made = False
        report_date_string = dt.datetime.strftime(report_date, '%Y%m%d')
        logging.info(
            'Requesting report for date: {} type: {} video: {}'.format(
                report_date_string, report_type, vid))
        url = self.create_url(report_type=report_type)
        body = {'reportDate': report_date_string,
                'metrics': ','.join(self.def_metrics)}
        if vid:
            body['creativeType'] = 'video'
        r = self.make_request(url, method='POST', headers=self.headers,
                              body=body)
        if 'reportId' not in r.json():
            logging.warning('reportId not in json: {}'.format(r.json()))
            if 'code' in r.json() and r.json()['code'] == '406':
                logging.warning('Could not request date range is too long.')
            else:
                time.sleep(30)
                report_made = self.make_report_request(report_date, report_type,
                                                       vid)
        else:
            report_id = r.json()['reportId']
            self.report_ids.append(
                {'report_id': report_id, 'date': report_date,
                 'complete': False})
            report_made = True
        return report_made

    def check_and_get_all_reports(self, report_ids):
        for report_id in report_ids:
            self.check_and_get_report(report_id)
        rem_report_ids = [x for x in self.report_ids if not x['complete']]
        if rem_report_ids:
            self.check_and_get_all_reports(rem_report_ids)

    def check_and_get_report(self, report_id_dict):
        logging.info('Checking report for date: {}'.format(
            dt.datetime.strftime(report_id_dict['date'], '%Y-%m-%d')))
        report_id = report_id_dict['report_id']
        url = self.create_url(report_id=report_id)
        r = self.make_request(url, method='GET', headers=self.headers)
        if 'status' in r.json() and 'SUCCESS' in r.json()['status']:
            logging.debug('Report available - downloading.')
            url += '/download'
            r = self.make_request(url, method='GET', headers=self.headers,
                                  json_response=False)
            df = pd.read_json(io.BytesIO(r.content), compression='gzip')
            if not df.empty:
                if 'impressions' in df.columns:
                    df = df.loc[(df['impressions'] > 0)]
                df['Date'] = report_id_dict['date']
                self.df = self.df.append(df, ignore_index=True)
            self.report_ids = [
                x for x in self.report_ids if x['report_id'] != report_id]
            report_id_dict['complete'] = True
            self.report_ids.append(report_id_dict)
        else:
            if 'status' in r.json():
                logging.info('Report unavailable - waiting 30s.  \n'
                             'Current status: {}\n Current Status Details: {}'
                             ''.format(r.json()['status'],
                                       r.json()['statusDetails']))
            else:
                logging.info('Report unavailable - waiting 30s.  \n'
                             'Current error: {}'.format(r.json()))
            time.sleep(30)

    def make_request(self, url, method, body=None, params=None, headers=None,
                     attempt=1, json_response=True):
        self.get_client()
        try:
            self.r = self.raw_request(url, method, body=body, params=params,
                                      headers=headers)
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError) as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url=url, method=method, body=body,
                                       params=params, attempt=attempt,
                                       json_response=json_response)
        if json_response and 'error' in self.r.json():
            logging.warning('Request error.  Retrying {}'.format(self.r.json()))
            time.sleep(30)
            attempt += 1
            if attempt > 10:
                self.request_error()
            self.r = self.make_request(url=url, method=method, body=body,
                                       params=params, attempt=attempt,
                                       json_response=json_response)
        return self.r

    def raw_request(self, url, method, body=None, params=None, headers=None):
        kwargs = {}
        for kwarg in [(body, 'json'), (params, 'params'), (headers, 'headers')]:
            if kwarg[0]:
                kwargs[kwarg[1]] = kwarg[0]
        if method == 'POST':
            request_method = self.client.post
        else:
            request_method = self.client.get
        self.r = request_method(url, **kwargs)
        return self.r

    def request_error(self):
        logging.warning('Unknown error: {}'.format(self.r.text))
        sys.exit(0)

    @staticmethod
    def list_dates(sd, ed):
        dates = []
        while sd < ed:
            dates.append(sd)
            sd = sd + dt.timedelta(days=1)
        return dates
