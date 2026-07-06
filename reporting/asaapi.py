import os
import sys
import jwt
import time
import json
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
import reporting.vmcolumns as vmc

config_path = utl.config_path


class AsaApi(object):
    config_path = utl.config_path
    default_config_file_name = 'asaapi.json'
    token_url = 'https://appleid.apple.com/auth/oauth2/token'
    audience = 'https://appleid.apple.com'
    scope = 'searchadsorg'
    base_url = 'https://api.searchads.apple.com/api/v5'
    campaign_url = '{}/campaigns'.format(base_url)
    acls_url = '{}/acls'.format(base_url)
    report_url = '{}/reports/campaigns'.format(base_url)
    client_id_str = 'client_id'
    team_id_str = 'team_id'
    key_id_str = 'key_id'
    org_id_str = 'org_id'
    private_key_str = 'private_key'
    private_key_file_str = 'private_key_file'
    campaign_filter_str = 'campaign_filter'
    page_limit = 1000
    max_report_days = 90
    cols = {
        'date': 'Date',
        'campaign': 'Campaign Name',
        'ad_group': 'Ad Group Name',
        'impressions': 'Impressions',
        'taps': 'Taps',
        'totalInstalls': 'Installs',
        'localSpend': 'Spend'
    }

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.team_id = None
        self.key_id = None
        self.org_id = None
        self.private_key = None
        self.campaign_filter = None
        self.config_list = None
        self.access_token = None
        self.headers = None
        self.key_list = [self.client_id_str, self.team_id_str,
                         self.key_id_str, self.org_id_str,
                         self.private_key_str]

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info(
            'Loading Apple Search Ads config file: {}'.format(config))
        self.config_file = os.path.join(self.config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.set_config_values(self.config)

    def load_config_dict(self, config):
        """Populate credentials from an in-memory dict, bypassing the
        config file load (used by the app-layer credential vault)."""
        self.config = config
        self.set_config_values(config)

    def set_config_values(self, config):
        self.client_id = config.get(self.client_id_str)
        self.team_id = config.get(self.team_id_str)
        self.key_id = config.get(self.key_id_str)
        self.org_id = config.get(self.org_id_str)
        self.private_key = config.get(self.private_key_str)
        private_key_file = config.get(self.private_key_file_str)
        if not self.private_key and private_key_file:
            key_file = os.path.join(self.config_path, private_key_file)
            try:
                with open(key_file, 'r') as f:
                    self.private_key = f.read()
            except IOError:
                logging.error('{} not found.  Aborting.'.format(key_file))
                sys.exit(0)
        self.campaign_filter = config.get(self.campaign_filter_str)
        self.config_list = [self.client_id, self.team_id, self.key_id,
                            self.org_id, self.private_key]

    def check_config(self):
        for item in self.config_list:
            if not item:
                logging.warning('Apple Search Ads config file is missing '
                                'values.  Aborting.')
                sys.exit(0)

    def get_client_secret(self):
        now = int(time.time())
        jwt_headers = {
            'alg': 'ES256',
            'kid': self.key_id
        }
        jwt_payload = {
            'iss': self.team_id,
            'iat': now,
            'exp': now + 3600,
            'aud': self.audience,
            'sub': self.client_id
        }
        client_secret = jwt.encode(payload=jwt_payload, key=self.private_key,
                                   algorithm='ES256', headers=jwt_headers)
        return client_secret

    def set_headers(self):
        logging.info('Retrieving access token')
        client_secret = self.get_client_secret()
        params = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': client_secret,
            'scope': self.scope}
        token_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.post(self.token_url, data=params, headers=token_headers)
        try:
            token = r.json()
        except json.decoder.JSONDecodeError as e:
            logging.warning('Response not json, exiting. \nError: {}\n '
                            'Response: {}'.format(e, r.text))
            sys.exit(0)
        if 'access_token' not in token:
            logging.warning('Could not retrieve access token.  '
                            'Response: {}'.format(token))
            sys.exit(0)
        self.access_token = token['access_token']
        self.headers = {
            'Authorization': 'Bearer {}'.format(self.access_token),
            'X-AP-Context': 'orgId={}'.format(self.org_id),
            'Content-Type': 'application/json'}
        logging.info('Headers set with access token')

    def make_request(self, url, method='get', json_body=None, params=None,
                     attempt=1):
        if attempt > 10:
            logging.warning('Max request attempts exceeded for {}.  '
                            'Returning None.'.format(url))
            return None
        if method == 'post':
            request_method = requests.post
        else:
            request_method = requests.get
        try:
            r = request_method(url, headers=self.headers, json=json_body,
                               params=params)
        except requests.exceptions.ConnectionError as e:
            logging.warning('Connection error, pausing for 60s '
                            'and retrying: {}'.format(e))
            time.sleep(60)
            return self.make_request(url, method, json_body, params,
                                     attempt + 1)
        if r.status_code == 401:
            logging.warning('Token expired, requesting new token '
                            'and retrying.')
            self.set_headers()
            return self.make_request(url, method, json_body, params,
                                     attempt + 1)
        if r.status_code == 429:
            logging.warning('Rate limit reached, pausing for 60s '
                            'and retrying.')
            time.sleep(60)
            return self.make_request(url, method, json_body, params,
                                     attempt + 1)
        return r

    @staticmethod
    def format_dates(sd, ed):
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    @staticmethod
    def date_check(sd, ed):
        if sd > ed:
            logging.warning('Start date greater than end date.  '
                            'Start date was set to end date.')
            sd = ed
        return sd, ed

    def get_data_default_check(self, sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None or ed.date() > dt.datetime.today().date():
            ed = dt.datetime.today()
        sd, ed = self.date_check(sd, ed)
        max_lookback = dt.datetime.today() - dt.timedelta(
            days=self.max_report_days - 1)
        if sd < max_lookback:
            logging.warning('Apple Search Ads only reports daily data {} '
                            'days back.  Start date was set to {}.'.format(
                                self.max_report_days, max_lookback.date()))
            sd = max_lookback
        return sd, ed

    def get_campaigns(self):
        campaigns = []
        for x in range(1, 101):
            params = {'limit': self.page_limit,
                      'offset': (x - 1) * self.page_limit}
            r = self.make_request(self.campaign_url, params=params)
            if not r or 'data' not in r.json():
                logging.warning('Could not get campaigns.  Response: '
                                '{}'.format(r.json() if r else r))
                break
            data = r.json()['data']
            if not data:
                break
            campaigns.extend(data)
            if len(data) < self.page_limit:
                break
        if self.campaign_filter:
            logging.info('Filtering campaigns on {}'.format(
                self.campaign_filter))
            campaigns = [x for x in campaigns
                         if self.campaign_filter in x['name']]
        return campaigns

    def request_report(self, campaign_id, sd, ed):
        url = '{}/{}/adgroups'.format(self.report_url, campaign_id)
        rows = []
        for x in range(1, 101):
            body = {
                'startTime': sd,
                'endTime': ed,
                'granularity': 'DAILY',
                'timeZone': 'UTC',
                'returnRowTotals': False,
                'returnGrandTotals': False,
                'returnRecordsWithNoMetrics': False,
                'selector': {
                    'orderBy': [{'field': 'adGroupId',
                                 'sortOrder': 'ASCENDING'}],
                    'pagination': {'offset': (x - 1) * self.page_limit,
                                   'limit': self.page_limit}}}
            r = self.make_request(url, method='post', json_body=body)
            if not r or 'data' not in r.json() or not r.json()['data']:
                logging.warning('Could not get report for campaign {}.  '
                                'Response: {}'.format(
                                    campaign_id, r.json() if r else r))
                break
            new_rows = r.json()['data']['reportingDataResponse']['row']
            rows.extend(new_rows)
            if len(new_rows) < self.page_limit:
                break
        return rows

    def rows_to_df(self, rows, campaign_name):
        data_rows = []
        for row in rows:
            metadata = row['metadata']
            for day in row['granularity']:
                data_row = {
                    'date': day['date'],
                    'campaign': campaign_name,
                    'ad_group': metadata['adGroupName'],
                    'impressions': day.get('impressions', 0),
                    'taps': day.get('taps', 0),
                    'totalInstalls': day.get('totalInstalls', 0),
                    'localSpend': day.get('localSpend', {}).get('amount', 0)}
                data_rows.append(data_row)
        df = pd.DataFrame(data_rows, columns=list(self.cols.keys()))
        return df

    def get_data(self, sd=None, ed=None, fields=None):
        self.set_headers()
        sd, ed = self.get_data_default_check(sd, ed)
        sd, ed = self.format_dates(sd, ed)
        logging.info('Getting data from {} to {}'.format(sd, ed))
        df = pd.DataFrame(columns=list(self.cols.keys()))
        campaigns = self.get_campaigns()
        for campaign in campaigns:
            rows = self.request_report(campaign['id'], sd, ed)
            cdf = self.rows_to_df(rows, campaign['name'])
            df = pd.concat([df, cdf], ignore_index=True)
        df = df.rename(columns=self.cols)
        logging.info('Data downloaded, returning df.')
        return df

    def get_org_ids(self):
        headers = {'Authorization': 'Bearer {}'.format(self.access_token)}
        r = requests.get(self.acls_url, headers=headers)
        if 'data' not in r.json() or not r.json()['data']:
            logging.warning('Could not get org ids.  Response: '
                            '{}'.format(r.json()))
            return []
        org_ids = [str(x['orgId']) for x in r.json()['data']]
        return org_ids

    def check_org_id(self, results, acc_col, success_msg, failure_msg):
        org_ids = self.get_org_ids()
        if str(self.org_id) in org_ids:
            row = [acc_col, ' '.join([success_msg, str(self.org_id)]), True]
            results.append(row)
        else:
            msg = ('Org ID NOT Found. '
                   'Double Check ID and Ensure Permissions were granted.')
            row = [acc_col, ' '.join([failure_msg, msg]), False]
            results.append(row)
        return results

    def check_campaign_names(self, results, camp_col, success_msg,
                             failure_msg):
        campaigns = self.get_campaigns()
        if not campaigns:
            msg = ('No campaigns under filter. '
                   'Double check filter and permissions.')
            row = [camp_col, ' '.join([failure_msg, msg]), False]
            results.append(row)
        else:
            msg = ' '.join(
                [success_msg, 'CAMPAIGNS INCLUDED IF DATA PAST START DATE:'])
            row = [camp_col, msg, True]
            results.append(row)
            for campaign in campaigns:
                row = [camp_col, campaign['name'], True]
                results.append(row)
        return results

    def test_connection(self, acc_col, camp_col, acc_pre):
        success_msg = 'SUCCESS:'
        failure_msg = 'FAILURE:'
        self.set_headers()
        results = self.check_org_id([], acc_col, success_msg, failure_msg)
        if False in results[0]:
            return pd.DataFrame(data=results, columns=vmc.r_cols)
        results = self.check_campaign_names(
            results, camp_col, success_msg, failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
