import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.vmcolumns as vmc
import reporting.utils as utl

config_path = utl.config_path


class CriApi(object):
    base_url = 'https://api.criteo.com'
    auth_url = '{}/oauth2/token'.format(base_url)
    version_url = '/2025-04/retail-media'
    default_config_file_name = 'criapi.json'
    line_item_str = 'Line'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.advertiser_id = None
        self.campaign_id = None
        self.ad_id_list = []
        self.config_list = None
        self.headers = None
        self.version = '2'
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Cri config file: {}'.format(config))
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
        self.advertiser_id = self.config['advertiser_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.advertiser_id]
        if 'campaign_id' in self.config:
            self.campaign_id = self.config['campaign_id']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Cri config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def refresh_token(self):
        data = {'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'client_credentials'}
        r = self.make_request(self.auth_url, method='POST', data=data)
        token = r.json()['access_token']
        return token

    def set_headers(self):
        token = self.refresh_token()
        self.headers = {'Authorization': 'Bearer {}'.format(token),
                        'Content-Type': 'application/json'}

    def make_request(self, url, method, headers=None, json_body=None, data=None,
                     params=None, attempt=1):
        if not json_body:
            json_body = {}
        if not headers:
            headers = {}
        if not data:
            data = {}
        if not params:
            params = {}
        if method == 'POST':
            request_method = requests.post
        else:
            request_method = requests.get
        try:
            r = request_method(url, headers=headers, json=json_body, data=data,
                               params=params)
        except requests.exceptions.ConnectionError as e:
            attempt += 1
            if attempt > 100:
                logging.warning('Could not connection with error: {}'.format(e))
                r = None
            else:
                logging.warning('Connection error, pausing for 60s '
                                'and retrying: {}'.format(e))
                time.sleep(60)
                r = self.make_request(url=url, method=method,
                                      headers=headers, json_body=json_body,
                                      attempt=attempt)
        return r

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    def get_data_default_check(self, sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() + dt.timedelta(days=1)
        if dt.datetime.today().date() == ed.date():
            ed += dt.timedelta(days=1)
        sd, ed = self.date_check(sd, ed)
        return sd, ed

    def get_data(self, sd=None, ed=None, fields=None):
        self.df = pd.DataFrame()
        sd, ed = self.get_data_default_check(sd, ed)
        self.df = self.request_and_get_data(sd, ed, fields)
        return self.df

    @staticmethod
    def parse_fields(fields):
        fields = fields.split(',')
        return fields

    def request_data(self, sd, ed, base_url, fields=None, campaign_id=''):
        msg = 'Getting campaign {} data form {} to {}'.format(
            campaign_id, sd, ed)
        logging.info(msg)
        self.set_headers()
        url = '{}/reports/campaigns'.format(base_url)
        params = {'startDate': sd,
                  'endDate': ed,
                  'timezone': 'America/New_York',
                  'id': campaign_id,
                  'format': 'json-compact'}
        if fields:
            if self.line_item_str in fields:
                fields = [campaign_id]
            else:
                fields = ' '.join(fields)
                fields = self.parse_fields(fields)
            url = '{}/reports/line-items'.format(base_url)
            params['ids'] = fields
        params = {'type': 'RetailMediaReportRequest', 'attributes': params}
        params = {'data': params}
        r = self.make_request(url, method='POST', headers=self.headers,
                              json_body=params)
        return r

    def check_if_advertiser_id(self, base_url, fields):
        """
        Checks if the supplied advertiser_id is advertiser or campaign

        :returns: All campaign ids for the advertiser
        """
        campaign_ids = [self.advertiser_id]
        self.set_headers()
        object_type = 'campaigns'
        if fields:
            object_type = 'line-items'
        url = '{}/accounts/{}/{}'.format(
            base_url, self.advertiser_id, object_type)
        r = self.make_request(url, method='GET', headers=self.headers)
        if 'data' in r.json():
            campaign_ids = [x['id'] for x in r.json()['data']]
        else:
            campaign_ids = [x.strip() for x in campaign_ids[0].split(',')]
        logging.info('Found {} {}'.format(
            len(campaign_ids), object_type))
        return campaign_ids

    def request_and_get_data(self, sd, ed, fields=None):
        df = pd.DataFrame()
        base_url = '{}{}'.format(self.base_url, self.version_url)
        campaign_ids = self.check_if_advertiser_id(base_url, fields)
        for campaign_id in campaign_ids:
            r = self.request_data(sd, ed, base_url, fields, campaign_id)
            if 'data' not in r.json():
                logging.warning('data was not in response returning blank df: '
                                '{}'.format(r.json()))
                tdf = pd.DataFrame()
            else:
                report_id = r.json()['data']['id']
                tdf = self.check_and_get_report(report_id, base_url)
            df = pd.concat([df, tdf], ignore_index=True)
        return df

    def check_and_get_report(self, report_id, base_url, max_attempts=60,
                             initial_delay=5, backoff_factor=1.1):
        """
        Checks status of requested report and moves on to download when status
        is 'success'
        uses exponential backoff to limit request amount.

        :param report_id: string of requested report ID
        :param base_url: string of base url used in for url endpoint
        :param max_attempts: integer for maximum attempts allowed to poll status
        :param initial_delay: integer for wait time between requests,
        used for exponential backoff
        :param backoff_factor: float used for exponential backoff, to minimize
        requests
        :returns: dataframe of downloaded report, or empty dataframe on failure
        """
        url = '{}/reports/{}/status'.format(base_url, report_id)
        error_count = 0
        df = pd.DataFrame()
        delay = initial_delay
        for x in range(1, max_attempts + 1):
            logging.info('Checking report attempt {}'.format(x))
            r = self.make_request(url, method='GET', headers=self.headers)
            result = r.json()
            if ('data' in result and 'attributes' in result['data'] and
                    'status' in result['data']['attributes']):
                status = result['data']['attributes']['status']
                if status == 'success':
                    df = self.download_report(url)
                    break
                elif status == 'failure' or status == 'expired':
                    logging.warning('Report failed returning blank df')
                    break
            else:
                logging.warning('Unexpected response: {}'.format(result))
                if 'errors' in result:
                    error = result['errors'][0]
                    if error.get('code') == 'authorization-token-expired':
                        logging.info('Token expired, refreshing...')
                        self.set_headers()
                error_count += 1
                if error_count > 10:
                    logging.warning('Too many errors returning blank df.')
                    break
            time.sleep(delay)
            delay = min(delay * backoff_factor, 60)
        return df

    def download_report(self, url):
        logging.info('Report available downloading.')
        self.set_headers()
        url = url.replace('status', 'output')
        for i in range(10):
            r = self.make_request(url, method='GET', headers=self.headers)
            if 'data' in r.json():
                df = pd.DataFrame(r.json()['data'],
                                  columns=r.json()['columns'])
                logging.info('Report downloaded returning df.')
                return df
            else:
                logging.info("'data' not in download response. Retrying. "
                             "Attempt {}".format(i+1))
                time.sleep(30)
        logging.warning('Unexpected response. Returning blank df: {}'
                        .format(r.json()))
        return pd.DataFrame()

    def check_permissions(self, results, acc_col, success_msg, failure_msg):
        self.set_headers()
        sd = dt.datetime.today() - dt.timedelta(days=30)
        ed = dt.datetime.today() - dt.timedelta(days=1)
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        base_url = '{}{}'.format(self.base_url, self.version_url)
        campaign_ids = self.check_if_advertiser_id(base_url, fields=None)
        r = self.request_data(sd, ed, base_url, fields=None,
                              campaign_id=campaign_ids[0])
        if (r.status_code == 200 and
                'data' in r.json()):
            row = [acc_col, ' '.join([success_msg, str(self.advertiser_id)]),
                   True]
            results.append(row)
        else:
            msg = ('Permissions NOT Granted. '
                   'Double Check Permissions were granted, '
                   'and verify Campaign ID is correct'
                   '\n Error Msg:')
            row = [acc_col, ' '.join([failure_msg, msg, r.reason]),
                   False]
            results.append(row)
        return results, r

    def test_connection(self, acc_col, camp_col=None, acc_pre=None):
        success_msg = 'SUCCESS:'
        failure_msg = 'FAILURE:'
        results, r = self.check_permissions(
            [], acc_col, success_msg, failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
