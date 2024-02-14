import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
import reporting.vmcolumns as vmc


class SamApi(object):
    config_path = utl.config_path
    base_url = 'https://reporting.trader.adgear.com/v1/reports'
    default_dimensions = [
        "campaign_id", "advertiser_id", "advertiser_name",  "campaign_name",
        "creative_id", "creative_group_name", "creative_name", "day"]
    default_metrics = [
        "impressions", "clicks", "buyer_spend", "media_spend", "video_start",
        "video_first_quartile", "video_midpoint", "video_third_quartile",
        "video_complete", "conv_caused", "conv_revenue", "conv_postclick"]

    def __init__(self):
        self.config = None
        self.config_file = None
        self.author_id = None
        self.access_token = None
        self.author_name = None
        self.campaign_id = None
        self.config_list = None
        self.client = None
        self.df = pd.DataFrame()
        self.r = None
        self.v = 1

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Sam config file: {}'.format(config))
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
        self.author_id = self.config['author_id']
        self.author_name = self.config['author_name']
        self.access_token = self.config['access_token']
        self.campaign_id = self.config['campaign_id']
        self.config_list = [self.config, self.author_name, self.access_token,
                            self.campaign_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Sam config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        report_id, error = self.create_report(sd, ed)
        if error:
            logging.warning('No report ID, returning blank df: {}', error)
            return pd.DataFrame()
        self.df = self.get_raw_data(report_id)
        self.check_empty_df()
        return self.df

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today()
        if ed is None:
            ed = dt.datetime.today()
        return sd, ed

    def check_empty_df(self):
        if (self.df.empty or self.df.iloc[0, 0] ==
                'No data returned by the reporting service.'):
            logging.warning('No data in response, returning empty df.')
            self.df = pd.DataFrame()

    def create_report(self, sd, ed, dimensions=None):
        """
        Generates report for download.

        :returns: Report ID for retrieval if successful, any error messages
        """
        logging.info('Creating report.')
        query = self.create_report_query(sd, ed, dimensions)
        header = self.create_header()
        body = {
            "title": "Report",
            "author_id": self.author_id,
            "author_name": self.author_name,
            'query': query,
            "output_formats": [
                {"format": "csv", "compression": "None"}
            ],
            "notification": {
                "emails": [
                    self.author_name
                ]
            }
        }
        self.r = self.make_request('post', self.base_url, body, header)
        if self.r.status_code != 201:
            logging.warning('Failed Request, Code: {}'.format(
                self.r.status_code))
            return None, self.r.status_code
        response = self.r.json()
        if 'id' in response:
            return response['id'], None
        else:
            logging.warning('ID not in response: {}'.format(response))
            return None, response

    def make_request(self, method, url, body=None, header=None):
        try:
            self.r = self.raw_request(method, url, body=body, header=header)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(method, url, body=body, header=header)
        return self.r

    def raw_request(self, method, url, body=None, header=None):
        if method == 'post':
            self.r = requests.post(url, json=body, headers=header)
        elif method == 'get':
            self.r = requests.get(url, headers=header)
        return self.r

    def get_raw_data(self, report_id):
        header = self.create_header()
        response = None
        url = '{}/{}'.format(self.base_url, report_id)
        for x in range(1, 101):
            self.r = self.make_request('get', url, header=header)
            try:
                response = self.r.json()
            except json.decoder.JSONDecodeError as e:
                logging.warning('No JSON in response retrying: {}'.format(e))
                time.sleep(60)
            if response.get('urls') and response['urls']:
                break
        report_url = (response['urls'])
        if report_url:
            logging.info('Found report url, downloading.')
            self.df = utl.import_read_csv(report_url[0], file_check=False,
                                          error_bad='warn')
        else:
            logging.warning('Report does not exist.')
            self.df = pd.DataFrame()
        return self.df

    def create_report_query(self, sd, ed, dimensions=None):
        query = {"type": "trader_delivery_all", "start_date": str(sd.date()),
                 "end_date": str(ed.date()), "time_zone": "America/New_York",
                 "metrics": self.default_metrics,
                 'dimensions': (dimensions if dimensions else
                                self.default_dimensions)}
        if self.campaign_id and not dimensions:
            campaign_filters = "campaign_id = " + self.campaign_id
            query['filter'] = campaign_filters
        return query

    def create_header(self):
        header = {'Authorization': 'Bearer connor.tullis:' + self.access_token,
                  'Content-Type': 'application/json'}
        return header

    def test_connection(self, acc_col, camp_col, acc_pre):
        results = []
        success_msg = 'SUCCESS -- ID:'
        failure_msg = 'FAILURE:'
        auth_msg = 'Authentication Failed.'
        missing_campaign = ('Campaign ID Not Found. '
                            'Check for typo and ensure access granted.')
        sd = dt.datetime.today() - dt.timedelta(days=365)
        ed = dt.datetime.today()
        report_id, error = self.create_report(
            sd, ed, dimensions=[self.default_dimensions[0]])
        if error:
            row = [acc_col, ' '.join([failure_msg, auth_msg]), False]
            results.append(row)
            return pd.DataFrame(data=results, columns=vmc.r_cols)
        df = self.get_raw_data(report_id)
        if int(self.campaign_id) in df['Campaign ID'].to_list():
            row = [acc_col, ' '.join([success_msg, str(self.campaign_id)]),
                   True]
            results.append(row)
        else:
            row = [acc_col, ' '.join([failure_msg, missing_campaign]),
                   False]
            results.append(row)
            return pd.DataFrame(data=results, columns=vmc.r_cols)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
