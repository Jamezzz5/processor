import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import reporting.utils as utl


class SimApi(object):
    config_path = utl.config_path
    url = 'https://api.similarweb.com'
    batch_url = '/v3/batch'
    rest_url = '/v4/website'
    validate_url = '/request-validate'
    website_url = '/traffic_and_engagement'
    demo_url = '/audience_interests'
    request_url = '/request-report'
    status_url = '/request-status/'
    retry_url = '/retry/'
    virtual_table_url = '/tables/describe'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.config_list = None
        self.api_key = None
        self.headers = None
        self.domains = None
        self.countries = None
        self.report_id = None
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix. '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Sim config file: {}'.format(config))
        self.config_file = os.path.join(self.config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found. Aborting.'.format(self.config_file))
            sys.exit(0)
        self.domains = self.config['domains']
        self.countries = self.config['countries']
        self.api_key = self.config['api_key']
        self.config_list = [self.config, self.api_key, self.domains,
                            self.countries]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Sim config file'
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        headers = {
            'accept': 'application/json',
            'content-type': 'application/json',
            'api-key': self.api_key
        }
        return headers

    def construct_payload(self, sd, ed):
        payload = {'metrics': ['all_traffic_visits', 'desktop_new_visitors',
                               'mobile_average_visit_duration'],
                   'filters': {
                       'domains': self.domains.split(','),
                       'countries': self.countries.split(','),
                       'include_subdomains': True
                   },
                   'granularity': 'monthly',
                   'start_date': sd.strftime("%Y-%m-%d"),
                   'end_date': ed.strftime("%Y-%m-%d"),
                   'response_format': 'csv',
                   'delivery_method': 'download_link'
                   }
        return payload

    # Uses Data Credits
    def make_request(self, sd, ed):
        headers = self.set_headers()
        url = self.url + self.batch_url + self.website_url + self.request_url
        payload = self.construct_payload(sd, ed)
        response = requests.request('POST', url, headers=headers, json=payload)
        report_id = response.json()['report_id']
        return report_id

    def check_report_status(self, report_id):
        headers = self.set_headers()
        url = self.url + self.batch_url + self.status_url + report_id
        r = requests.request('GET', url, headers=headers)
        report_url = r.json()['download_url']
        return report_url

    @staticmethod
    def get_df_from_response(report_url):
        df = pd.read_csv(report_url)
        return df

    def make_validate_request(self, sd, ed):
        headers = self.set_headers()
        url = self.url + self.batch_url + self.website_url + self.validate_url
        payload = self.construct_payload(sd, ed)
        r = requests.request('POST', url, headers=headers, json=payload)
        print(r.text)

    def discover_table(self, sd, ed):
        headers = self.set_headers()
        url = self.url + self.batch_url + self.virtual_table_url
        payload = self.construct_payload(sd, ed)
        r = requests.request('GET', url, headers=headers, json=payload)
        df = pd.DataFrame(r.json())
        return df
