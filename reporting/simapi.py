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


class SimApi(object):
    config_path = utl.config_path
    url = 'https://api.similarweb.com'
    batch_url = '/v3/batch'
    rest_url = '/v4/website'
    validate_url = '/request-validate'
    website_url = '/traffic_and_engagement'
    request_url = '/request-report'
    status_url = '/request-status/'
    retry_url = '/retry/'

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
        self.df = pd.DataFrame()

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
        payload = {'metrics': ['all_traffic_visits', 'desktop_visits',
                               'mobile_visits', 'all_traffic_bounce_rate',
                               'desktop_bounce_rate', 'mobile_bounce_rate',
                               'all_traffic_pages_per_visit',
                               'desktop_pages_per_visit',
                               'mobile_pages_per_visit',
                               'all_traffic_average_visit_duration',
                               'desktop_average_visit_duration',
                               'mobile_average_visit_duration',
                               'desktop_unique_visitors',
                               'mobile_unique_visitors', 'deduplicated_audience'
                               ],
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
    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        report_id = self.make_request(sd, ed)
        self.df = self.check_report_status(report_id)
        self.check_empty_df()
        return self.df

    # Uses Data Credits
    def make_request(self, sd, ed):
        headers = self.set_headers()
        url = '{}{}{}{}'.format(self.url, self.batch_url, self.website_url,
                                self.request_url)
        report_id = self.config.get('report_id')
        if not report_id:
            payload = self.construct_payload(sd, ed)
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                report_id = response.json()['report_id']
                self.config['report_id'] = report_id
                return report_id
            elif response.status_code == 400:
                logging.warning('Metrics are not available in table')
                return None
            else:
                logging.warning(f'Unexpected status code: '
                                f'{response.status_code}')
        else:
            return report_id

    def check_report_status(self, report_id):
        headers = self.set_headers()
        url = '{}{}{}{}'.format(self.url, self.batch_url, self.status_url,
                                report_id)
        for x in range(10):
            r = requests.get(url, headers=headers)
            if r.status_code != 200:
                return None
            status = r.json()['status']
            if status == 'completed':
                report_url = r.json()['download_url']
                logging.info('Found report url, downloading.')
                self.df = pd.read_csv(report_url)
                return self.df
            if status in ['processing', 'pending']:
                time.sleep(30)
        logging.warning('Job still in progress, please try again in a moment')
        return self.df

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=35)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=30)
        return sd, ed

    def check_empty_df(self):
        if (self.df.empty or self.df.iloc[0, 0] ==
                'No data returned'):
            logging.warning('No data in response, returning empty df.')
            self.df = pd.DataFrame()

    def make_validate_request(self, sd=None, ed=None):
        headers = self.set_headers()
        url = '{}{}{}{}'.format(self.url, self.batch_url, self.website_url,
                                self.validate_url)
        sd, ed = self.get_data_default_check(sd, ed)
        payload = self.construct_payload(sd, ed)
        r = requests.post(url, headers=headers, json=payload)
        results = json.loads(r.content)
        return results

    def check_estimated_credits(self, acc_col, success_msg, failure_msg):
        r = self.make_validate_request()
        results = []
        if 'estimated_credits' in r:
            results_text = r['estimated_credits']
            if results_text <= 3000:
                row = [acc_col, ' '.join([success_msg, json.dumps(r)]),
                       True]
                results.append(row)
        else:
            msg = ('This request is using over 3000 credits,'
                   ' Be aware that we only have 25000 per month.'
                   ' Double check your settings and'
                   ' if everything is correct continue'
                   '\n Warning:')
            row = [acc_col, ' '.join([failure_msg, msg]), False]
            results.append(row)
        return results

    def test_connection(self, acc_col=None, camp_col=None, acc_pre=None):
        success_msg = 'SUCCESS:'
        failure_msg = 'WARNING:'
        self.set_headers()
        results = self.check_estimated_credits(acc_col, success_msg,
                                               failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
