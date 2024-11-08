import os
import sys
import json
import logging
import requests
import pandas as pd
import reporting.utils as utl


class PixApi(object):
    config_path = utl.config_path
    url = 'https://api.pixalate.com/api/v2/mrt/domains/'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.config_list = None
        self.api_key = None
        self.headers = None
        self.domains = None
        self.countries = None
        self.r = None
        self.df = pd.DataFrame()

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix. '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Pix config file: {}'.format(config))
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
                logging.warning('{} not in Pix config file'
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        headers = {
            'accept': 'application/json',
            'content-type': 'application/json',
            'x-api-key': self.api_key
        }
        return headers

    def get_data(self, sd=None, ed=None, fields=None):
        headers = self.set_headers()
        domain_list = self.domains.split(',')
        rows = []
        for domain in domain_list:
            url = '{}{}'.format(self.url, domain)
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed for domain {domain}: {e}")
                continue
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode failed for domain {domain}: {e}")
                continue
            docs = data.get('docs', [])
            rows.extend([{
                'Domain': doc.get('adDomain'),
                'Region': doc.get('region'),
                'Device': doc.get('device'),
                'True Reach': doc.get('domainDetails', {}).get('trueReach'),
                'IVT': doc.get('riskOverview', {}).get('ivt'),
                'Viewability': doc.get('riskOverview', {}).get('viewability')
            } for doc in docs])
        self.df = pd.DataFrame(rows)
        self.check_empty_df()
        return self.df

    def check_empty_df(self):
        if (self.df.empty or self.df.iloc[0, 0] ==
                'No data returned'):
            logging.warning('No data in response, returning empty df.')
            self.df = pd.DataFrame()
