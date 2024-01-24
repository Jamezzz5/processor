import os
import sys
import json
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl

config_path = utl.config_path


class QtApi(object):

    base_url = 'https://developers.quantcast.com/api/v1/accounts/'
    token_url = 'https://auth.quantcast.com/oauth2/default/v1/token'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.username = None
        self.password = None
        self.advertiser = None
        self.campaign_filter = None
        self.config_list = None
        self.header = None
        self.act_id = None
        self.access_token = None
        self.response_tokens = []
        self.aid_dict = {}
        self.cid_dict = {}
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Quantcast config file: {}'.format(config))
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
        self.username = self.config['username']
        self.password = self.config['password']
        self.advertiser = self.config['advertiser']
        self.act_id = self.config['act_id']
        self.config_list = [self.config, self.username, self.password,
                            self.advertiser, self.act_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Quantcast config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_header(self):
        token = self.get_token()
        self.access_token = token['access_token']
        self.header = {"Authorization": "Bearer " + self.access_token}
        logging.info('Header set with access token')

    def get_token(self):
        logging.info('Retrieving access token')
        token_header = {"Content-Type": "application/x-www-form-urlencoded"}
        params = (('client_id', self.username),
                  ('client_secret', self.password),
                  ('scope', 'api_access read_reports'),
                  ('grant_type', 'client_credentials'))
        r = requests.post(self.token_url, data=params, headers=token_header)
        token = r.json()
        logging.info('Access token retrieved')
        return token

    @staticmethod
    def format_dates(sd, ed):
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.'
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
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
        self.set_header()
        req_url = self.base_url + self.act_id + '/report'
        sd, ed = self.get_data_default_check(sd, ed)
        sd, ed = self.format_dates(sd, ed)
        logging.info('Getting data from {} to {}'.format(sd, ed))
        params = {'from': sd, 'to': ed}
        for i in range(1,10):
            r = requests.post(req_url, headers=self.header, json=params)
            if 'data' in r.json():
                tdf = pd.DataFrame(r.json()['data'])
                logging.info('Data downloaded, returning df')
                return tdf
            else:
                logging.warning('Could not retrieve data, retrying:'
                                '{}'.format(r.json()))
        logging.warning('Failed to retrieve data'
                        '{}'.format(r.json()))
        return pd.DataFrame()
