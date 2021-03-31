# -*- coding: utf-8 -*-

import io
import os
import sys
import json
import time
import base64
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl

config_path = utl.config_path


class QtApi(object):
    
    base_url = 'https://developers.quantcast.com/api/v1/accounts/'
    token_url = 'https://www.quantcast.com/oauth2/token'


    def __init__(self):
        self.config = None
        self.config_file = None
        self.username = None
        self.password = None
        self.advertiser = None
        self.campaign_filter = None
        self.config_list = None
        self.header = None
        self.act_id = None #added
        self.access_token = None #added
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
#changed here
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
                            self.advertiser,self.act_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Quantcast config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)
#changed here
    def set_header(self):
        token = self.get_token()
        self.access_token = token['access_token']
        self.header = {"Authorization": "Bearer " + self.access_token}
 #changed here  
    def get_token(self):
        token_url = 'https://www.quantcast.com/oauth2/token'
        token_header = {"Content-Type": "application/x-www-form-urlencoded"}
        parms = (('username', self.username), ('password', self.password),('grant_type', 'password'))
        r = requests.post(token_url , data=parms, headers=token_header)
        token = r.json()
        return token
#changed here
    def make_request(self, method, headers = None, json_body=None, data=None,
                     params=None, attempt=1):
        auth_url = 'https://developers.quantcast.com/api/v1/accounts/' + self.act_id + '/hello'
        headers_self = self.header
        if not headers:
            headers = self.header
        if not json_body:
            json_body = {}
        if not data:
            data = {}
        if not params:
            params = {}
        if method == 'POST':
            request_method = requests.post
        else:
            request_method = requests.get
        try:
            r = request_method(auth_url, headers=headers_self)
        except requests.exceptions.ConnectionError as e:
            attempt += 1
            if attempt > 100:
                logging.warning('Could not connection with error: {}'.format(e))
                r = None
            else:
                logging.warning('Connection error, pausing for 60s '
                                'and retrying: {}'.format(e))
                time.sleep(60)
                r = self.make_request(auth_url, method, headers,attempt)
        return r

    @staticmethod
    def format_dates(sd, ed):
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        return sd, ed

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.  '
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
        req_url = 'https://developers.quantcast.com/api/v1/accounts/' + self.act_id + '/report'
        self.df = pd.DataFrame()
        sd, ed = self.get_data_default_check(sd, ed)
        sd, ed = self.date_check(sd, ed)
        sd, ed = self.format_dates(sd, ed)
        params = {'from': sd, 'to': ed}
        #token = str(self.access_token)
        #testhead = {"Authorization": "Bearer " + token}
        r = requests.post(req_url,headers = testhead,json=params)
        tdf = pd.DataFrame(r.json()['data'])
        tdf.to_csv("outtst_db.csv")
        return tdf
    

