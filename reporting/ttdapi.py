import requests
import logging
import json
import sys
import pandas as pd
from StringIO import StringIO

configfile = 'Config/ttdconfig.json'


url = 'https://api.thetradedesk.com/v3'
configpath = 'Config/'


class TtdApi(object):
    def __init__(self):
        self.df = pd.DataFrame()
        self.configfile = None
        self.config = None
        self.config_list = None
        self.login = None
        self.password = None
        self.ad_id = None
        self.ex_id = None
        self.auth_token = None
        self.headers = None

    def input_config(self, config):
        logging.info('Loading TTD config file: ' + str(config))
        self.configfile = configpath + config
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(self.configfile + ' not found.  Aborting.')
            sys.exit(0)
        self.login = self.config['LOGIN']
        self.password = self.config['PASS']
        self.ad_id = self.config['ADID']
        self.ex_id = self.config['EXID']
        self.config_list = [self.login, self.password]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warn(item + 'not in TTD config file.  Aborting.')
                sys.exit(0)

    def authenticate(self):
        auth_url = "{0}/authentication".format(url)
        userpass = {'Login': self.login, 'Password': self.password}
        self.headers = {'Content-Type': 'application/json'}
        r = requests.post(auth_url, headers=self.headers, json=userpass)
        if r.status_code != 200:
            logging.error('Failed to authenticate with error code: '
                          + str(r.status_code) + ' Error: ' + str(r.content))
            sys.exit(0)
        auth_token = json.loads(r.text)['Token']
        return auth_token

    def get_download_url(self):
        auth_token = self.authenticate()
        rep_url = '{0}/myreports/reportexecution/query/advertisers'.format(url)
        payload = {
            'AdvertiserIds': [self.ad_id],
            'PageStartIndex': 0,
            'PageSize': 10
        }
        self.headers = {'Content-Type': 'application/json',
                        'TTD-Auth': auth_token}
        r = requests.post(rep_url, headers=self.headers, json=payload)
        data = json.loads(r.content)
        dl_url = [x['ReportDeliveries'][0]['DownloadURL'] for x
                  in data['Result']
                  if x['ReportExecutionId'] == int(self.ex_id)]
        return dl_url[0]

    def get_data(self, sd=None, ed=None, fields=None):
        logging.info('Getting TTD data for execution id: ' + str(self.ex_id))
        dl_url = self.get_download_url()
        r = requests.get(dl_url, headers=self.headers)
        self.df = pd.read_csv(StringIO(r.content.decode('utf-8')))
        return self.df
