import os
import sys
import time
import json
import logging
import requests
import reporting.utils as utl

config_path = utl.config_path

signin_url = 'https://us-east-1.online.tableau.com/api/3.3/auth/signin'


class TabApi(object):
    def __init__(self, config_file='tabconfig.json'):
        self.config_file = config_file
        self.config = None
        self.username = None
        self.password = None
        self.site = None
        self.site_id = None
        self.datasource = None
        self.config_list = None
        self.headers = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Tableau config file: {}'.format(config))
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
        self.site = self.config['site']
        self.datasource = self.config['datasource']
        self.config_list = [self.config, self.username, self.password,
                            self.site, self.datasource]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Tableau config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        self.headers = {'Content-Type': 'application/json',
                        'Accept': 'application/json'}
        data = {"credentials": {
                "name": self.username,
                "password": self.password,
                "site": {"contentUrl": self.site}}}
        r = requests.post(signin_url, json=data, headers=self.headers)
        session_id = r.json()['credentials']['token']
        self.headers['X-Tableau-Auth'] = session_id
        self.site_id = r.json()['credentials']['site']['id']

    def find_datasource(self):
        ds_url = ('https://online.tableau.com/api/3.3/sites/'
                  '{}/datasources?pageSize=1000'.format(self.site_id))
        r = requests.get(ds_url, headers=self.headers)
        ds_id = [x['id'] for x in r.json()['datasources']['datasource']
                 if x['name'] == self.datasource][0]
        return ds_id

    def find_extract_refreshes(self, ds_id):
        er_url = ('https://online.tableau.com/api/3.3/sites/{}/tasks/'
                  'extractRefreshes'.format(self.site_id))
        r = requests.get(er_url, headers=self.headers)
        er_id = [x['extractRefresh']['id'] for x in r.json()['tasks']['task']
                 if 'datasource' in x['extractRefresh'] and
                 x['extractRefresh']['datasource']['id'] == ds_id][0]
        return er_id

    def send_refresh_request(self, er_id):
        rr_url = ('https://us-east-1.online.tableau.com/api/3.3/sites/{}/tasks/'
                  'extractRefreshes/{}/runNow'.format(self.site_id, er_id))
        r = requests.post(rr_url, json={}, headers=self.headers)
        job_id = r.json()['job']['id']
        return job_id

    def check_job(self, job_id):
        job_url = ('https://online.tableau.com/api/3.3/sites/'
                   '{}/jobs/{}'.format(self.site_id, job_id))
        r = requests.get(job_url, headers=self.headers)
        logging.info('Job request response as follows {}'.format(r.json()))
        if 'completedAt' in r.json()['job']:
            logging.info('Extract refresh completed at {}'
                         ''.format(r.json()['job']['completedAt']))
            return True
        else:
            return False

    def check_job_until_complete(self, job_id):
        for x in range(1, 100):
            if self.check_job(job_id):
                break
            else:
                logging.info('Extract refresh not complete.  '
                             'Attempt #{}'.format(x))
                time.sleep(120)

    def refresh_extract(self):
        self.set_headers()
        ds_id = self.find_datasource()
        er_id = self.find_extract_refreshes(ds_id)
        job_id = self.send_refresh_request(er_id)
        self.check_job_until_complete(job_id)
