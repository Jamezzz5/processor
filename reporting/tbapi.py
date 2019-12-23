import os
import sys
import time
import json
import logging
import requests
import reporting.utils as utl

config_path = utl.config_path


class TabApi(object):
    base_url = 'https://us-east-1.online.tableau.com/api/'
    version = '3.3'
    base_url = '{}{}/'.format(base_url, version)

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

    def set_headers(self):
        self.headers = {'Content-Type': 'application/json',
                        'Accept': 'application/json'}
        data = {"credentials": {
                "name": self.username,
                "password": self.password,
                "site": {"contentUrl": self.site}}}
        url = self.create_url('auth/signin')
        r = self.make_request(url, 'post', resp_key='credentials',
                              json_data=data)
        session_id = r.json()['credentials']['token']
        self.headers['X-Tableau-Auth'] = session_id
        self.site_id = r.json()['credentials']['site']['id']

    def create_url(self, addition_url, site_url=False):
        url = self.base_url
        if site_url:
            url = '{}sites/{}/'.format(url, self.site_id)
        url = '{}{}'.format(url, addition_url)
        return url

    def make_request(self, url, request_type='get', resp_key=None,
                     json_data=None, attempt=1):
        if request_type == 'get':
            r = requests.get(url, headers=self.headers)
        else:
            r = requests.post(url, json=json_data, headers=self.headers)
        if resp_key and resp_key not in r.json():
            logging.warning('Attempt {}, {} not in response: '
                            '{}'.format(attempt, resp_key, r.json()))
            time.sleep(10)
            attempt += 1
            if attempt < 10:
                r = self.make_request(url, request_type, resp_key,
                                      json_data=json_data, attempt=attempt)
            else:
                logging.warning('Maximum attempts exceeded - stopping.')
                sys.exit(0)
        return r

    def find_datasource(self):
        logging.info('Finding datasource: {}.'.format(self.datasource))
        ds_url = 'datasources?pageSize=1000'
        ds_url = self.create_url(ds_url, site_url=True)
        r = self.make_request(ds_url, resp_key='datasources')
        ds_id = [x['id'] for x in r.json()['datasources']['datasource']
                 if x['name'] == self.datasource][0]
        return ds_id

    def find_extract_refreshes(self, ds_id):
        logging.info('Finding extract refresh id: {}.'.format(ds_id))
        er_url = 'tasks/extractRefreshes'
        er_url = self.create_url(er_url, site_url=True)
        r = self.make_request(er_url, resp_key='tasks')
        er_id = [x['extractRefresh']['id'] for x in r.json()['tasks']['task']
                 if 'datasource' in x['extractRefresh'] and
                 x['extractRefresh']['datasource']['id'] == ds_id][0]
        return er_id

    def send_refresh_request(self, er_id):
        logging.info('Sending refresh request for id: {}'.format(er_id))
        rr_url = 'tasks/extractRefreshes/{}/runNow'.format(er_id)
        rr_url = self.create_url(rr_url, site_url=True)
        r = self.make_request(rr_url, 'post', resp_key='job', json_data={})
        job_id = r.json()['job']['id']
        return job_id

    def check_job(self, job_id):
        job_url = 'jobs/{}'.format(job_id)
        job_url = self.create_url(job_url, site_url=True)
        r = self.make_request(job_url, resp_key='job')
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
        if self.datasource:
            self.set_headers()
            ds_id = self.find_datasource()
            er_id = self.find_extract_refreshes(ds_id)
            job_id = self.send_refresh_request(er_id)
            self.check_job_until_complete(job_id)
        else:
            logging.warning('Tableau api not configured, it was not refreshed.')
