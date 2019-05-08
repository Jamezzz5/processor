import io
import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl

config_path = utl.config_path

login_url = 'https://adapi.sizmek.com/sas/login/login/'
report_url = 'https://adapi.sizmek.com/analytics/reports/saveAndExecute'
base_report_get_url = 'https://adapi.sizmek.com/analytics/reports/executions/'

def_dimensions = ["Site Name", "Placement Name", "Ad Name", "Placement ID",
                  "Placement Dimension", "Campaign Name", "Ad ID"]

def_metrics = ["Served Impressions", "Total Clicks", "Video Played 25%",
               "Video Played 50%", "Video Played 75%", "Video Started",
               "Video Fully Played", "Total Conversions",
               "Post Click Conversions", "Post Impression Conversions",
               "Total Media Cost"]

p2c_fields = ['Conversion Date', 'Conversion ID', 'Event Date',
              'Event Type Name', 'Conversion Time Lag', 'Placement Name',
              'Campaign Name', 'Site Name', 'Winner Placement Name',
              'Query String', 'Conversion Activity Name',
              'Winner Event Type Name']


class SzkApi(object):
    def __init__(self):
        self.config = None
        self.config_file = None
        self.username = None
        self.password = None
        self.api_key = None
        self.campaign_ids = None
        self.config_list = None
        self.email = None
        self.headers = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Sizmek config file: {}'.format(config))
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
        self.api_key = self.config['api_key']
        self.campaign_ids = self.config['campaign_ids']
        self.email = self.config['email']
        self.config_list = [self.config, self.username, self.password,
                            self.api_key, self.campaign_ids, self.email]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Sizmek config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        self.headers = {'api-key': self.api_key}
        data = {'username': self.username, 'password': self.password}
        r = requests.post(login_url, data=json.dumps(data),
                          headers=self.headers)
        session_id = r.json()['result']['sessionId']
        self.headers['Authorization'] = session_id

    @staticmethod
    def parse_fields(fields):
        field_dict = {'type': 'AnalyticsReport',
                      'timeBreakdown': 'Day',
                      'attributeIDs': def_dimensions,
                      'metricIDs': def_metrics}
        if fields:
            for field in fields:
                if field == 'Total':
                    field_dict['timeBreakdown'] = 'Total'
                if field == 'Placement':
                    field_dict['attributeIDs'] = ['Placement Name',
                                                  'Placement ID']
                if field == 'P2C':
                    field_dict['type'] = 'ReportP2C'
                if field[:6] == 'Filter':
                    field = field.split('::')
                    field_dict['filters'] = {str(field[1]): [int(x) for x in
                                             field[2].split(',')]}
        return field_dict

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
        sd = '{}{}'.format(sd.isoformat(), '.000Z')
        ed = '{}{}'.format(ed.isoformat(), '.000Z')
        return sd, ed

    def get_data_default_check(self, sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() + dt.timedelta(days=1)
        if dt.datetime.today().date() == ed.date():
            ed += dt.timedelta(days=1)
        sd, ed = self.date_check(sd, ed)
        fields = self.parse_fields(fields)
        return sd, ed, fields

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        report_get_url = self.request_report(sd, ed, fields)
        report_dl_url = self.get_report_dl_url(report_get_url)
        self.df = self.download_report_to_df(report_dl_url)
        return self.df

    def request_report(self, sd, ed, fields):
        logging.info('Requesting report for {} to {}.'.format(sd, ed))
        self.set_headers()
        report = self.create_report_body(sd, ed, fields)
        r = requests.post(report_url, headers=self.headers, json=report)
        if 'result' in r.json() and r.json()['result']:
            execution_id = r.json()['result']['executionID']
            report_get_url = '{}{}'.format(base_report_get_url, execution_id)
        else:
            logging.warning('Error in response as follows: {}'.format(r.json()))
            sys.exit(0)
        return report_get_url

    def get_report_dl_url(self, url):
        report_dl_url = None
        for attempt in range(200):
            time.sleep(120)
            r = requests.get(url, headers=self.headers)
            logging.info('Checking report.  Attempt: {} \n'
                         'Response: {}'.format(attempt + 1, r.json()))
            if r.json()['result']['executionStatus'] == 'FINISHED':
                logging.info('Report has been generated.')
                report_dl_url = r.json()['result']['files'][0]['url']
                break
            elif 'fault' in r.json():
                logging.warning('Fault in response: {}'.format(r.json()))
                self.set_headers()
        return report_dl_url

    def download_report_to_df(self, url):
        if url:
            r = requests.get(url)
            raw_file = io.StringIO(r.text)
            self.df = pd.read_csv(raw_file)
        else:
            logging.warning('No report download url, returning empty df.')
            self.df = pd.DataFrame()
        return self.df

    def create_report_body(self, sd, ed, fields):
        report = {"entities": [{
                  "type": fields['type'],
                  "reportName": "test",
                  "reportScope": {
                    "entitiesHierarchy": {
                      "entitiesHierarchyLevelType": "CAMPAIGN",
                      "accounts": [
                        454
                      ],
                      "advertisers": [],
                      "campaigns": [int(x) for x in
                                    self.campaign_ids.split(',')],
                      "sites": []
                    },
                    "attributionModelID": -1,
                    "impressionCookieWindow": 0,
                    "clickCookieWindow": 0,
                    "filters": {},
                    "currencyOptions": {
                      "type": "Custom",
                      "defaultCurrency": 1,
                      "targetCurrency": 1,
                      "currencyExchangeDate": "2019-03-14"
                    },
                    "timeRange": {
                      "timeZone": "US/Pacific",
                      "type": "Custom",
                      "dataStartTimestamp": "{}".format(sd),
                      "dataEndTimestamp": "{}".format(ed)
                    },
                  },
                  "reportStructure": {
                    "attributeIDs": fields['attributeIDs'],
                    "metricIDs": fields['metricIDs'],
                    "attributeIDsOnColumns": [
                      "Conversion Tag Name"
                    ],
                    "timeBreakdown": fields['timeBreakdown']
                  },
                  "reportExecution": {
                    "type": "Ad_Hoc"
                  },
                  "reportDeliveryMethods": [
                    {
                      "type": "URL",
                      "exportFileType": "CSV",
                      "compressionType": "NONE",
                      "emailRecipients": ['{}'.format(self.email)],
                      "exportFileNamePrefix": "test"
                    }
                  ],
                  "reportAuthorization": {
                    "type": "mm3",
                    "userID": 1073752812
                  }, }]}
        if fields['type'] == 'ReportP2C':
            report['entities'][0]["p2cDelimiter"] = ","
            report['entities'][0]["maxConversionPathLength"] = 5
            report['entities'][0]["withHeader"] = True
            report['entities'][0]['reportStructure'] = {
                "p2cFields": p2c_fields
            }
        if 'filters' in fields:
            report['entities'][0]['reportScope']['filters'] = fields['filters']
        return report
