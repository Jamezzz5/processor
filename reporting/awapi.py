import os
import sys
import yaml
import time
import logging
import requests
import numpy as np
import pandas as pd
import datetime as dt
import reporting.utils as utl
from io import BytesIO
from googleads import adwords

config_path = utl.config_path

VIEWS = 'Views'
VIEWS25 = 'Video played to 25%'
VIEWS50 = 'Video played to 50%'
VIEWS75 = 'Video played to 75%'
VIEWS100 = 'Video played to 100%'
VIEW_METRICS = [VIEWS25, VIEWS50, VIEWS75, VIEWS100]

def_fields = ['Date', 'AccountDescriptiveName', 'CampaignName', 'AdGroupName',
              'ImageCreativeName', 'Headline', 'HeadlinePart1', 'DisplayUrl',
              'HeadlinePart2', 'Description', 'Description1', 'Description2',
              'Impressions', 'Clicks', 'Cost', 'VideoViews',
              'VideoQuartile25Rate', 'VideoQuartile50Rate',
              'VideoQuartile75Rate', 'VideoQuartile100Rate']

conv_fields = ['Date', 'AccountDescriptiveName', 'CampaignName', 'AdGroupName',
               'ImageCreativeName', 'Headline', 'HeadlinePart1', 'DisplayUrl',
               'HeadlinePart2', 'Description', 'Description1', 'Description2',
               'ConversionTypeName', 'Conversions', 'ViewThroughConversions',
               'AllConversions']

uac_fields = ['Date', 'AccountDescriptiveName', 'CampaignName',
              'Impressions', 'Clicks', 'Cost', 'VideoViews',
              'VideoQuartile25Rate', 'VideoQuartile50Rate',
              'VideoQuartile75Rate', 'VideoQuartile100Rate', 'Conversions']


class AwApi(object):
    def __init__(self):
        self.df = pd.DataFrame()
        self.config = None
        self.configfile = None
        self.client_id = None
        self.client_secret = None
        self.developer_token = None
        self.refresh_token = None
        self.client_customer_id = None
        self.campaign_filter = None
        self.config_list = []
        self.adwords_client = None
        self.v = 'v201809'
        self.report_type = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix. '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Adwords config file: {}'.format(config))
        self.configfile = os.path.join(config_path, config)
        self.load_config()
        self.check_config()
        self.adwords_client = (adwords.AdWordsClient.
                               LoadFromStorage(self.configfile))

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = yaml.safe_load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.configfile))
            sys.exit(0)
        self.config = self.config['adwords']
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.developer_token = self.config['developer_token']
        self.refresh_token = self.config['refresh_token']
        self.client_customer_id = self.config['client_customer_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.developer_token, self.refresh_token,
                            self.client_customer_id]
        if 'campaign_filter' in self.config:
            self.campaign_filter = self.config['campaign_filter']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in AW config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    @staticmethod
    def video_calc(df):
        for column in VIEW_METRICS:
            df[column] = df[column].str.strip('%').astype(np.float)
            df[column] = (df[column] / 100) * df[VIEWS]
        return df

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        return sd, ed

    def parse_fields(self, fields):
        api_fields = def_fields
        self.report_type = 'AD_PERFORMANCE_REPORT'
        if fields is not None:
            if 'Conversions' in fields:
                api_fields = conv_fields
            if 'UAC' in fields:
                api_fields = uac_fields
                self.report_type = 'CAMPAIGN_PERFORMANCE_REPORT'
            for field in fields:
                if field == 'Device':
                    api_fields += ['Device']
        return api_fields

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        sd = sd.date()
        ed = ed.date()
        fields = self.parse_fields(fields)
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date'
                            'was set to end date.')
            sd = ed
        logging.info('Getting Adwords data from {} until {}'.format(sd, ed))
        report_downloader = self.get_downloader()
        report = {
            'reportName': 'Adwords_Report',
            'dateRangeType': 'CUSTOM_DATE',
            'reportType': self.report_type,
            'downloadFormat': 'CSV',
            }
        selector = {'fields': fields,
                    'dateRange': {'min': sd, 'max': ed}}
        if self.campaign_filter:
            selector['predicates'] = {'field': 'CampaignName',
                                      'operator': 'CONTAINS',
                                      'values': self.campaign_filter}
        if self.report_type == 'UAC':
            selector['predicates'] = {'field': 'advertisingChannelSubType',
                                      'operator': 'EQUALS',
                                      'values': 'UNIVERSAL_APP_CAMPAIGN'}
        report['selector'] = selector
        r = BytesIO()
        report_downloader.DownloadReport(report, r,
                                         skip_report_header=True,
                                         skip_report_summary=True)
        r.seek(0)
        self.df = pd.read_csv(r, parse_dates=True)
        if 'Cost' in self.df.columns:
            self.df['Cost'] /= 1000000
        if 'Views' in self.df.columns:
            self.df = self.video_calc(self.df)
        return self.df

    def get_downloader(self):
        try:
            report_downloader = self.adwords_client.GetReportDownloader(self.v)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            report_downloader = self.get_downloader()
        return report_downloader
