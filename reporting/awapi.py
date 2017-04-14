import logging
from googleads import adwords
from StringIO import StringIO
import datetime as dt
import pandas as pd
import numpy as np
import yaml
import sys

config_path = 'Config/'

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
        self.config_list = []
        self.adwords_client = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warn('Config file name not in vendor matrix.  Aborting.')
            sys.exit(0)
        logging.info('Loading Adwords config file: ' + str(config))
        self.configfile = config_path + config
        self.load_config()
        self.check_config()
        self.configfile = config_path + config
        self.adwords_client = (adwords.AdWordsClient.
                               LoadFromStorage(self.configfile))

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = yaml.safe_load(f)
        except IOError:
            logging.error(self.configfile + ' not found.  Aborting.')
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

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warn(item + 'not in Sizmek config file.  Aborting.')
                sys.exit(0)

    @staticmethod
    def video_calc(df):
        for column in VIEW_METRICS:
            df[column] = df[column].str.strip('%').astype(np.float)
            df[column] = (df[column] / 100) * df[VIEWS]
        return df

    @staticmethod
    def get_data_default_check(sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields is None:
            fields = def_fields
        return sd, ed, fields

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        sd = sd.date()
        ed = ed.date()
        if sd > ed:
            logging.warn('Start date greater than end date.  Start date was ' +
                         'set to end date.')
            sd = ed
        logging.info('Getting Adwords data from ' + str(sd) + ' until ' +
                     str(ed))
        report_downloader = self.adwords_client.GetReportDownloader('v201609')
        report = {
            'reportName': 'Adwords_Report',
            'dateRangeType': 'CUSTOM_DATE',
            'reportType': 'AD_PERFORMANCE_REPORT',
            'downloadFormat': 'CSV',
            'selector': {'fields': fields,
                         'dateRange': {'min': sd, 'max': ed}}}
        r = StringIO()
        report_downloader.DownloadReport(report, r,
                                         skip_report_header=True,
                                         skip_report_summary=True)
        r.seek(0)
        self.df = pd.read_csv(r, parse_dates=True)
        self.df['Cost'] /= 1000000
        self.df = self.video_calc(self.df)
        return self.df
