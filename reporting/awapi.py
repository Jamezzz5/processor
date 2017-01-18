import logging
from googleads import adwords
import datetime as dt
import pandas as pd
import numpy as np
import os
import yaml
import sys

logging.basicConfig(format=('%(asctime)s [%(module)14s]' +
                            '[%(levelname)8s] %(message)s'))
log = logging.getLogger()
log.setLevel(logging.INFO)
configpath = 'Config/'

VIEWS = 'Views'
VIEWS25 = 'Video played to 25%'
VIEWS50 = 'Video played to 50%'
VIEWS75 = 'Video played to 75%'
VIEWS100 = 'Video played to 100%'
VIEWMETRICS = [VIEWS25, VIEWS50, VIEWS75, VIEWS100]

def_fields = ['Date', 'AccountDescriptiveName', 'CampaignName', 'AdGroupName',
              'ImageCreativeName', 'Headline', 'HeadlinePart1', 'DisplayUrl',
              'HeadlinePart2', 'Description', 'Description1', 'Description2',
              'Impressions', 'Clicks', 'Cost', 'VideoViews',
              'VideoQuartile25Rate', 'VideoQuartile50Rate',
              'VideoQuartile75Rate', 'VideoQuartile100Rate']


class AwApi(object):
    def __init__(self):
        self.df = pd.DataFrame()

    def inputconfig(self, config):
        if str(config) == 'nan':
            logging.warn('Config file name not in vendor matrix.  Aborting.')
            sys.exit(0)
        logging.info('Loading Adwords config file: ' + str(config))
        self.configfile = configpath + config
        self.loadconfig()
        self.checkconfig()
        self.configfile = configpath + config
        self.adwords_client = (adwords.AdWordsClient.
                               LoadFromStorage(self.configfile))

    def loadconfig(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = yaml.safe_load(f)
        except:
            logging.error(self.configfile + ' not found.  Aborting.')
            sys.exit(0)
        self.config = self.config['adwords']
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.developer_token = self.config['developer_token']
        self.refresh_token = self.config['refresh_token']
        self.client_customer_id = self.config['client_customer_id']
        self.configlist = [self.config, self.client_id, self.client_secret,
                           self.developer_token, self.refresh_token,
                           self.client_customer_id]

    def checkconfig(self):
        for item in self.configlist:
            if item == '':
                logging.warn(item + 'not in Sizmek config file.  Aborting.')
                sys.exit(0)

    def videocalc(self, df):
        for column in VIEWMETRICS:
            df[column] = df[column].str.strip('%').astype(np.float)
            df[column] = (df[column] / 100) * df[VIEWS]
        return df

    def getdata(self, sd=(dt.datetime.today() - dt.timedelta(days=2)),
                ed=(dt.datetime.today() - dt.timedelta(days=1)),
                fields=def_fields):
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
            'selector': {'fields': def_fields,
                         'dateRange': {'min': sd, 'max': ed}}}
        with open('tempgoogle.csv', 'w') as f:
            report_downloader.DownloadReport(report, f,
                                             skip_report_header=True,
                                             skip_report_summary=True)
        self.df = pd.read_csv('tempgoogle.csv', parse_dates=True)
        os.remove('tempgoogle.csv')
        self.df['Cost'] = self.df['Cost'] / 1000000
        self.df = self.videocalc(self.df)
        return self.df
