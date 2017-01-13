import logging
from googleads import adwords
import datetime as dt
import pandas as pd
import os
import yaml
import sys

logging.basicConfig(format=('%(asctime)s [%(module)14s]' +
                            '[%(levelname)8s] %(message)s'))
log = logging.getLogger()
log.setLevel(logging.INFO)
configpath = 'Config/'

def_fields = ['Date', 'AccountDescriptiveName', 'CampaignName', 'AdGroupName',
              'ImageCreativeName', 'Headline', 'HeadlinePart1',
              'HeadlinePart2', 'Description', 'Description1', 'Description2',
              'Impressions', 'Clicks', 'Cost', 'VideoViews']


class AwApi(object):
    def __init__(self):
        self.df = pd.DataFrame()

    def inputconfig(self, config):
        logging.info('Loading Adwords config file: ' + config)
        self.configfile = configpath + config
        self.loadconfig()
        self.checkconfig()
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

    def checkconfig(self):
        if self.client_id == '':
            logging.warn('Client ID not in AW config file.  Aborting.')
            sys.exit(0)
        if self.client_secret == '':
            logging.warn('Client Secret not in AW config file. Aborting.')
            sys.exit(0)
        if self.developer_token == '':
            logging.warn('Developer Token not in AW config file. Aborting.')
            sys.exit(0)
        if self.refresh_token == '':
            logging.warn('Refresh Token not in AW config file. Aborting.')
            sys.exit(0)
        if self.client_customer_id == '':
            logging.warn('Client Customer ID not in AW config file. Aborting.')
            sys.exit(0)

    def getdata(self, sd=(dt.date.today() - dt.timedelta(days=2)),
                ed=(dt.date.today() - dt.timedelta(days=1)),
                fields=def_fields):
        sd = sd.date()
        ed = ed.date()
        if sd > ed:
            logging.warn('Start date greater than end date.  Start date was' +
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
        self.df = pd.read_csv('tempgoogle.csv')
        os.remove('tempgoogle.csv')
        self.df['Cost'] = self.df['Cost'] / 1000000
        return self.df
