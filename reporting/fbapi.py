import json
import datetime as dt
import pandas as pd
import logging
import time
import sys
import ast
from facebookads.api import FacebookAdsApi
from facebookads import objects
from facebookads.adobjects.adsinsights import AdsInsights
from facebookads.exceptions import FacebookRequestError

logging.basicConfig(format=('%(asctime)s [%(module)14s]' +
                            '[%(levelname)8s] %(message)s'))
log = logging.getLogger()
log.setLevel(logging.INFO)


def_fields = ['campaign_name', 'adset_name', 'ad_name', 'impressions',
              'inline_link_clicks', 'spend', 'video_10_sec_watched_actions',
              'video_p50_watched_actions', 'video_p100_watched_actions']
nestedcol = ['video_10_sec_watched_actions', 'video_p100_watched_actions',
             'video_p50_watched_actions']
colnamedic = {'date_start': 'Reporting Starts', 'date_stop': 'Reporting Ends',
              'campaign_name': 'Campaign', 'adset_name': 'Ad Set',
              'ad_name': 'Ad Name', 'impressions': 'Impressions',
              'inline_link_clicks': 'Link Clicks',
              'spend': 'Amount Spent (USD)',
              'video_10_sec_watched_actions': '3-Second Video Views',
              'video_p50_watched_actions': 'Video Watches at 50%',
              'video_p100_watched_actions': 'Video Watches at 100%'}
configpath = 'Config/'
configfile = 'Config/fbconfig.json'


class FbApi(object):
    def __init__(self):
        self.df = pd.DataFrame()

    def inputconfig(self, config):
        logging.info('Loading Facebook config file: ' + config)
        self.configfile = configpath + config
        self.loadconfig()
        self.checkconfig()

    def loadconfig(self):
        try:
            with open(configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(configfile + ' not found.  Aborting.')
            sys.exit(0)
        self.app_id = self.config['app_id']
        self.app_secret = self.config['app_secret']
        self.access_token = self.config['access_token']
        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
        self.account = objects.AdAccount(self.config['act_id'])

    def checkconfig(self):
        if self.app_id == '':
            logging.warn('App ID not in FB config file.  Aborting.')
            sys.exit(0)
        if self.app_secret == '':
            logging.warn('App Secret not in FB config file. Aborting.')
            sys.exit(0)
        if self.account == '':
            logging.warn('Account ID not in FB config file. Aborting.')
            sys.exit(0)
        if self.access_token == '':
            logging.warn('Access Token not in FB config file. Aborting.')
            sys.exit(0)

    def getdata(self, sd, ed=(dt.date.today() - dt.timedelta(days=1)),
                fields=def_fields):
        if sd > ed:
            logging.warn('Start date greater than end date.  Start data was' +
                         'set to end date.')
            sd = ed
        cd = sd
        delta = dt.timedelta(days=1)
        while cd <= ed:
            logging.info('Getting FB data for ' + str(cd))
            try:
                insights = list(self.account.get_insights(
                    fields=fields,
                    params={'level': AdsInsights.Level.ad,
                            'time_range': {'since': str(cd),
                                           'until': str(cd), },
                            'time_increment': 1, }))
            except FacebookRequestError as e:
                if e._api_error_code == 190:
                    logging.error('Facebook Access Token invalid.  Aborting.')
                    sys.exit(0)
                if e._api_error_code == 17:
                    logging.warn('Facebook rate limit reached.  Pausing for ' +
                                 '300 seconds.')
                    time.sleep(300)
                    continue
            if not insights:
                cd = cd + delta
                continue
            self.df = self.df.append(insights, ignore_index=True)
            cd = cd + delta
        self.df = self.renamecols()
        for col in nestedcol:
            try:
                self.df[col] = self.df[col].apply(lambda x: self.cleandata(x))
            except KeyError:
                continue
        return self.df

    def cleandata(self, x):
        if str(x) == str('nan'):
            return 0
        x = str(x).strip('[]')
        return ast.literal_eval(x)['value']

    def renamecols(self):
        self.df = self.df.rename(columns=colnamedic)
        return self.df
