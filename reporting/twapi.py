import json
import datetime as dt
import pandas as pd
import logging
import time
import sys
from pytz import timezone
from twitter_ads.client import Client
from twitter_ads.campaign import Campaign
from twitter_ads.campaign import LineItem


logging.basicConfig(format=('%(asctime)s [%(module)14s]' +
                            '[%(levelname)8s] %(message)s'))
log = logging.getLogger()
log.setLevel(logging.INFO)


def_fields = ['ENGAGEMENT', 'BILLING', 'VIDEO']
configpath = 'Config/'


class TwApi(object):
    def __init__(self):
        self.df = pd.DataFrame()

    def inputconfig(self, config):
        logging.info('Loading Twitter config file: ' + config)
        self.configfile = configpath + config
        self.loadconfig()
        self.checkconfig()
        self.client = Client(self.consumer_key, self.consumer_secret,
                             self.access_token, self.access_token_secret)
        self.account = self.client.accounts(self.account_id)

    def loadconfig(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(self.configfile + ' not found.  Aborting.')
            sys.exit(0)
        self.consumer_key = self.config['CONSUMER_KEY']
        self.consumer_secret = self.config['CONSUMER_SECRET']
        self.access_token = self.config['ACCESS_TOKEN']
        self.access_token_secret = self.config['ACCESS_TOKEN_SECRET']
        self.account_id = self.config['ACCOUNT_ID']

    def checkconfig(self):
        if self.consumer_key == '':
            logging.warn('Consumer Key not in TW config file.  Aborting.')
            sys.exit(0)
        if self.consumer_secret == '':
            logging.warn('Consumer Secret not in TW config file.  Aborting.')
            sys.exit(0)
        if self.access_token == '':
            logging.warn('Access Token not in TW config file.  Aborting.')
            sys.exit(0)
        if self.access_token_secret == '':
            logging.warn('Token Secret not in TW config file.  Aborting.')
            sys.exit(0)
        if self.account_id == '':
            logging.warn('Account ID not in TW config file.  Aborting.')
            sys.exit(0)

    def getdata(self, sd, ed=(dt.date.today() - dt.timedelta(days=1)),
                fields=def_fields):
        if sd > ed:
            logging.warn('Start date greater than end date.  Start date was' +
                         'set to end date.')
            sd = ed - dt.timedelta(days=1)
        sd = sd.to_datetime().replace(tzinfo=timezone('US/Pacific'))
        ed = ed.to_datetime().replace(tzinfo=timezone('US/Pacific'))
        print sd, ed
        print type(sd)
       
        line_items = list(self.account.line_items())
        ids = map(lambda x: x.id, line_items)
        ids_lists = map(None, *(iter(ids),) * 20)
        for ids in ids_lists:
            ids = filter(None, ids)
            queued_job = LineItem.queue_async_stats_job(self.account, ids, fields,
                                                        start_time=sd, end_time=ed,
                                                        granularity='DAY')
        job_id = queued_job['id']
        seconds = 15
        time.sleep(seconds)
        async_stats_job_result = LineItem.async_stats_job_result(self.account, job_id)
        async_data = LineItem.async_stats_job_data(self.account, async_stats_job_result['url'])
        print async_data

        """
        data = LineItem.all_stats(self.account, ids_list, fields, start_time=sd,
                                  end_time=ed, granularity='DAY')

            
        LineItem.all_stats(self.account)
        
        line_items = list(self.account.line_items())
        stats = line_items[0].stats(fields)
        print(stats)
        
        line_items = list(self.account.line_items(None, count=10))[:10]
        line_items[0].stats(fields)
        ids = map(lambda x: x.id, line_items)
        LineItem.all_stats(self.account, ids, fields)
        queued_job = LineItem.queue_async_stats_job(self.account, ids, fields)
        job_id = queued_job['id']
        seconds = 15
        time.sleep(seconds)
        async_stats_job_result = LineItem.async_stats_job_result(self.account, job_id)
        async_data = LineItem.async_stats_job_data(self.account, async_stats_job_result['url'])
        print async_data
        """
        return self.df

    def date_format(self, date):
        date = pytz.timezone('America/Los_Angeles').localize(date)
        date = date.astimezone(pytz.UTC)
        date = date.replace(tzinfo=None).isoformat() + 'Z'
        return date
