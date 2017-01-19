import json
import logging
import sys
import pytz
import ast
import datetime as dt
import pandas as pd
import oauth2 as oauth

def_fields = ['ENGAGEMENT', 'BILLING', 'VIDEO']
configpath = 'Config/'

DOMAIN = 'https://ads-api.twitter.com'
PLACEMENT = '&placement=ALL_ON_TWITTER'
URLACC = '/1/accounts/'
URLSTACC = '/1/stats/accounts/'
URLEIDS = 'entity_ids='
URLCEN = '&entity=CAMPAIGN'
URLMG = '&metric_groups='
URLGST = '&granularity=DAY&start_time='
URLET = '&end_time='

reqcamp = 'campaigns'
reqdformat = '%Y-%m-%dT%H:%M:%SZ'

colspend = 'billed_charge_local_micro'
colcid = 'id'
coldate = 'Date'
colcname = 'name'

jsondata = 'data'
jsonparam = 'params'
jsonreq = 'request'
jsonmet = 'metrics'
jsonseg = 'segment'
jsonidd = 'id_data'
jsonst = 'start_time'
jsonet = 'end_time'

colnamedic = {'billed_charge_local_micro': 'Spend',
              'campaign': 'Campaign name',
              'impressions': 'Impressions',
              'clicks': 'Clicks',
              'url_clicks': 'Link clicks',
              'video_views_25': 'Video played 25%',
              'video_views_50': 'Video played 50%',
              'video_views_75': 'Video played 75%',
              'video_views_100': 'Video completions',
              'video_total_views': 'Video views'}


class TwApi(object):
    def __init__(self):
        self.df = pd.DataFrame()

    def inputconfig(self, config):
        logging.info('Loading Twitter config file: ' + config)
        self.configfile = configpath + config
        self.loadconfig()
        self.checkconfig()

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
        self.configlist = [self.consumer_key, self.consumer_secret,
                           self.access_token, self.access_token_secret,
                           self.account_id]

    def checkconfig(self):
        for item in self.configlist:
            if item == '':
                logging.warn(item + 'not in Twitter config file.  Aborting.')
                sys.exit(0)

    def request(self, url):
        consumer = oauth.Consumer(key=self.consumer_key,
                                  secret=self.consumer_secret)
        token = oauth.Token(key=self.access_token,
                            secret=self.access_token_secret)
        client = oauth.Client(consumer, token)
        response, content = client.request(url, method='GET')
        try:
            data = json.loads(content)
        except:
            data = None
        return response, data

    def getcids(self):
        resource_url = DOMAIN + URLACC + '%s/%s' % (self.account_id, reqcamp)
        res_headers, res_data = self.request(resource_url)
        cidname = {}
        for item in res_data[jsondata]:
            if item[colcid] not in cidname.keys():
                cidname[item[colcid]] = item[colcname]
        return cidname

    def getdata(self, sd=(dt.datetime.today() - dt.timedelta(days=2)),
                ed=(dt.datetime.today() - dt.timedelta(days=1)),
                fields=def_fields):
        ed = ed + dt.timedelta(days=1)
        if sd > ed:
            logging.warn('Start date greater than end date.  Start date was' +
                         'set to end date.')
            sd = ed - dt.timedelta(days=1)
        full_date_list = self.listdates(sd, ed)
        date_lists = map(None, *(iter(full_date_list),) * 7)
        stats_url = DOMAIN + URLSTACC + '%s?' % (self.account_id)
        metric_groups = URLMG + '%s' % ','.join(fields)
        self.cidname = self.getcids()
        ids_lists = map(None, *(iter(self.cidname.keys()),) * 20)
        for date_list in date_lists:
            date_list = filter(None, date_list)
            sd = self.date_format(date_list[0])
            ed = self.date_format(date_list[-1])
            logging.info('Getting Twitter data from ' + sd + ' until ' + ed)
            query_params = (URLGST + '{}' + URLET + '{}').format(sd, ed)
            df = pd.DataFrame()
            for ids in ids_lists:
                ids = filter(None, ids)
                entity_ids = (URLEIDS + '{}' + URLCEN).format(','.join(ids))
                query_url = (stats_url + entity_ids + query_params +
                             metric_groups + PLACEMENT)
                header, data = self.request(query_url)
                self.dates = self.getdates(data)
                iddf = pd.io.json.json_normalize(data[jsondata],
                                                 [jsonidd], [colcid])
                iddf = pd.concat([iddf,
                                  iddf[jsonmet].apply(pd.Series)], axis=1)
                df = df.append(iddf)
            df = self.clean_df(df)
            self.df = self.df.append(df)
        self.df = self.renamecols()
        return self.df

    def date_format(self, date):
        date = pytz.timezone('America/Los_Angeles').localize(date)
        date = date.astimezone(pytz.UTC)
        date = date.replace(tzinfo=None).isoformat() + 'Z'
        return date

    def clean_df(self, df):
        df = df.drop([jsonmet, jsonseg], axis=1).set_index(colcid)
        ndf = pd.DataFrame(columns=[coldate, colcid])
        for col in df.columns:
            tdf = df[col].apply(lambda x: self.cleandata(x)).apply(pd.Series)
            tdf = tdf.unstack().reset_index()
            tdf = tdf.rename(columns={0: col, 'level_0': coldate})
            ndf = pd.merge(ndf, tdf, on=[coldate, colcid], how='outer')
        df = ndf
        df['campaign'] = df[colcid].map(self.cidname)
        df[colspend] = df[colspend] / 1000000
        df[coldate].replace(self.dates, inplace=True)
        return df

    def cleandata(self, x):
        if str(x) == str('nan'):
            return 0
        x = str(x).strip('[]')
        return ast.literal_eval(x)

    def getdates(self, data):
        sd = dt.datetime.strptime(data[jsonreq][jsonparam][jsonst],
                                  reqdformat).date()
        ed = dt.datetime.strptime(data[jsonreq][jsonparam][jsonet],
                                  reqdformat).date()
        dates = self.listdates(sd, ed - dt.timedelta(days=1))
        dates = {k: v for k, v in enumerate(dates)}
        return dates

    def listdates(self, sd, ed):
        dates = []
        while sd <= ed:
            dates.append(sd)
            sd = sd + dt.timedelta(days=1)
        return dates

    def renamecols(self):
        self.df = self.df.rename(columns=colnamedic)
        return self.df
