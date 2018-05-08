import json
import logging
import sys
import pytz
import ast
import time
import datetime as dt
import pandas as pd
import pandas.io.json as pdjson
import oauth2 as oauth
import reporting.utils as utl

def_fields = ['ENGAGEMENT', 'BILLING', 'VIDEO']
configpath = utl.config_path

DOMAIN = 'https://ads-api.twitter.com'
PLACEMENT = '&placement=ALL_ON_TWITTER'
URLACC = '/3/accounts/'
URLSTACC = '/3/stats/accounts/'
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
jsontz = 'timezone'

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
        self.configfile = None
        self.config = None
        self.consumer_key = None
        self.consumer_secret = None
        self.access_token = None
        self.access_token_secret = None
        self.account_id = None
        self.config_list = []
        self.dates = None
        self.cidname = None

    def input_config(self, config):
        logging.info('Loading Twitter config file: %s', config)
        self.configfile = configpath + config
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('%s not found.  Aborting.', self.configfile)
            sys.exit(0)
        self.consumer_key = self.config['CONSUMER_KEY']
        self.consumer_secret = self.config['CONSUMER_SECRET']
        self.access_token = self.config['ACCESS_TOKEN']
        self.access_token_secret = self.config['ACCESS_TOKEN_SECRET']
        self.account_id = self.config['ACCOUNT_ID']
        self.config_list = [self.consumer_key, self.consumer_secret,
                            self.access_token, self.access_token_secret,
                            self.account_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('%s not in config file.  Aborting.', item)
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
        except IOError:
            data = None
        except ValueError:
            logging.warning('Rate limit exceeded.  Restarting after 300s.')
            time.sleep(300)
            response, data = self.request(url)
        return response, data

    def get_cids(self):
        resource_url = DOMAIN + URLACC + '%s/%s' % (self.account_id, reqcamp)
        res_headers, res_data = self.request(resource_url)
        cid_name = {}
        for item in res_data[jsondata]:
            if item[colcid] not in cid_name.keys():
                cid_name[item[colcid]] = item[colcname]
        return cid_name

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
        ed = ed + dt.timedelta(days=1)
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date'
                            'was set to end date.')
            sd = ed - dt.timedelta(days=1)
        full_date_list = self.list_dates(sd, ed)
        date_lists = [full_date_list[i:i + 7] for i
                      in range(0, len(full_date_list), 7)]
        timezone = self.get_account_timezone()
        stats_url = DOMAIN + URLSTACC + '%s?' % self.account_id
        metric_groups = URLMG + '%s' % ','.join(fields)
        self.cidname = self.get_cids()
        ids_lists = map(None, *(iter(self.cidname.keys()),) * 20)
        for date_list in date_lists:
            if date_list[0] == date_list[-1]:
                sd = self.date_format(date_list[0] - dt.timedelta(days=1),
                                      timezone)
            else:
                sd = self.date_format(date_list[0], timezone)
            ed = self.date_format(date_list[-1], timezone)
            logging.info('Getting Twitter data from %s until %s', sd, ed)
            query_params = (URLGST + '{}' + URLET + '{}').format(sd, ed)
            df = pd.DataFrame()
            for ids in ids_lists:
                ids = filter(None, ids)
                entity_ids = (URLEIDS + '{}' + URLCEN).format(','.join(ids))
                query_url = (stats_url + entity_ids + query_params +
                             metric_groups + PLACEMENT)
                header, data = self.request(query_url)
                self.dates = self.get_dates(data)
                id_df = pdjson.json_normalize(data[jsondata],
                                                  [jsonidd], [colcid])
                id_df = pd.concat([id_df,
                                  id_df[jsonmet].apply(pd.Series)], axis=1)
                df = df.append(id_df)
            df = self.clean_df(df)
            self.df = self.df.append(df)
        self.df = self.rename_cols()
        return self.df

    def get_account_timezone(self):
        acc_url = DOMAIN + URLACC + '%s/' % self.account_id
        header, data = self.request(acc_url)
        return data[jsondata][jsontz]

    @staticmethod
    def date_format(date, timezone):
        date = pytz.timezone(timezone).localize(date)
        date = date.astimezone(pytz.UTC)
        date = date.replace(tzinfo=None).isoformat() + 'Z'
        return date

    def clean_df(self, df):
        df = df.drop([jsonmet, jsonseg], axis=1).set_index(colcid)
        ndf = pd.DataFrame(columns=[coldate, colcid])
        for col in df.columns:
            tdf = df[col].apply(lambda x: self.clean_data(x)).apply(pd.Series)
            tdf = tdf.unstack().reset_index()
            tdf = tdf.rename(columns={0: col, 'level_0': coldate})
            ndf = pd.merge(ndf, tdf, on=[coldate, colcid], how='outer')
        df = ndf
        df['campaign'] = df[colcid].map(self.cidname)
        df[colspend] /= 1000000
        df[coldate].replace(self.dates, inplace=True)
        return df

    @staticmethod
    def clean_data(x):
        if str(x) == str('nan'):
            return 0
        x = str(x).strip('[]')
        return ast.literal_eval(x)

    def get_dates(self, data):
        sd = dt.datetime.strptime(data[jsonreq][jsonparam][jsonst],
                                  reqdformat).date()
        ed = dt.datetime.strptime(data[jsonreq][jsonparam][jsonet],
                                  reqdformat).date()
        dates = self.list_dates(sd, ed - dt.timedelta(days=1))
        dates = {k: v for k, v in enumerate(dates)}
        return dates

    @staticmethod
    def list_dates(sd, ed):
        dates = []
        while sd <= ed:
            dates.append(sd)
            sd = sd + dt.timedelta(days=1)
        return dates

    def rename_cols(self):
        self.df = self.df.rename(columns=colnamedic)
        return self.df
