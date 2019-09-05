import os
import sys
import ast
import pytz
import json
import time
import logging
import pandas as pd
import datetime as dt
import oauth2 as oauth
import reporting.utils as utl
import pandas.io.json as pdjson

def_fields = ['ENGAGEMENT', 'BILLING', 'VIDEO']
configpath = utl.config_path

base_url = 'https://ads-api.twitter.com'
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
        self.client = None
        self.cid_dict = None
        self.asid_dict = None
        self.adid_dict = None
        self.tweet_dict = None
        self.v = 5

    def input_config(self, config):
        logging.info('Loading Twitter config file: {}.'.format(config))
        self.configfile = os.path.join(configpath, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.configfile))
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
                logging.warning('{} not in config file. '
                                ' Aborting.'.format(item))
                sys.exit(0)

    def get_client(self):
        consumer = oauth.Consumer(key=self.consumer_key,
                                  secret=self.consumer_secret)
        token = oauth.Token(key=self.access_token,
                            secret=self.access_token_secret)
        self.client = oauth.Client(consumer, token)

    def request(self, url, resp_key=None):
        self.get_client()
        response, content = self.client.request(url, method='GET')
        try:
            data = json.loads(content)
        except IOError:
            data = None
        except ValueError:
            logging.warning('Rate limit exceeded.  Restarting after 300s.')
            time.sleep(300)
            response, data = self.request(url, resp_key)
        if resp_key and resp_key not in data:
            logging.warning('{} not in data, retrying. '
                            ' {}'.format(resp_key, data))
            time.sleep(60)
            response, data = self.request(url, resp_key)
        return response, data

    def get_ids(self, entity, eid, name, parent, sd=None, parent_filter=None):
        url = self.create_base_url(entity)
        if parent_filter:
            url += '&{}s={}'.format(parent, ','.join(parent_filter))
        headers, data = self.request(url)
        if sd:
            data['data'] = [x for x in data['data'] if
                            (x['end_time'] and
                             dt.datetime.strptime(
                                 x['end_time'], '%Y-%m-%dT%H:%M:%SZ') > sd)
                            or not x['end_time']]
        if 'data' in data:
            id_dict = {x[eid]: {'parent': x[parent], 'name': x[name]}
                       for x in data['data']}
        else:
            id_dict = {}
        id_dict = self.page_through_ids(data, id_dict, url, eid, name, parent,
                                        sd)
        return id_dict

    def page_through_ids(self, data, id_dict, first_url, eid, name, parent,
                         sd):
        if 'next_cursor' in data and data['next_cursor']:
            url = "{}&cursor={}".format(first_url, data['next_cursor'])
            headers, data = self.request(url)
            for x in data['data']:
                if sd:
                    if (x['end_time'] and dt.datetime.strptime(
                            x['end_time'], '%Y-%m-%dT%H:%M:%SZ') > sd):
                        id_dict[x[eid]] = {'parent': x[parent],
                                           'name': x[name]}
                else:
                    id_dict[x[eid]] = {'parent': x[parent], 'name': x[name]}
            id_dict = self.page_through_ids(data, id_dict, first_url,
                                            eid, name, parent, sd)
        return id_dict

    def get_all_id_dicts(self, sd):
        self.cid_dict = self.get_ids('campaigns', 'id', 'name',
                                     'funding_instrument_id', sd)
        self.asid_dict = self.get_ids('line_items', 'id', 'name',
                                      'campaign_id',
                                      parent_filter=self.cid_dict.keys())
        self.adid_dict = self.get_ids('promoted_tweets', 'id',
                                      'tweet_id', 'line_item_id',
                                      parent_filter=self.asid_dict.keys())
        self.tweet_dict = self.get_ids('scoped_timeline', 'id',
                                       'text', 'id',)

    @staticmethod
    def get_data_default_check(sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields is None:
            fields = def_fields
        return sd, ed, fields

    def create_stats_url(self, fields=None, ids=None, sd=None, ed=None,
                         entity='PROMOTED_TWEET', placement='ALL_ON_TWITTER'):
        act_url = '/{}/stats/accounts/{}?'.format(self.v, self.account_id)
        ent_url = 'entity_ids={}&entity={}'.format(','.join(ids), entity)
        sded_url = '&granularity=DAY&start_time={}&end_time={}'.format(sd, ed)
        metric_url = '&metric_groups={}'.format(','.join(fields))
        place_url = '&placement={}'.format(placement)
        url = base_url + act_url + ent_url + sded_url + metric_url + place_url
        return url

    def create_base_url(self, entity=None):
        act_url = '/{}/accounts/{}'.format(self.v, self.account_id)
        url = base_url + act_url
        if entity:
            url += '/{}'.format(entity)
            if entity != 'cards':
                url += '?count=1000'
            if entity == 'promoted_tweets':
                url += '&with_deleted=true'
        return url

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        sd, ed = self.get_date_info(sd, ed)
        self.df = self.get_df_for_all_dates(sd, ed, fields)
        if not self.df.empty:
            self.df = self.add_parents(self.df)
            self.df = self.rename_cols()
        return self.df

    def get_df_for_all_dates(self, sd, ed, fields):
        full_date_list = self.list_dates(sd, ed)
        timezone = self.get_account_timezone()
        self.get_all_id_dicts(sd)
        ids_lists = [list(self.adid_dict.keys())[i:i + 20] for i
                     in range(0, len(self.adid_dict.keys()), 20)]
        for date in full_date_list:
            sd = self.date_format(date, timezone)
            ed = self.date_format(date + dt.timedelta(days=1), timezone)
            logging.info('Getting Twitter data from '
                         '{} until {}'.format(sd, ed))
            for place in ['ALL_ON_TWITTER', 'PUBLISHER_NETWORK']:
                df = self.get_df_for_date(ids_lists, fields, sd, ed,
                                          date, place)
                df = self.clean_df(df)
                self.df = self.df.append(df, sort=True).reset_index(drop=True)
        return self.df

    def get_df_for_date(self, ids_lists, fields, sd, ed, date, place):
        df = pd.DataFrame()
        for ids in ids_lists:
            url = self.create_stats_url(fields, ids, sd, ed, placement=place)
            header, data = self.request(url, resp_key=jsondata)
            self.dates = self.get_dates(date)
            id_df = pdjson.json_normalize(data[jsondata], [jsonidd], [colcid])
            id_df = pd.concat([id_df, id_df[jsonmet].apply(pd.Series)], axis=1)
            df = df.append(id_df, sort=True)
        return df

    @staticmethod
    def get_date_info(sd, ed):
        ed = ed + dt.timedelta(days=1)
        if sd > ed:
            logging.warning('Start date greater than end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
        return sd, ed

    def get_account_timezone(self):
        url = self.create_base_url()
        header, data = self.request(url)
        if jsondata not in data:
            logging.warning('Data not in response : {}'.format(data))
        return data[jsondata][jsontz]

    @staticmethod
    def date_format(date, timezone):
        date = pytz.timezone(timezone).localize(date)
        date = date.astimezone(pytz.UTC)
        date = date.replace(tzinfo=None).isoformat() + 'Z'
        return date

    def add_parents(self, df):
        parent_maps = [[self.adid_dict, 'tweetid'],
                       [self.asid_dict, 'adset'], [self.cid_dict, 'campaign']]
        for parent in parent_maps:
            df = self.replace_with_parent(df, parent, 'id')
        df = df.dropna(how='all', subset=['impressions', 'clicks',
                                          'billed_charge_local_micro'])
        df = self.add_tweets(df)
        df = self.add_cards(df)
        return df

    def add_tweets(self, df):
        tweet_ids = df['tweetid'].unique()
        id_dict = {}
        tids = [tweet_ids[x:x + 100] for x in range(0, len(tweet_ids), 100)]
        for tid in tids:
            url = ('https://api.twitter.com/1.1/statuses/lookup.json?'
                   'id={}&include_card_uri=true'
                   .format(','.join([str(x) for x in tid])))
            h, d = self.request(url)
            for x in d:
                if 'card_uri' in x:
                    id_dict[str(x['id'])] = {'name': x['text'],
                                             'Card name': x['card_uri']}
                else:
                    id_dict[str(x['id'])] = {'name': x['text'],
                                             'Card name': None}
        df = self.replace_with_parent(df, [id_dict, 'Tweet Text'], 'tweetid')
        return df

    def add_cards(self, df):
        card_uris = df['Card name'].unique()
        card_uris = [x for x in card_uris if x is not None and str(x) != 'nan']
        card_uris = [card_uris[x:x + 100] for x in range(0, len(card_uris), 100)]
        uri_dict = {}
        for uri in card_uris:
            url = self.create_base_url('cards')
            url += '/all?card_uris={}'.format(','.join(uri))
            h, d = self.request(url)
            if 'data' not in d:
                logging.warning('Card not found got response: {}'.format(d))
                uri_dict[uri] = 'No Card Name'
            else:
                for x in d['data']:
                    uri_dict[x['card_uri']] = x['name']
        df['Card name'] = df['Card name'].map(uri_dict)
        return df

    @staticmethod
    def replace_with_parent(df, parent, id_col):
        df[id_col] = df[id_col].map(parent[0])
        df = df.join(df[id_col].apply(pd.Series))
        df = utl.col_removal(df, 'API_Twitter', [0, id_col], warn=False)
        df = df.rename(columns={'name': parent[1], 'parent': id_col})
        return df

    def clean_df(self, df):
        if df.empty:
            return df
        df = df.drop([jsonmet, jsonseg], axis=1).set_index(colcid)
        ndf = pd.DataFrame(columns=[coldate, colcid])
        ndf = utl.data_to_type(ndf, str_col=[colcid], int_col=[coldate])
        for col in df.columns:
            tdf = df[col].apply(lambda x: self.clean_data(x)).apply(pd.Series)
            tdf = tdf.unstack().reset_index()
            tdf = tdf.rename(columns={0: col, 'level_0': coldate})
            tdf = utl.data_to_type(tdf, str_col=[colcid], int_col=[coldate])
            ndf = pd.merge(ndf, tdf, on=[coldate, colcid], how='outer')
        df = ndf
        df[colspend] /= 1000000
        df[coldate].replace(self.dates, inplace=True)
        return df

    @staticmethod
    def clean_data(x):
        if str(x) == str('nan'):
            return 0
        x = str(x).strip('[]')
        return ast.literal_eval(x)

    def get_dates(self, date):
        dates = self.list_dates(date, date + dt.timedelta(days=1))
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
