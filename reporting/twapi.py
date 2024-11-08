import os
import io
import sys
import ast
import gzip
import pytz
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
from requests_oauthlib import OAuth1Session
from requests.exceptions import ConnectionError

def_fields = ['ENGAGEMENT', 'BILLING', 'VIDEO']
conv_fields = ['MOBILE_CONVERSION', 'WEB_CONVERSION']
user_field = 'USER_STATS'
conversion_field = 'CONVERSIONS'
configpath = utl.config_path

base_url = 'https://ads-api.twitter.com'
standard_base_url = 'https://api.twitter.com/1.1'
user_url = '/users/lookup.json?'
status_url = '/statuses/lookup.json?'
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
web_conversions = ['conversion_purchases', 'conversion_sign_ups',
                   'conversion_site_visits', 'conversion_downloads',
                   'conversion_custom']
mobile_conversions = ['mobile_conversion_spent_credits',
                      'mobile_conversion_installs',
                      'mobile_conversion_content_views',
                      'mobile_conversion_add_to_wishlists',
                      'mobile_conversion_checkouts_initiated',
                      'mobile_conversion_reservations',
                      'mobile_conversion_tutorials_completed',
                      'mobile_conversion_achievements_unlocked',
                      'mobile_conversion_searches',
                      'mobile_conversion_add_to_carts',
                      'mobile_conversion_payment_info_additions',
                      'mobile_conversion_re_engages',
                      'mobile_conversion_shares', 'mobile_conversion_rates',
                      'mobile_conversion_logins', 'mobile_conversion_updates',
                      'mobile_conversion_levels_achieved',
                      'mobile_conversion_invites',
                      'mobile_conversion_key_page_views',
                      'mobile_conversion_downloads',
                      'mobile_conversion_sign_ups',
                      'mobile_conversion_site_visits',
                      'mobile_conversion_purchases']

user_fields = ['id', 'name', 'screen_name', 'location', 'description',
               'followers_count', 'friends_count', 'listed_count',
               'favourites_count', 'verified', 'statuses_count',
               'withheld_in_countries']


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
        self.campaign_filter = None
        self.dates = None
        self.client = None
        self.cid_dict = None
        self.asid_dict = None
        self.adid_dict = None
        self.mcid_dict = None
        self.promoted_account_id_dict = None
        self.tweet_dict = None
        self.usernames = None
        self.async_requests = []
        self.v = 11

    def reset_dicts(self):
        self.df = pd.DataFrame()
        self.dates = None
        self.client = None
        self.cid_dict = None
        self.asid_dict = None
        self.adid_dict = None
        self.promoted_account_id_dict = None
        self.tweet_dict = None
        self.async_requests = []

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
                            self.access_token, self.access_token_secret]
        if 'CAMPAIGN_FILTER' in self.config:
            self.campaign_filter = self.config['CAMPAIGN_FILTER']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in config file. '
                                ' Aborting.'.format(item))
                sys.exit(0)

    def get_client(self):
        self.client = OAuth1Session(self.consumer_key, self.consumer_secret,
                                    self.access_token, self.access_token_secret)

    def make_request(self, url, params=None, method='GET'):
        self.get_client()
        try:
            if params:
                r = self.client.request(method=method, url=url, params=params)
            else:
                r = self.client.request(method=method, url=url)
        except ConnectionError as e:
            logging.warning('Connection error retrying. \n: {}'.format(e))
            time.sleep(60)
            r = self.make_request(url=url, params=params)
        return r

    def request(self, url, resp_key=None, params=None, method='GET'):
        r = self.make_request(url=url, params=params, method=method)
        try:
            data = r.json()
        except IOError:
            data = None
        except ValueError:
            logging.warning('Rate limit exceeded.  Restarting after 300s.')
            time.sleep(300)
            data = self.request(url, resp_key, params)
        if resp_key and resp_key not in data:
            logging.warning('{} not in response, retrying. '
                            ' {}'.format(resp_key, data))
            time.sleep(60)
            data = self.request(url, resp_key, params)
        return data

    def get_id_dict(self, url, params, eid, name, parent, sd=None,
                    ad_name=None):
        data = self.request(url, params=params)
        if sd:
            et = 'end_time'
            week_fmt = '%Y-%m-%dT%H:%M:%SZ'
            last_week_sd = sd = sd - dt.timedelta(days=7)
            data['data'] = [
                x for x in data['data'] if
                (x[et] and dt.datetime.strptime(x[et], week_fmt) > last_week_sd)
                or not x[et]]
        if 'data' in data:
            if ad_name:
                id_dict = {x[eid]: {
                    'parent': x[parent],
                    'name': x[name],
                    'ad_name': x[ad_name],
                    'card_uri': x['card_uri'] if 'card_uri' in x else None
                } for x in data['data']}
            else:
                id_dict = {x[eid]: {'parent': x[parent], 'name': x[name]}
                           for x in data['data']}
        else:
            logging.warning('Data not in response: {}'.format(data))
            id_dict = {}
        id_dict = self.page_through_ids(data, id_dict, url, eid, name, parent,
                                        sd, params)
        return id_dict

    def get_user_stats(self, usernames):
        url = standard_base_url + user_url
        data = self.request(url, params={'screen_name': usernames})
        logging.info('Getting user data for {}'.format(usernames))
        if 'errors' in data:
            logging.warning('Error in response: {}'.format(data))
            return pd.DataFrame()
        users_df = pd.DataFrame(data)
        users_df = users_df[user_fields]
        return users_df

    def get_ids(self, entity, eid, name, parent, sd=None, parent_filter=None,
                ad_name=None):
        original_parent_filter = parent_filter
        url, params = self.create_base_url(entity)
        if parent_filter:
            original_params = params.copy()
            params = []
            parent_filter = [list(parent_filter)[x:x + 200]
                             for x in range(0, len(parent_filter), 200)]
            for pf in parent_filter:
                param = original_params.copy()
                param['{}s'.format(parent)] = ','.join(pf)
                params.append(param)
        else:
            params = [params]
        id_dict = {}
        for param in params:
            new_id_dict = self.get_id_dict(url=url, params=param, eid=eid,
                                           name=name, parent=parent, sd=sd,
                                           ad_name=ad_name)
            id_dict.update(new_id_dict)
        if self.campaign_filter and entity == 'campaigns':
            id_dict = {k: v for k, v in id_dict.items()
                       if self.campaign_filter in v['name']}
        if sd and not id_dict:
            logging.warning('No {} with start date {}, '
                            'attempting without start date.'.format(entity, sd))
            id_dict = self.get_ids(entity=entity, eid=eid, name=name,
                                   parent=parent, sd=None,
                                   parent_filter=original_parent_filter,
                                   ad_name=ad_name)
        return id_dict

    def page_through_ids(self, data, id_dict, first_url, eid, name, parent,
                         sd, params):
        if 'next_cursor' in data and data['next_cursor']:
            params['cursor'] = data['next_cursor']
            data = self.request(first_url, params=params)
            for x in data['data']:
                if sd:
                    if (x['end_time'] and dt.datetime.strptime(
                            x['end_time'], '%Y-%m-%dT%H:%M:%SZ') > sd):
                        id_dict[x[eid]] = {'parent': x[parent],
                                           'name': x[name]}
                else:
                    id_dict[x[eid]] = {'parent': x[parent], 'name': x[name]}
            id_dict = self.page_through_ids(data, id_dict, first_url,
                                            eid, name, parent, sd, params)
        return id_dict

    def get_all_id_dicts(self, sd):
        self.cid_dict = self.get_ids(entity='campaigns', eid='id', name='name',
                                     parent='funding_instrument_id')
        self.asid_dict = self.get_ids(entity='line_items', eid='id',
                                      name='name', parent='campaign_id', sd=sd,
                                      parent_filter=self.cid_dict.keys())
        self.adid_dict = self.get_ids('promoted_tweets', 'id',
                                      'tweet_id', 'line_item_id',
                                      parent_filter=self.asid_dict.keys())
        self.mcid_dict = self.get_ids(entity='media_creatives', eid='id',
                                      name='account_media_id',
                                      parent='line_item_id',
                                      parent_filter=self.asid_dict.keys())
        self.promoted_account_id_dict = self.get_ids(
            entity='promoted_accounts', eid='id', name='id',
            parent='line_item_id', parent_filter=self.asid_dict.keys())
        self.tweet_dict = self.get_ids('tweets', 'id',
                                       'full_text', 'id', ad_name='name')

    def get_data_default_check(self, sd, ed, fields):
        request_fields = def_fields
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields is None:
            return sd, ed, request_fields
        for field in fields:
            if field == conversion_field:
                request_fields = def_fields + conv_fields
            elif user_field in field:
                split_field = field.split(':')
                if len(split_field) > 1:
                    self.usernames = split_field[1]
        return sd, ed, request_fields

    def create_stats_url(self, fields=None, ids=None, sd=None, ed=None,
                         entity='PROMOTED_TWEET', placement='ALL_ON_TWITTER',
                         async_request=False):
        params = {}
        if fields:
            params = {'entity_ids': ','.join(ids),
                      'entity': entity,
                      'granularity': 'DAY',
                      'start_time': sd,
                      'end_time': ed,
                      'metric_groups': ','.join(fields),
                      'placement': placement}
        if async_request:
            job_url = '/jobs/'
        else:
            job_url = '/'
        act_url = '/{}/stats{}accounts/{}'.format(
            self.v, job_url, self.account_id)
        url = base_url + act_url
        return url, params

    def create_base_url(self, entity=None):
        act_url = '/{}/accounts/{}'.format(self.v, self.account_id)
        url = base_url + act_url
        params = {}
        if entity:
            url += '/{}'.format(entity)
            params['with_deleted'] = 'true'
            if entity != 'cards':
                params['count'] = '1000'
            if entity == 'tweets':
                params['tweet_type'] = 'PUBLISHED'
                params['timeline_type'] = 'ALL'
        return url, params

    def get_data(self, sd=None, ed=None, fields=None, async_request=True):
        self.reset_dicts()
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        sd, ed = self.get_date_info(sd, ed)
        if self.usernames:
            self.df = self.get_user_stats(usernames=self.usernames)
            self.df[coldate] = dt.date.today()
            return self.df
        self.config_list.append(self.account_id)
        self.check_config()
        self.df = self.get_df_for_all_dates(sd, ed, fields,
                                            async_request=async_request)
        if async_request:
            self.get_df_for_all_async_requests()
        if not self.df.empty:
            self.df = self.add_parents(self.df)
            self.df = self.rename_cols()
        return self.df

    def get_df_for_all_dates(self, sd, ed, fields, async_request=False):
        full_date_list = self.list_dates(sd, ed)
        timezone = self.get_account_timezone()
        if not timezone:
            timezone = self.find_account()
            if not timezone:
                return pd.DataFrame()
        self.get_all_id_dicts(sd)
        for entity in [('PROMOTED_TWEET', self.adid_dict),
                       ('PROMOTED_ACCOUNT', self.promoted_account_id_dict),
                       ('MEDIA_CREATIVE', self.mcid_dict)]:
            entity_name = entity[0]
            entity_dict = entity[1]
            ids_lists = [list(entity_dict.keys())[i:i + 20] for i
                         in range(0, len(entity_dict.keys()), 20)]
            self.loop_through_dates_for_df(
                full_date_list=full_date_list, timezone=timezone,
                ids_lists=ids_lists, fields=fields, entity=entity_name,
                async_request=async_request)
        return self.df

    def loop_through_dates_for_df(self, full_date_list, timezone, ids_lists,
                                  fields, entity, async_request=False):
        for date in full_date_list:
            sd = self.date_format(date, timezone)
            ed = self.date_format(date + dt.timedelta(days=1), timezone)
            logging.info('Getting Twitter data from '
                         '{} until {} for {}'.format(sd, ed, entity))
            if entity == 'PROMOTED_TWEET':
                possible_placements = ['ALL_ON_TWITTER', 'PUBLISHER_NETWORK']
            else:
                possible_placements = ['ALL_ON_TWITTER']
            for place in possible_placements:
                df = self.get_df_for_date(ids_lists, fields, sd, ed,
                                          date, place, entity=entity,
                                          async_request=async_request)
                if not async_request:
                    df = self.clean_df(df)
                    self.df = pd.concat(
                        [self.df, df], sort=True).reset_index(drop=True)
        return self.df

    def get_df_for_date_synchronous(self, ids, fields, sd, ed, date,
                                    place, entity, df):
        url, params = self.create_stats_url(fields, ids, sd, ed,
                                            entity=entity, placement=place)
        data = self.request(url, resp_key=jsondata, params=params)
        df = self.convert_response_to_df(data=data, date=date, df=df)
        return df

    def convert_response_to_df(self, data, date, df):
        self.dates = self.get_dates(date)
        id_df = pd.json_normalize(data[jsondata], [jsonidd], [colcid])
        col_names = {x: x.replace('metrics.', '') for x in id_df.columns}
        id_df = id_df.rename(columns=col_names)
        for col in mobile_conversions + web_conversions:
            if col in id_df.columns:
                col_df = id_df[col].apply(pd.Series)
                col_df.columns = ['{} - {}'.format(col, col_1)
                                  for col_1 in col_df.columns]
                id_df = pd.concat([id_df, col_df], axis=1)
        df = pd.concat([df, id_df], sort=True)
        return df

    def make_request_for_date_asynchronous(self, ids, fields, sd, ed,
                                           date, place, entity,
                                           times_requested=1):
        twitter_request = TwitterAsyncRequests(
            ids, fields, sd, ed, date, place, entity,
            times_requested=times_requested)
        url, params = self.create_stats_url(
            fields, ids, sd, ed, entity=entity, placement=place,
            async_request=True)
        data = self.request(url, params=params, method='POST')
        if ('data' in data and isinstance(data['data'], dict)
                and 'id' in data['data']):
            twitter_request.data = data
            self.async_requests.append(twitter_request)
        else:
            logging.warning('Response was incorrect: {}'.format(data))
            time.sleep(30)
            self.make_request_for_date_asynchronous(
                ids, fields, sd, ed, date, place, entity, times_requested)

    def get_df_for_date(self, ids_lists, fields, sd, ed, date, place,
                        entity='PROMOTED_TWEET', async_request=False):
        df = pd.DataFrame()
        for ids in ids_lists:
            if async_request:
                self.make_request_for_date_asynchronous(
                    ids, fields, sd, ed, date, place, entity)
            else:
                df = self.get_df_for_date_synchronous(
                    ids, fields, sd, ed, date, place, entity, df)
        return df

    def get_df_for_all_async_requests(self):
        async_requests = [x for x in self.async_requests if not x.completed]
        if async_requests:
            logging.info('{} jobs have not yet been completed.'.format(
                len(async_requests)))
            async_requests = async_requests.copy()
            self.df = self.check_all_async_request_ids(
                async_requests=async_requests)
            time.sleep(30)
            self.get_df_for_all_async_requests()
        else:
            logging.info('All jobs completed returning df.')
            return True

    def check_all_async_request_ids(self, async_requests=None):
        url, params = self.create_stats_url(async_request=True)
        params['count'] = 1000
        async_requests = [async_requests[x:x + 200]
                          for x in range(0, len(async_requests), 200)]
        for idx, async_request in enumerate(async_requests):
            job_id = [x.data['data']['id'] for x in async_request]
            logging.info('Checking job batch {} of {}'.format(
                idx + 1, len(async_requests)))
            params['job_ids'] = ','.join('{}'.format(x) for x in job_id)
            data = self.request(url=url, params=params, method='GET')
            for d in data['data']:
                current_request = [x for x in self.async_requests
                                   if x.data['data']['id'] == d['id']][0]
                needs_reset = current_request.check_for_reset()
                if d['status'] == 'SUCCESS':
                    self.get_df_from_completed_job(d)
                elif d['status'] == 'FAILED' or needs_reset:
                    logging.warning(
                        'Job #{} has been attempted {} times and will be '
                        'requested again.  Status: {}'.format(
                            d['id'], current_request.attempts_processing,
                            d['status']))
                    self.make_request_for_date_asynchronous(
                        current_request.ids, current_request.fields,
                        current_request.sd, current_request.ed,
                        current_request.date, current_request.place,
                        current_request.entity,
                        current_request.times_requested + 1)
                    self.async_requests = [x for x in self.async_requests
                                           if x.data['data']['id'] != d['id']]
        return self.df

    def get_df_from_completed_job(self, d):
        df = pd.DataFrame()
        cur_job = [x for x in self.async_requests
                   if x.data['data']['id'] == d['id']][0]
        logging.debug('Job {} completed adding to df'.format(d['id']))
        data_dl_url = d['url']
        try:
            r = requests.get(data_dl_url)
        except requests.exceptions.SSLError as e:
            logging.warning('SSL error with job: {} Retrying.'
                            'Error: {}'.format(d['id'], e))
            return None
        if r.status_code == 504:
            logging.warning('504 code with job: {} Retrying.'.format(d['id']))
            return None
        zip_obj = gzip.GzipFile(fileobj=io.BytesIO(r.content), mode='rb')
        response_data = json.loads(zip_obj.read())
        df = self.convert_response_to_df(
            data=response_data, date=cur_job.date, df=df)
        df = self.clean_df(df)
        self.df = pd.concat([self.df, df], sort=True).reset_index(drop=True)
        cur_job.completed = True

    @staticmethod
    def get_date_info(sd, ed):
        ed = ed + dt.timedelta(days=1)
        if sd > ed:
            logging.warning('Start date greater than end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
        return sd, ed

    def find_account(self):
        """
        Loops all available config files and attempts to determine if
        account matches if correct by calling get_account_timezone.

        :returns: account timezone if available False if failed
        """
        logging.warning('Incorrect account trying all config files.')
        config_dir = os.path.join(utl.config_path, 'twitter_api_cred')
        config_files = [x for x in os.listdir(config_dir)
                        if x.endswith('.json')]
        current_account = self.account_id
        current_campaign = self.campaign_filter
        old_config = self.configfile
        for config_file in config_files:
            self.configfile = os.path.join(config_dir, config_file)
            logging.info('Attempting file {}'.format(self.configfile))
            self.load_config()
            self.account_id = current_account
            self.campaign_filter = current_campaign
            timezone = self.get_account_timezone()
            if timezone:
                self.config['ACCOUNT_ID'] = self.account_id
                self.config['CAMPAIGN_FILTER'] = self.campaign_filter
                with open(old_config, 'w') as f:
                    json.dump(self.config, f)
                return timezone
        return False

    def get_account_timezone(self):
        url, params = self.create_base_url()
        data = self.request(url, params=params)
        if jsondata not in data:
            logging.warning('Data not in response : {}'.format(data))
            return False
        return data[jsondata][jsontz]

    @staticmethod
    def date_format(date, timezone):
        date = pytz.timezone(timezone).localize(date)
        date = date.astimezone(pytz.UTC)
        date = date.replace(tzinfo=None).isoformat() + 'Z'
        return date

    def add_parents(self, df):
        ad_maps = [('PROMOTED ACCOUNT', self.promoted_account_id_dict),
                   ('MEDIA CREATIVE', self.mcid_dict)]
        for entity in ad_maps:
            for id_key in entity[1]:
                current_dict = entity[1][id_key]
                current_dict['name'] = entity[0]
                self.adid_dict[current_dict['parent']] = current_dict
                self.adid_dict[id_key] = current_dict
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
        ad_name_dict = {str(x): self.tweet_dict[x]['ad_name']
                        for x in self.tweet_dict
                        if 'ad_name' in self.tweet_dict[x]}
        df['ad_name'] = df['tweetid'].astype(str)
        df['ad_name'] = df['ad_name'].replace(ad_name_dict)
        df['ad_name'] = df['ad_name'].fillna('Untitled ad')
        tweet_ids = df['tweetid'].unique()
        id_dict = {}
        for tweet_id in tweet_ids:
            if tweet_id in ['PROMOTED ACCOUNT', 'MEDIA CREATIVE']:
                continue
            if int(tweet_id) in self.tweet_dict:
                cur_tweet = self.tweet_dict[int(tweet_id)]
                if 'card_uri' in cur_tweet and 'name' in cur_tweet:
                    new_dict = {'name': cur_tweet['name'],
                                'Card name': cur_tweet['card_uri']}
                    id_dict[str(tweet_id)] = new_dict
        id_dict['PROMOTED ACCOUNT'] = {'name': 'PROMOTED ACCOUNT'}
        id_dict['MEDIA CREATIVE'] = {'name': 'MEDIA CREATIVE'}
        df = self.replace_with_parent(df, [id_dict, 'Tweet Text'], 'tweetid')
        return df

    def add_cards(self, df):
        card_col = 'Card name'
        if card_col not in df.columns:
            logging.warning('{} not in df.'.format(card_col))
            return df
        card_uris = df[card_col].unique()
        card_uris = [x for x in card_uris if x is not None and str(x) != 'nan'
                     and x[:4] == 'card']
        card_uris = [card_uris[x:x + 100]
                     for x in range(0, len(card_uris), 100)]
        uri_dict = {}
        for uri in card_uris:
            url, params = self.create_base_url('cards')
            urls = [url + '/all', url]
            for u in urls:
                params['card_uris'] = ','.join(uri)
                d = self.request(u, params=params)
                if 'data' not in d:
                    logging.warning('Card not found got response: {}'.format(d))
                    uri_dict[uri] = 'No Card Name'
                else:
                    for x in d['data']:
                        uri_dict[x['card_uri']] = x['name']
        df[card_col] = df[card_col].map(uri_dict)
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
        for col in mobile_conversions + web_conversions:
            if col in df.columns:
                df = utl.col_removal(df, 'API_Twitter', [col], warn=False)
        df = utl.col_removal(df, '', [jsonmet, jsonseg], warn=False)
        df = df.set_index(colcid)
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


class TwitterAsyncRequests(object):
    def __init__(self, ids, fields, sd, ed, date, place, entity, data=None,
                 times_requested=1):
        self.ids = ids
        self.fields = fields
        self.sd = sd
        self.ed = ed
        self.date = date
        self.place = place
        self.entity = entity
        self.data = data
        self.completed = False
        self.times_requested = times_requested
        self.attempts_processing = 0

    def check_for_reset(self):
        self.attempts_processing += 1
        if self.attempts_processing > 20 * self.times_requested:
            return True
        else:
            return False
