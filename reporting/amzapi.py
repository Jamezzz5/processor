import io
import os
import sys
import pytz
import json
import time
import copy
import logging
import requests
import oauthlib
import pandas as pd
import datetime as dt
import reporting.utils as utl
from requests_oauthlib import OAuth2Session
import reporting.vmcolumns as vmc

config_path = utl.config_path


class AmzApi(object):
    base_url = 'https://advertising-api.amazon.com'
    eu_url = 'https://advertising-api-eu.amazon.com'
    fe_url = 'https://advertising-api-fe.amazon.com'
    refresh_url = 'https://api.amazon.com/auth/o2/token'
    def_metrics = [
        'impressions', 'clicks', 'cost',
        'attributedConversions14d',
        'attributedConversions14dSameSKU', 'attributedUnitsOrdered14d',
        'attributedSales14d', 'attributedSales14dSameSKU']
    sponsored_columns = ['date', 'impressions', 'clicks', 'cost']
    sp_columns = [
        'purchases14d', 'purchasesSameSku14d', 'unitsSoldClicks14d',
        'unitsSoldSameSku14d', 'sales14d', 'attributedSalesSameSku14d',
        'adGroupId']
    sb_columns = [
        'detailPageViewsClicks', 'newToBrandDetailPageViews',
        'newToBrandDetailPageViewsClicks', 'newToBrandPurchases',
        'newToBrandPurchasesClicks', 'newToBrandSales',
        'newToBrandSalesClicks', 'newToBrandUnitsSold',
        'newToBrandUnitsSoldClicks', 'purchases', 'purchasesClicks',
        'purchasesPromoted', 'sales', 'salesClicks', 'salesPromoted',
        'unitsSold', 'unitsSoldClicks', 'video5SecondViews',
        'videoCompleteViews', 'videoFirstQuartileViews', 'detailPageViews',
        'videoMidpointViews', 'videoThirdQuartileViews', 'videoUnmutes']
    default_config_file_name = 'amzapi.json'
    campaign_col = 'campaignName'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.advertiser_id = None
        self.campaign_id = None
        self.profile_id = None
        self.report_ids = []
        self.report_types = []
        self.export_id = ''
        self.campaign_export_id = ''
        self.cid_df = pd.DataFrame()
        self.config_list = None
        self.client = None
        self.headers = None
        self.version = '2'
        self.amazon_dsp = False
        self.timezone = None
        self.df = pd.DataFrame()
        self.r = None
        self.cache_file = os.path.join(config_path, 'report_cache.json')
        if not os.path.exists(self.cache_file):
            with open(self.cache_file, 'w') as f:
                json.dump({}, f)
        with open(self.cache_file, 'r') as f:
            self.report_cache = json.load(f)
        self.fresh_pull = False

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading AMZ config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.access_token = self.config['access_token']
        self.refresh_token = self.config['refresh_token']
        self.advertiser_id = self.config['advertiser_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token]
        if 'campaign_id' in self.config:
            self.campaign_id = self.config['campaign_id']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in AMZ config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def refresh_client_token(self, extra, attempt=1):
        try:
            token = self.client.refresh_token(self.refresh_url, **extra)
        except requests.exceptions.ConnectionError as e:
            attempt += 1
            if attempt > 100:
                logging.warning('Max retries exceeded: {}'.format(e))
                token = None
            else:
                logging.warning('Connection error retrying 60s: {}'.format(e))
                token = self.refresh_client_token(extra, attempt)
        return token

    def get_client(self, errors=0):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        try:
            token = self.refresh_client_token(extra)
        except oauthlib.oauth2.rfc6749.errors.CustomOAuth2Error as e:
            logging.warning('Could not get token attempting again. '
                            'Oauth error as follows {}'.format(e))
            time.sleep(30)
            errors += 1
            if errors > 10:
                logging.warning('Could not get token exiting.')
                sys.exit(0)
            self.get_client(errors=errors + 1)
        self.client = OAuth2Session(self.client_id, token=token)

    def set_headers(self, content_type=''):
        self.headers = {'Amazon-Advertising-API-ClientId': self.client_id}
        if self.profile_id:
            self.headers['Amazon-Advertising-API-Scope'] = str(self.profile_id)
        if content_type:
            self.headers['Content-Type'] = content_type
            self.headers['Accept'] = content_type

    def get_dsp_profiles(self, dsp_profiles, endpoint=None):
        """
        Loops through list of dsp_profiles to match requested one

        :param dsp_profiles: List of dsp profiles
        :param endpoint: Endpoint to check
        :return: The profile if it exists else None
        """
        profile = None
        for dsp_profile in dsp_profiles:
            dsp_id = str(dsp_profile['profileId'])
            self.headers['Amazon-Advertising-API-Scope'] = dsp_id
            url = '{}/dsp/advertisers'.format(endpoint)
            r = self.make_request(
                url, method='GET', headers=self.headers,
                json_response_key='response',
                skip_error_type='ENTITY_NOT_SUPPORTED')
            if 'response' in r.json():
                profile = [x for x in r.json()['response'] if
                           self.advertiser_id in x['advertiserId']]
            if profile:
                profile = profile[0]
                self.profile_id = profile['advertiserId']
                self.set_headers()
                self.base_url = self.check_correct_endpoint(
                    [dsp_profile], endpoint)
                self.amazon_dsp = True
                self.timezone = pytz.timezone(profile['timezone'])
                break
        return profile

    def get_accounts_by_user(self, endpoint):
        profile = []
        url = '{}/adsAccounts/list'.format(endpoint)
        r = self.make_request(url, method='POST', headers=self.headers)
        for ad_account in r.json()['adsAccounts']:
            for alternate_id in ad_account['alternateIds']:
                if 'entityId' in alternate_id:
                    if self.advertiser_id[1:] in alternate_id['entityId']:
                        country_code = alternate_id['countryCode']
                        profile = [x for x in ad_account['alternateIds']
                                   if x['countryCode'] == country_code and
                                   'profileId' in x]
                        break
        return profile

    def check_correct_endpoint(self, profile, url):
        region_map = {'NA': ['US', 'CA', 'MX'],
                      'EU': ['GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'SE', 'PL',
                             'BE'],
                      'FE': ['JP', 'SG', 'AU']}
        endpoints = {'NA': self.base_url,
                     'EU': self.eu_url,
                     'FE': self.fe_url}
        country_code = profile[0]['countryCode']
        for region, countries in region_map.items():
            if country_code in countries:
                url = endpoints[region]
        return url

    def get_profiles(self):
        self.set_headers()
        for endpoint in [self.base_url, self.eu_url, self.fe_url]:
            url = '{}/v{}/profiles'.format(endpoint, self.version)
            json_response = []
            for _ in range(5):
                r = self.make_request(url, method='GET', headers=self.headers)
                json_response = r.json()
                if isinstance(json_response, list):
                    break
                else:
                    time.sleep(10)
            profile = [x for x in json_response
                       if self.advertiser_id[1:] in x['accountInfo']['id']]
            if not profile:
                profile = self.get_accounts_by_user(endpoint)
            if profile:
                self.profile_id = profile[0]['profileId']
                self.set_headers()
                self.base_url = self.check_correct_endpoint(profile, endpoint)
                return True
            dsp_profiles = [x for x in json_response if 'agency'
                            in x['accountInfo']['type']]
            if dsp_profiles:
                profile = self.get_dsp_profiles(dsp_profiles, endpoint)
                if profile:
                    return True
        logging.warning('Could not find the specified profile, check that '
                        'the provided account ID {} is correct and API has '
                        'access.'.format(self.advertiser_id))
        return False

    @staticmethod
    def date_check(sd, ed):
        if sd > ed or sd == ed:
            logging.warning('Start date greater than or equal to end date.  '
                            'Start date was set to end date.')
            sd = ed - dt.timedelta(days=1)
        return sd, ed

    def set_fields(self, fields):
        if fields:
            for field in fields:
                if field == 'hsa':
                    self.report_types.append('hsa')
                if field == 'refresh':
                    self.fresh_pull = True

    def get_data_default_check(self, sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=2)
        if ed is None:
            ed = dt.datetime.today() + dt.timedelta(days=1)
        if dt.datetime.today().date() == ed.date():
            ed += dt.timedelta(days=1)
        if self.amazon_dsp:
            if ed.date() > dt.datetime.today().date():
                ed = dt.datetime.today()
        sd, ed = self.date_check(sd, ed)
        self.set_fields(fields)
        return sd, ed

    def create_url(self, report_type='sp', version=True, record_type='adGroups',
                   report_id=False):
        url = self.base_url
        if version:
            url = '{}/v{}'.format(url, self.version)
        if report_id:
            url = '{}/reports/{}'.format(url, report_id)
        else:
            url = '{}/{}/{}/report'.format(url, report_type, record_type)
        return url

    def old_hsa_report_flow(self, sd, ed):
        date_list = self.list_dates(sd, ed)
        report_made = self.request_reports_for_all_dates(date_list)
        if report_made:
            self.check_and_get_all_reports(self.report_ids)
        else:
            logging.warning('Report not made returning blank df.')
        return self.df

    def get_data(self, sd=None, ed=None, fields=None):
        self.report_ids = []
        self.df = pd.DataFrame()
        self.profile_id = None
        profile_found = self.get_profiles()
        if not profile_found:
            return self.df
        sd, ed = self.get_data_default_check(sd, ed, fields)
        date_list = self.list_dates(sd, ed)
        report_ids = []
        self.purge_expired_cache(fresh_pull=self.fresh_pull)
        for cur_date in date_list:
            end_date = dt.datetime.combine(cur_date, dt.time.max)
            report_id = self.request_report(cur_date, end_date)
            report_ids.extend(report_id)
        if not report_ids:
            logging.warning('Could not generate report, returning blank df')
            return self.df
        self.df = self.check_and_get_reports(report_ids)
        logging.info('All reports downloaded - returning dataframe.')
        self.df = self.filter_df_on_campaign(self.df)
        return self.df

    def filter_df_on_campaign(self, df):
        if self.campaign_id and self.campaign_col in df.columns:
            df = df[df[self.campaign_col].astype('U').str.contains(
                self.campaign_id)]
        return df

    def get_report_id(self, url, body):
        duplicate_str = 'The Request is a duplicate of : '
        report_id = None
        for x in range(5):
            r = self.make_request(url, method='POST', body=body,
                                  headers=self.headers)
            if 'reportId' in r.json():
                report_id = r.json()['reportId']
            elif ('message' in r.json() and r.json()['message'] ==
                    'Too Many Requests'):
                logging.warning(
                    'Too many requests pausing.  Attempt: {}.  '
                    'Response: {}'.format((x + 1), r.json()))
                time.sleep(30)
            elif 'detail' in r.json() and duplicate_str in r.json()['detail']:
                logging.warning('Duplicate request, attempting to pull.')
                report_id = r.json()['detail'].split(duplicate_str)[1]
            else:
                logging.warning('Error in request as follows: {}'
                                .format(r.json()))
                time.sleep(2)
            if report_id:
                break
        return report_id

    @staticmethod
    def get_dsp_request_body(body):
        body.update({
            "format": "JSON",
            "metrics": ['totalCost', 'impressions', 'clickThroughs',
                        'videoStart', 'videoFirstQuartile', 'videoMidpoint',
                        'videoThirdQuartile', 'videoComplete',
                        'totalSales14d', 'totalPurchases14d',
                        'totalROAS14d', 'sales14d', 'purchases14d'],
            "type": "CAMPAIGN",
            "dimensions": ["ORDER", "LINE_ITEM", "CREATIVE"],
            "timeUnit": "DAILY"
        })
        return body

    def get_sponsored_body(self, body, ad_product, cols, report_type,
                           group_by_ad_group=False):
        body['configuration']['adProduct'] = ad_product
        body['configuration']['columns'] = self.sponsored_columns + cols
        body['configuration']['reportTypeId'] = report_type
        group_by = ['campaign']
        if group_by_ad_group:
            group_by = ['adGroup']
        body['configuration']['groupBy'] = group_by
        return body

    def get_sponsored_bodies(self, body):
        body['configuration'] = {
            'timeUnit': 'DAILY',
            'format': 'GZIP_JSON'}
        sp_items = ['SPONSORED_PRODUCTS', self.sp_columns, 'spCampaigns', True]
        sb_items = ['SPONSORED_BRANDS', self.sb_columns, 'sbCampaigns', False]
        request_bodies = []
        for ad_product, cols, report_type, group_by in [sp_items, sb_items]:
            body_copy = copy.deepcopy(body)
            request_body = self.get_sponsored_body(
                body_copy, ad_product, cols, report_type, group_by)
            request_bodies.append(request_body)
        return request_bodies

    def check_and_get_export(self, export_id, entity='adGroups'):
        """
        Retrieves a previously requested export and returns as df

        :param export_id: The id of the export to retrieve
        :param entity: The entity type to
        :return:
        """
        df = pd.DataFrame()
        lower_entity = entity.lower()
        content_type = 'application/vnd.{}export.v1+json'.format(lower_entity)
        self.set_headers(content_type)
        url = '{}/exports/{}'.format(self.base_url, export_id)
        r = self.make_request(url, method='GET', headers=self.headers,
                              json_response_key='url', sleep_time=5)
        if 'url' in r.json():
            report_url = r.json()['url']
            r = requests.get(report_url)
            df = pd.read_json(io.BytesIO(r.content), compression='gzip')
            cols = ['campaignId', 'name']
            if entity == 'adGroups':
                cols.append('adGroupId')
                col_rename = 'adGroupName'
            else:
                col_rename = self.campaign_col
            col_rename = {'name': col_rename}
            df = df[cols]
            df = df.rename(columns=col_rename)
        return df

    def request_export(self, entity='adGroups'):
        """
        Requests an export for a file that has names and ids

        :param entity: The object level to request for
        :return: The id of the export to pull
        """
        lower_entity = entity.lower()
        content_type = 'application/vnd.{}export.v1+json'.format(lower_entity)
        self.set_headers(content_type)
        url = '{}/{}/export'.format(self.base_url, entity)
        body = {'adProductFilter': ["SPONSORED_PRODUCTS", "SPONSORED_BRANDS"],
                'stateFilter': ['ENABLED', 'PAUSED', 'ARCHIVED']}
        r = self.make_request(url, method='POST', headers=self.headers,
                              body=body, json_response_key='exportId')
        export_id = r.json()['exportId']
        return export_id

    def filter_request_body_on_campaign(self, request_bodies):
        if self.cid_df.empty:
            self.cid_df = self.check_and_get_export(
                self.campaign_export_id, entity='campaigns')
        df = self.filter_df_on_campaign(self.cid_df)
        campaign_ids = df['campaignId'].to_list()
        for body in request_bodies:
            filter_list = [{"field": "campaignId", "values": campaign_ids}]
            body['configuration']['filters'] = filter_list
        return request_bodies

    def request_report(self, sd, ed):
        delta = ed - sd
        delta = delta.days
        if delta > 31:
            new_date = delta - 31
            logging.warning('Dates exceed 31 day limit: shortening length by {}'
                            ' days'.format(new_date))
            logging.info("Recommend creating an additional API card")
            ed = ed - dt.timedelta(days=new_date)
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        cache_key = ('DSP_{}_{}'.format(sd, ed) if self.amazon_dsp else
                     'SP_{}_{}'.format(sd, ed))
        if cache_key in self.report_cache:
            logging.info('reusing cached report IDs for {}'.format(cache_key))
            report_ids = self.report_cache[cache_key]['report_ids']
            return report_ids
        is_dsp = ' DSP ' if self.amazon_dsp else ' Sponsored Product/Brand '
        msg = 'Requesting{}report for dates: {} to {}'.format(is_dsp, sd, ed)
        logging.info(msg)
        base_body = {"endDate": ed, "startDate": sd}
        if self.amazon_dsp:
            sp_body = self.get_dsp_request_body(base_body)
            header = 'application/vnd.dspcreatereports.v3+json'
            url = '{}/accounts/{}/dsp/reports'.format(
                self.base_url, self.profile_id)
            request_bodies = [sp_body]
        else:
            header = 'application/vnd.createasyncreportrequest.v3+json'
            url = '{}/reporting/reports'.format(self.base_url)
            request_bodies = self.get_sponsored_bodies(base_body)
            if not self.export_id:
                self.export_id = self.request_export()
                self.campaign_export_id = self.request_export('campaigns')
        self.headers['Accept'] = header
        report_ids = []
        for request_body in request_bodies:
            report_id = self.get_report_id(url, request_body)
            report_ids.append(report_id)
        timestamp = dt.datetime.now(tz=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.report_cache[cache_key] = {'timestamp': timestamp,
                                        'report_ids': report_ids}
        with open(self.cache_file, 'w') as f:
            json.dump(self.report_cache, f)
        return report_ids

    @staticmethod
    def is_cache_expired(cache_entry, hours=24):
        if not cache_entry:
            return True
        timestamp = cache_entry.get('timestamp')
        timestamp = dt.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
        if not timestamp:
            return True
        age = dt.datetime.utcnow() - timestamp
        return age.total_seconds() > hours * 3600

    def purge_expired_cache(self, hours=24, fresh_pull=False):
        keys_to_delete = []
        if fresh_pull:
            for key, entry in self.report_cache.items():
                keys_to_delete.append(key)
            for key in keys_to_delete:
                del self.report_cache[key]
            logging.info('Doing fresh pull, removing cache')
        else:
            for key, entry in self.report_cache.items():
                if self.is_cache_expired(cache_entry=entry, hours=hours):
                    keys_to_delete.append(key)
            for key in keys_to_delete:
                del self.report_cache[key]
                logging.info('Expired report cache entry removed: {}'.format(key))

    def check_and_get_reports(self, report_ids, attempts=150, wait=30):
        if not isinstance(report_ids, list):
            report_ids = [report_ids]
        df_list = []
        for report_id in report_ids:
            df = self.check_report_status(report_id, attempts, wait)
            df_list.append(df)
        self.df = self.merge_dataframes(df_list)
        if self.export_id and not self.df.empty:
            exports = [(self.export_id, 'adGroups', 'adGroupId'),
                       (self.campaign_export_id, 'campaigns', 'campaignId')]
            for export_id, entity, id_col in exports:
                id_df = self.check_and_get_export(export_id, entity=entity)
                self.df = self.df.merge(id_df, on=id_col, how='left')
        return self.df

    def check_report_status(self, report_id, attempts, wait):
        if self.amazon_dsp:
            self.headers['Accept'] = 'application/vnd.dspgetreports.v3+json'
            url = '{}/accounts/{}/dsp/reports/{}'.format(
                self.base_url, self.profile_id, report_id)
            complete_status = 'SUCCESS'
            url_key = 'location'
        else:
            self.headers['Accept'] = (
                'application/vnd.createasyncreportrequest.v3+json')
            url = '{}/reporting/reports/{}'.format(
                self.base_url, report_id)
            complete_status = 'COMPLETED'
            url_key = 'url'
        df = pd.DataFrame()
        for attempt in range(attempts):
            logging.info(
                'Checking for report {} attempt {}'.format(
                    report_id, attempt + 1))
            r = self.make_request(url, method='GET', headers=self.headers)
            if 'status' in r.json():
                if r.json()['status'] == complete_status:
                    report_url = r.json()[url_key]
                    r = requests.get(report_url)
                    if self.amazon_dsp:
                        df = pd.DataFrame(r.json())
                    else:
                        df = pd.read_json(
                            io.BytesIO(r.content), compression='gzip'
                        )
                    if df.empty:
                        logging.warning('Dataframe empty, likely no data  - '
                                        'returning empty dataframe')
                    else:
                        if self.amazon_dsp and 'date' in df.columns:
                            df['date'] = df['date'].apply(
                                lambda x: dt.datetime.fromtimestamp(
                                    x / 1000, tz=pytz.UTC).date())
                    break
                else:
                    time.sleep(wait)
            elif ('message' in r.json() and r.json()['message'] ==
                  'Too Many Requests'):
                logging.warning(
                    'Too many requests pausing.  Attempt: {}.  '
                    'Response: {}'.format((attempt + 1), r.json()))
                time.sleep(wait)
            else:
                logging.warning(
                    'No status in response as follows: {}'.format(r.json()))
        return df

    @staticmethod
    def merge_dataframes(dfs):
        valid_dfs = [df for df in dfs if df is not None and not df.empty]
        return pd.concat(valid_dfs, ignore_index=True,
                         sort=False) if valid_dfs else pd.DataFrame()

    def request_reports_for_all_dates(self, date_list):
        for report_date in date_list:
            report_made = self.request_reports_for_date(report_date)
            if not report_made:
                return False
        return True

    def request_reports_for_date(self, report_date):
        for report_type in self.report_types:
            if report_type == 'hsa':
                has_video = [True, False]
            else:
                has_video = [False]
            for vid in has_video:
                report_made = self.make_report_request(report_date, report_type,
                                                       vid)
                if not report_made:
                    return False
        return True

    def make_report_request(self, report_date, report_type, vid, attempt=1):
        report_made = False
        report_date_string = dt.datetime.strftime(report_date, '%Y%m%d')
        logging.info(
            'Requesting report for date: {} type: {} video: {} attempt: {}'
            .format(report_date_string, report_type, vid, attempt))
        url = self.create_url(report_type=report_type)
        body = {'reportDate': report_date_string,
                'metrics': ','.join(self.def_metrics)}
        if vid:
            body['creativeType'] = 'video'
        r = self.make_request(url, method='POST', headers=self.headers,
                              body=body)
        if 'reportId' not in r.json():
            logging.warning('reportId not in json: {}'.format(r.json()))
            if 'code' in r.json() and r.json()['code'] == '406':
                logging.warning('Could not request date range is too long.')
            else:
                if attempt < 10:
                    time.sleep(30)
                    attempt += 1
                    report_made = self.make_report_request(
                        report_date, report_type, vid, attempt
                    )
        else:
            report_id = r.json()['reportId']
            self.report_ids.append(
                {'report_id': report_id, 'date': report_date,
                 'complete': False})
            report_made = True
        return report_made

    def check_and_get_all_reports(self, report_ids):
        for report_id in report_ids:
            self.check_and_get_report(report_id)
        rem_report_ids = [x for x in self.report_ids if not x['complete']]
        if rem_report_ids:
            self.check_and_get_all_reports(rem_report_ids)

    def check_and_get_report(self, report_id_dict):
        logging.info('Checking report for date: {}'.format(
            dt.datetime.strftime(report_id_dict['date'], '%Y-%m-%d')))
        report_id = report_id_dict['report_id']
        url = self.create_url(report_id=report_id)
        r = self.make_request(url, method='GET', headers=self.headers)
        if 'status' in r.json() and 'SUCCESS' in r.json()['status']:
            logging.debug('Report available - downloading.')
            url += '/download'
            r = self.make_request(url, method='GET', headers=self.headers,
                                  json_response=False)
            df = pd.read_json(io.BytesIO(r.content), compression='gzip')
            if not df.empty:
                if 'impressions' in df.columns:
                    df = df.loc[(df['impressions'] > 0)]
                df['Date'] = report_id_dict['date']
                self.df = pd.concat([self.df, df], ignore_index=True)
            self.report_ids = [
                x for x in self.report_ids if x['report_id'] != report_id]
            report_id_dict['complete'] = True
            self.report_ids.append(report_id_dict)
        else:
            if 'status' in r.json():
                logging.info('Report unavailable - waiting 30s.  \n'
                             'Current status: {}\n Current Status Details: {}'
                             ''.format(r.json()['status'],
                                       r.json()['statusDetails']))
            else:
                logging.info('Report unavailable - waiting 30s.  \n'
                             'Current error: {}'.format(r.json()))
            time.sleep(30)

    def make_request(self, url, method, body=None, params=None, headers=None,
                     attempt=1, json_response=True, json_response_key='',
                     skip_error_type='', sleep_time=30):
        self.get_client()
        attempts = 10
        for x in range(attempts):
            request_success = True
            try:
                self.r = self.raw_request(url, method, body=body, params=params,
                                          headers=headers)
            except (requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError) as e:
                logging.warning('Warning SSLError as follows {}'.format(e))
                request_success = False
            json_error = json_response and 'error' in self.r.json()
            json_error_2 = (json_response_key and
                            json_response_key not in self.r.json())
            skip_error = False
            if skip_error_type:
                if 'errors' in self.r.json():
                    error_response = self.r.json()['errors']
                    if error_response:
                        error_response = error_response[0]['errorType']
                        if error_response == skip_error_type:
                            skip_error = True
            if json_error or json_error_2 and not skip_error:
                logging.warning(
                    'Request error.  Retrying {}'.format(self.r.json()))
                request_success = False
            if request_success:
                break
            else:
                time.sleep(sleep_time)
                attempt += 1
                if attempt > attempts:
                    self.request_error()
        return self.r

    def raw_request(self, url, method, body=None, params=None, headers=None):
        kwargs = {}
        for kwarg in [(body, 'json'), (params, 'params'), (headers, 'headers')]:
            if kwarg[0]:
                kwargs[kwarg[1]] = kwarg[0]
        if method == 'POST':
            request_method = self.client.post
        else:
            request_method = self.client.get
        self.r = request_method(url, **kwargs)
        return self.r

    def request_error(self):
        logging.warning('Unknown error: {}'.format(self.r.text))
        sys.exit(0)

    @staticmethod
    def list_dates(sd, ed):
        dates = []
        while sd < ed:
            dates.append(sd)
            sd = sd + dt.timedelta(days=1)
        return dates

    def check_advertiser_id(self, results, acc_col, success_msg, failure_msg):
        profile = self.get_profiles()
        if profile:
            row = [acc_col, ' '.join([success_msg, str(self.advertiser_id)]),
                   True]
            results.append(row)
        else:
            msg = ('Advertiser ID NOT Found. '
                   'Double Check ID and Ensure Permissions were granted.')
            row = [acc_col, ' '.join([failure_msg, msg]), False]
            results.append(row)
        return results, profile

    def check_campaign_ids(self, results, camp_col, success_msg, failure_msg):
        sd = dt.datetime.today() - dt.timedelta(days=30)
        ed = dt.datetime.today() - dt.timedelta(days=1)
        sd = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed = dt.datetime.strftime(ed, '%Y-%m-%d')
        body = {"endDate": ed, "startDate": sd}
        if self.amazon_dsp:
            body.update({
                "type": "CAMPAIGN",
                "dimensions": ["ORDER"],
                "metrics": ["totalCost"],
                "startDate": sd,
                "endDate": ed
            })
            self.headers['Accept'] = 'application/vnd.dspcreatereports.v3+json'
            url = '{}/accounts/{}/dsp/reports'.format(
                self.base_url, self.profile_id)
        else:
            body['configuration'] = {
                    'adProduct': 'SPONSORED_PRODUCTS',
                    'columns':  ["cost", "campaignId", self.campaign_col],
                    'reportTypeId': 'spCampaigns',
                    'format': 'GZIP_JSON',
                    'groupBy': ['campaign'],
                    "timeUnit": "SUMMARY"
                }
            self.headers['Accept'] = (
                'application/vnd.createasyncreportrequest.v3+json')
            url = '{}/reporting/reports'.format(self.base_url)
        report_id = self.get_report_id(url, body)
        if not report_id:
            msg = ' '.join([failure_msg,
                            'Unable to check campaign names. Try again later.'])
            row = [camp_col, msg, True]
            results.append(row)
            return results
        self.check_and_get_reports(report_id, 10, 10)
        if self.df.empty:
            msg = ' '.join([failure_msg,
                            'Unable to check campaign names. Try again later.'])
            row = [camp_col, msg, True]
            results.append(row)
            return results
        df = self.filter_df_on_campaign(self.df)
        if df.empty:
            msg = ' '.join([failure_msg, 'No Campaigns Under Filter. '
                                         'Double Check Filter and Active.'])
            row = [camp_col, msg, False]
            results.append(row)
        elif self.amazon_dsp:
            msg = ' '.join(
                [success_msg, 'All Campaigns Under Advertiser Included.'])
            row = [camp_col, msg, True]
            results.append(row)
        else:
            msg = ' '.join(
                [success_msg, 'CAMPAIGNS INCLUDED IF DATA PAST START DATE:'])
            row = [camp_col, msg, True]
            results.append(row)
            for campaign in df[self.campaign_col].tolist():
                row = [camp_col, campaign, True]
                results.append(row)
        return results

    def test_connection(self, acc_col, camp_col, acc_pre):
        success_msg = 'SUCCESS:'
        failure_msg = 'FAILURE:'
        self.set_headers()
        results, r = self.check_advertiser_id(
            [], acc_col, success_msg, failure_msg)
        if False in results[0]:
            return pd.DataFrame(data=results, columns=vmc.r_cols)
        results = self.check_campaign_ids(
            results, camp_col, success_msg, failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
