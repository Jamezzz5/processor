import html
import io
import os
import re
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
import reporting.gsapi as gsapi

config_path = utl.config_path


class AmzApi(object):
    base_url = 'https://advertising-api.amazon.com'
    na_url = 'https://advertising-api.amazon.com'
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
        'videoMidpointViews', 'videoThirdQuartileViews', 'videoUnmutes',
        'campaignId']
    default_config_file_name = 'amzapi.json'
    campaign_col = 'campaignName'
    sp_keyword_columns = [
        'searchTerm', 'keywordId', 'matchType', 'targeting',
        'keywordBid', 'keywordType', 'portfolioId']
    sb_keyword_columns = [
        'searchTerm', 'keywordId', 'matchType', 'keywordBid']
    # Ads API v1 is one unified resource for sponsored ads and DSP, so
    # there is no per-ad-product url or vendor media type any more.  Paths
    # and field names below come from Amazon's published v1 collection.
    v1_report_path = '/adsApi/v1/create/reports'
    v1_retrieve_path = '/adsApi/v1/retrieve/reports'
    v1_delete_path = '/adsApi/v1/delete/reports'
    v1_accounts_path = '/adsApi/v1/query/advertiserAccounts'
    v1_format = 'GZIP_JSON'
    v1_dimension_fields = [
        'date.value', 'advertiserAccount.id', 'advertiserAccount.name',
        'campaign.id', 'campaign.name', 'budgetCurrency.value',
        'adProduct.value']
    v1_ad_group_fields = ['adGroup.id', 'adGroup.name']
    v1_metric_fields = [
        'metric.impressions', 'metric.clicks', 'metric.ctr',
        'metric.totalCost', 'metric.purchases', 'metric.sales',
        'metric.unitsSold', 'metric.costPerPurchase',
        'metric.purchaseRate', 'metric.roas', 'metric.purchasesPromoted',
        'metric.salesPromoted', 'metric.unitsSoldPromoted',
        'metric.newToBrandPurchases', 'metric.newToBrandSales',
        'metric.newToBrandUnitsSold', 'metric.newToBrandRoas',
        'metric.detailPageViews', 'metric.brandedSearches']
    v1_dsp_metric_fields = [
        'metric.supplyCost', 'metric.viewableImpressions',
        'metric.completeViewsVideoAd', 'metric.firstQuartileVideoAd',
        'metric.midpointVideoAd', 'metric.thirdQuartileVideoAd',
        'metric.unmutesVideoAd', 'metric.completeListensAudioAd',
        'metric.purchasesHalo', 'metric.salesHalo',
        'metric.unitsSoldHalo', 'metric.addToCart']
    # v1 field names are dotted and product neutral, while every
    # dictionary and vendor matrix downstream keys off the v2/v3 column
    # names.  Renaming on the way out keeps that translation untouched.
    # The conversion rows assume v1's unqualified purchases/sales carry
    # the 14 day window the v3 columns named explicitly, and that
    # 'Promoted' is v1's name for the old same-SKU split.
    v1_column_renames = {
        'date.value': 'date',
        'dateRange.value': 'dateRange',
        'advertiserAccount.id': 'advertiserId',
        'advertiserAccount.name': 'advertiserName',
        'advertiserAccount.entityId': 'entityId',
        'advertiserAccount.timeZone': 'timeZone',
        'budgetCurrency.value': 'currency',
        'adProduct.value': 'adProduct',
        'campaign.id': 'campaignId',
        'campaign.name': 'campaignName',
        'adGroup.id': 'adGroupId',
        'adGroup.name': 'adGroupName',
        'metric.impressions': 'impressions',
        'metric.clicks': 'clicks',
        'metric.ctr': 'ctr',
        'metric.totalCost': 'cost',
        'metric.roas': 'roas',
        'metric.costPerPurchase': 'costPerPurchase',
        'metric.purchaseRate': 'purchaseRate',
        'metric.purchases': 'purchases14d',
        'metric.sales': 'sales14d',
        'metric.unitsSold': 'unitsSoldClicks14d',
        'metric.purchasesPromoted': 'purchasesSameSku14d',
        'metric.salesPromoted': 'attributedSalesSameSku14d',
        'metric.unitsSoldPromoted': 'unitsSoldSameSku14d',
        'metric.newToBrandPurchases': 'newToBrandPurchases',
        'metric.newToBrandSales': 'newToBrandSales',
        'metric.newToBrandUnitsSold': 'newToBrandUnitsSold',
        'metric.detailPageViews': 'detailPageViews',
        'metric.brandedSearches': 'brandedSearches'}
    # The DSP flow already spoke a different set of legacy names for the
    # same numbers, so it overlays the shared map rather than forking it.
    v1_dsp_column_renames = {
        'metric.totalCost': 'totalCost',
        'metric.clicks': 'clickThroughs',
        'metric.completeViewsVideoAd': 'videoComplete',
        'metric.firstQuartileVideoAd': 'videoFirstQuartile',
        'metric.midpointVideoAd': 'videoMidpoint',
        'metric.thirdQuartileVideoAd': 'videoThirdQuartile',
        'metric.purchases': 'totalPurchases14d',
        'metric.sales': 'totalSales14d',
        'metric.roas': 'totalROAS14d',
        'metric.purchasesPromoted': 'purchases14d',
        'metric.salesPromoted': 'sales14d'}
    # Amazon's published examples carry completedReportParts as null, so
    # the download key is not pinned down.  Presence of a url is the
    # terminal signal and these statuses only short circuit the wait.
    v1_url_keys = ['url', 'location', 'downloadUrl', 'presignedUrl']
    v1_complete_status = ['COMPLETED', 'COMPLETE', 'SUCCESS', 'SUCCEEDED']
    v1_failed_status = ['FAILED', 'FAILURE', 'CANCELLED', 'DELETED']

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
        self.seen_asin = {}
        self.asin_path = os.path.join(config_path,
                                      'asin_product_mapping.csv')
        if not os.path.exists(self.cache_file):
            with open(self.cache_file, 'w') as f:
                json.dump({}, f)
        with open(self.cache_file, 'r') as f:
            self.report_cache = json.load(f)
        self.fresh_pull = False
        self.product_report = False
        self.include_keywords = False
        self.dsp_id = ''
        self.use_v1 = True
        self.v1_account_id = ''
        self.product_sheet_id = '1BIc9mreRHelaI8sXdnFm8eRW4kB3iJyN4w0BbcqIsjg'

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
        if 'use_v1' in self.config:
            self.use_v1 = self.config['use_v1']

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
            self.dsp_id = str(dsp_profile['profileId'])
            self.headers['Amazon-Advertising-API-Scope'] = self.dsp_id
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
        endpoints = {'NA': self.na_url,
                     'EU': self.eu_url,
                     'FE': self.fe_url}
        country_code = profile[0]['countryCode']
        for region, countries in region_map.items():
            if country_code in countries:
                url = endpoints[region]
        return url

    def get_profiles(self):
        self.set_headers()
        for endpoint in [self.na_url, self.eu_url, self.fe_url]:
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
                if field.lower() == 'keyword':
                    self.include_keywords = True
                    logging.info('Keyword-level data enabled via API Fields')
                if field.lower() == 'refresh':
                    self.fresh_pull = True
                if field.lower() == 'product':
                    self.product_report = True
                if field.lower() == 'v1':
                    self.use_v1 = True
                if field.lower() == 'v3':
                    self.use_v1 = False
                    logging.info('Ads API v1 disabled via API Fields')

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

    def set_v1_headers(self):
        """
        Builds the header set the Ads API v1 requires.

        v1 renamed the client id header and moved account selection off
        the scope header for anything that is not sponsored ads, so the
        v3 header set is not accepted as is.

        :returns: dict of headers for a v1 request
        """
        headers = {'Amazon-Ads-ClientId': self.client_id,
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        if self.amazon_dsp:
            account_id = self.v1_account_id or self.dsp_id
            if account_id:
                headers['Amazon-Ads-AccountId'] = str(account_id)
        elif self.profile_id:
            headers['Amazon-Advertising-API-Scope'] = str(self.profile_id)
        return headers

    def check_v1_supported(self):
        """
        Says whether the requested report shape exists in v1 yet.

        Amazon's published v1 field list carries no ASIN, search term or
        creative field, so the product, keyword and legacy hsa reports
        have to stay on the v3 flow until those fields are documented.

        :returns: True when the v1 flow can answer the request
        """
        unsupported = []
        if self.product_report:
            unsupported.append('product')
        if self.include_keywords:
            unsupported.append('keyword')
        if 'hsa' in self.report_types:
            unsupported.append('hsa')
        if unsupported:
            logging.info(
                'Ads API v1 has no documented fields for the {} report, '
                'using the v3 flow.'.format(', '.join(unsupported)))
            return False
        return True

    def get_v1_account_id(self):
        """
        Resolves the advertiserAccountId a v1 report body is keyed on.

        The profile flow this class already runs resolves a v3 profile
        id.  The v1 accounts resource returns both identifiers on one
        row, so the profile id is matched against alternateIds to bridge
        the two rather than asking the user to supply a second id.

        :returns: the advertiser account id, or '' when unresolved
        """
        url = '{}{}'.format(self.base_url, self.v1_accounts_path)
        r = self.make_request(url, method='POST', body={},
                             headers=self.set_v1_headers(),
                             json_response=False)
        accounts = r.json().get('advertiserAccounts') or []
        if not accounts:
            logging.warning('No v1 advertiser accounts in response: {}'
                            ''.format(r.json()))
            return ''
        wanted = [str(self.profile_id), str(self.dsp_id),
                  str(self.advertiser_id)]
        for account in accounts:
            ids = [str(account.get('advertiserAccountId'))]
            for alternate in account.get('alternateIds') or []:
                ids.append(str(alternate.get('profileId')))
                ids.append(str(alternate.get('entityId')))
            if [x for x in ids if x and x in wanted]:
                account_id = str(account.get('advertiserAccountId'))
                logging.info('Resolved v1 advertiser account {} for {}'
                             ''.format(account_id, self.advertiser_id))
                return account_id
        logging.warning('Could not match a v1 advertiser account to {}, '
                        'requesting on the scope header alone.'
                        ''.format(self.advertiser_id))
        return ''

    def get_v1_fields(self):
        """
        Assembles the field list for a v1 report request.

        :returns: list of v1 field names
        """
        fields = list(self.v1_dimension_fields)
        fields += self.v1_ad_group_fields
        fields += self.v1_metric_fields
        if self.amazon_dsp:
            fields += self.v1_dsp_metric_fields
        return fields

    def get_v1_report_body(self, sd, ed):
        """
        Builds the body for a v1 create report call.

        The campaign filter is deliberately left off: v1 exposes a
        query filter but its shape is not published, and this class
        already narrows on campaign name once the frame is in hand.

        :param sd: start date as a yyyy-mm-dd string
        :param ed: end date as a yyyy-mm-dd string
        :returns: request body dict
        """
        report = {'format': self.v1_format,
                  'periods': [{'datePeriod': {'startDate': sd,
                                              'endDate': ed}}],
                  'query': {'fields': self.get_v1_fields()}}
        body = {'reports': [report]}
        if self.v1_account_id:
            body['accessRequestedAccounts'] = [
                {'advertiserAccountId': str(self.v1_account_id)}]
        return body

    @staticmethod
    def parse_v1_reports(response_json):
        """
        Pulls the report objects out of a v1 multi status envelope.

        v1 answers create and retrieve with a 207 and per index success
        and error entries, so a caller cannot read a report id off the
        top level the way v3 allowed.

        :param response_json: decoded body of a v1 report call
        :returns: list of report dicts and list of error entries
        """
        reports = []
        if not isinstance(response_json, dict):
            return reports, [response_json]
        for entry in response_json.get('success') or []:
            report = entry.get('report')
            if report:
                reports.append(report)
        error = response_json.get('error')
        if isinstance(error, list):
            errors = [x for x in error if x]
        elif error:
            errors = [error]
        else:
            errors = []
        return reports, errors

    def request_v1_report(self, sd, ed):
        """
        Requests a v1 report and returns the ids it was given.

        :param sd: start date as a yyyy-mm-dd string
        :param ed: end date as a yyyy-mm-dd string
        :returns: list of report ids
        """
        url = '{}{}'.format(self.base_url, self.v1_report_path)
        is_dsp = ' DSP ' if self.amazon_dsp else ' Sponsored '
        logging.info('Requesting v1{}report for dates: {} to {}'
                     ''.format(is_dsp, sd, ed))
        r = self.make_request(url, method='POST',
                              body=self.get_v1_report_body(sd, ed),
                              headers=self.set_v1_headers(),
                              json_response=False)
        reports, errors = self.parse_v1_reports(r.json())
        if errors:
            logging.warning('v1 report request errors: {}'.format(errors))
        report_ids = [x['reportId'] for x in reports if x.get('reportId')]
        if not report_ids:
            logging.warning('No v1 report id in response: {}'
                            ''.format(r.json()))
        return report_ids

    def get_v1_download_urls(self, report):
        """
        Finds the download locations on a completed v1 report.

        Amazon's published examples carry completedReportParts as null,
        so the key holding the url is not pinned down.  Every plausible
        key is checked and the report object is logged when none match,
        rather than quietly reporting no data.

        :param report: a v1 report dict
        :returns: list of download urls
        """
        urls = []
        parts = report.get('completedReportParts') or []
        if isinstance(parts, dict):
            parts = [parts]
        for part in parts:
            if isinstance(part, str):
                urls.append(part)
                continue
            for key in self.v1_url_keys:
                if isinstance(part, dict) and part.get(key):
                    urls.append(part[key])
                    break
        if not urls:
            for key in self.v1_url_keys:
                if report.get(key):
                    urls.append(report[key])
                    break
        return urls

    @staticmethod
    def v1_download_to_df(url, report_format):
        """
        Downloads one v1 report part into a dataframe.

        The format is read off the report rather than assumed, and the
        gzip magic number is checked because a CSV part may arrive
        compressed and pandas cannot infer that from a buffer.

        :param url: the download url
        :param report_format: format reported by v1, e.g. CSV
        :returns: dataframe
        """
        r = requests.get(url)
        content = r.content
        compression = 'gzip' if content[:2] == b'\x1f\x8b' else None
        if 'CSV' in str(report_format).upper():
            return pd.read_csv(io.BytesIO(content), compression=compression)
        return pd.read_json(io.BytesIO(content), compression=compression)

    def check_v1_report_status(self, report_id, attempts=150, wait=30):
        """
        Polls one v1 report and returns it as a dataframe.

        A download url appearing is the terminal signal; the status
        strings only short circuit the wait, because the published
        status enum is not documented.

        :param report_id: the v1 report id to poll
        :param attempts: how many times to poll before giving up
        :param wait: seconds between polls
        :returns: dataframe, empty when the report never completed
        """
        url = '{}{}'.format(self.base_url, self.v1_retrieve_path)
        body = {'reportIds': [report_id]}
        df = pd.DataFrame()
        for attempt in range(attempts):
            logging.info('Checking for v1 report {} attempt {}'
                         ''.format(report_id, attempt + 1))
            r = self.make_request(url, method='POST', body=body,
                                  headers=self.set_v1_headers(),
                                  json_response=False)
            reports, errors = self.parse_v1_reports(r.json())
            if errors:
                logging.warning('v1 report {} error: {}'
                                ''.format(report_id, errors))
            if not reports:
                time.sleep(wait)
                continue
            report = reports[0]
            status = str(report.get('status', '')).upper()
            urls = self.get_v1_download_urls(report)
            if urls:
                df_list = [self.v1_download_to_df(x, report.get('format'))
                           for x in urls]
                df = self.merge_dataframes(df_list)
                if df.empty:
                    logging.warning('v1 report {} downloaded empty, likely '
                                    'no data.'.format(report_id))
                break
            if status in self.v1_failed_status:
                logging.warning('v1 report {} failed: {} {}'.format(
                    report_id, report.get('failureCode'),
                    report.get('failureReason')))
                break
            if status in self.v1_complete_status:
                logging.warning('v1 report {} is complete but carries no '
                                'download location: {}'
                                ''.format(report_id, report))
                break
            time.sleep(wait)
        return df

    def rename_v1_columns(self, df):
        """
        Maps v1 field names onto the legacy report column names.

        Only columns the frame actually has are renamed, so adding a
        field to the request cannot rename a column that is not there.

        :param df: dataframe as downloaded from v1
        :returns: dataframe with legacy column names
        """
        if df.empty:
            return df
        renames = self.v1_column_renames.copy()
        if self.amazon_dsp:
            renames.update(self.v1_dsp_column_renames)
        renames = {k: v for k, v in renames.items() if k in df.columns}
        return df.rename(columns=renames)

    def get_v1_data(self, sd, ed):
        """
        Runs the whole v1 report flow for one date range.

        v1 accepts a date range on one report, so unlike the v3 flow
        this does not fan out a request per day.

        :param sd: start date as a datetime
        :param ed: end date as a datetime
        :returns: dataframe with legacy column names
        """
        sd_str = dt.datetime.strftime(sd, '%Y-%m-%d')
        ed_str = dt.datetime.strftime(ed, '%Y-%m-%d')
        if not self.v1_account_id:
            self.v1_account_id = self.get_v1_account_id()
        report_ids = self.request_v1_report(sd_str, ed_str)
        if not report_ids:
            return pd.DataFrame()
        df_list = [self.check_v1_report_status(x) for x in report_ids]
        df = self.merge_dataframes(df_list)
        return self.rename_v1_columns(df)

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
        self.export_id = ''
        self.campaign_export_id = ''
        self.v1_account_id = ''
        profile_found = self.get_profiles()
        if not profile_found:
            return self.df
        sd, ed = self.get_data_default_check(sd, ed, fields)
        if self.use_v1 and self.check_v1_supported():
            self.df = self.get_v1_data(sd, ed)
            if not self.df.empty:
                self.df = self.filter_df_on_campaign(self.df)
                return self.df
            logging.warning('Ads API v1 returned no rows, falling back to '
                            'the v3 report flow.')
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
        if self.product_report and not self.amazon_dsp:
            self.df = self.get_product_name_df(self.df)
        if self.product_report:
            self.df = self.apply_categorization(self.df)
        return self.df

    def get_categorization_keywords(self):
        """
        :return: list of keyword strings from google sheet
        """
        df = gsapi.GsApi().get_simple_df(sheet_id=self.product_sheet_id)
        keywords = df.values.tolist()
        keywords = [x for sublist in keywords for x in sublist]
        return keywords

    def apply_categorization(self, df):
        """
        Categorizes products as Game or Non-Game based on presence of keywords
        in product name
        :return: dataframe with new column for category
        """
        keywords = self.get_categorization_keywords()
        if self.amazon_dsp:
            df['myProductCategory'] = df['productName'].apply(
                lambda x: self.classify_product(x, keywords))
        else:
            df['advertisedCategory'] = df['advertised_title'].apply(
                lambda x: self.classify_product(x, keywords))
            df['purchasedCategory'] = df['purchased_title'].apply(
                lambda x: self.classify_product(x, keywords))
        return df

    @staticmethod
    def classify_product(name, non_game_keywords):
        """
        Classifies a product as Game or Non-Game based on presence of keywords
        """
        category = 'Game'
        if pd.isna(name):
            return category
        name_lower = name.lower()
        if any(keyword in name_lower for keyword in non_game_keywords):
            category = 'Non-Game'
        return category

    def set_product_body(self, body):
        """
        Create request body for product report which includes ASIN-level data
        and product
        """
        if self.amazon_dsp:
            body.update({
                "configuration": {
                    'adProduct': 'DEMAND_SIDE_PLATFORM',
                    'columns': ['date', 'productName', 'marketplace',
                                'asin', 'featuredAsin', 'totalNewToBrandDPVs',
                                'totalPurchases', 'lineItemName', 'lineItemId',
                                'productCategory'],
                    'reportTypeId': 'dspProduct',
                    'filters': [{
                        'field': 'advertiserId',
                        'values': [self.advertiser_id]
                    }],
                    'format': 'GZIP_JSON',
                    'groupBy': ['lineItem'],
                    'timeUnit': 'DAILY'
                }
            })
        else:
            body.update({
                "configuration": {
                    'adProduct': 'SPONSORED_PRODUCTS',
                    'columns': ['purchasedAsin', 'advertisedAsin',
                                'campaignId', 'date', 'adGroupId', 'sales14d',
                                'unitsSoldClicks14d', 'purchases14d',
                                'adGroupName', 'campaignName'],
                    'reportTypeId': 'spPurchasedProduct',
                    'format': 'GZIP_JSON',
                    'groupBy': ['asin'],
                    'timeUnit': 'DAILY'
                }
            })
        return body

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
                        'totalROAS14d', 'sales14d', 'purchases14d',
                        'offAmazonPurchases14d', 'subscribe14d'],
            "type": "CAMPAIGN",
            "dimensions": ["ORDER", "LINE_ITEM", "CREATIVE"],
            "timeUnit": "DAILY"
        })
        return body

    def get_sponsored_body(self, body, ad_product, cols, report_type,
                           group_by_ad_group=False, group_by_column=None):
        body['configuration']['adProduct'] = ad_product
        body['configuration']['columns'] = self.sponsored_columns + cols
        body['configuration']['reportTypeId'] = report_type
        if self.include_keywords and not self.amazon_dsp:
            group_by = [group_by_column]
        else:
            group_by = ['campaign']
            if group_by_ad_group:
                group_by += ['adGroup']
        body['configuration']['groupBy'] = group_by
        return body

    def get_sponsored_bodies(self, body):
        body['configuration'] = {
            'timeUnit': 'DAILY',
            'format': 'GZIP_JSON'}
        if self.include_keywords and not self.amazon_dsp:
            rep_items = [
                ('SPONSORED_PRODUCTS', self.sp_keyword_columns +
                 self.sp_columns, 'spSearchTerm', False, 'searchTerm'),
                ('SPONSORED_BRANDS', self.sb_keyword_columns,
                 'sbSearchTerm', False, 'searchTerm')]
        else:
            rep_items = [
                ('SPONSORED_PRODUCTS', self.sp_columns, 'spCampaigns',
                 True, None),
                ('SPONSORED_BRANDS', self.sb_columns, 'sbCampaigns',
                 False, None)]
        request_bodies = []
        for (ad_product, cols, report_type, group_by_ad_group,
             group_by_column) in rep_items:
            body_copy = copy.deepcopy(body)
            request_body = self.get_sponsored_body(
                body_copy, ad_product, cols, report_type, group_by_ad_group,
                group_by_column)
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
        amz_type = 'sp'
        report_type = 'normal'
        if self.amazon_dsp:
            amz_type = 'dsp'
        if self.product_report:
            report_type = 'product'
        cache_key = '{}_{}_{}_{}_{}'.format(amz_type, report_type,
                                         self.advertiser_id, sd, ed)
        if cache_key in self.report_cache:
            logging.info('reusing cached report IDs for {}'.format(cache_key))
            report_ids = self.report_cache[cache_key]['report_ids']
            if not self.export_id and not self.amazon_dsp:
                self.export_id = self.request_export()
                self.campaign_export_id = self.request_export('campaigns')
            return report_ids
        is_dsp = ' DSP ' if self.amazon_dsp else ' Sponsored Product/Brand '
        msg = 'Requesting{}report for dates: {} to {}'.format(is_dsp, sd, ed)
        logging.info(msg)
        base_body = {"endDate": ed, "startDate": sd}
        if self.product_report:
            url = '{}/reporting/reports'.format(self.base_url)
            prod_body = self.set_product_body(base_body)
            request_bodies = [prod_body]
            if self.amazon_dsp:
                self.headers['Amazon-Advertising-API-Scope'] = self.dsp_id
        elif self.amazon_dsp:
            sp_body = self.get_dsp_request_body(base_body)
            header = 'application/vnd.dspcreatereports.v3+json'
            url = '{}/accounts/{}/dsp/reports'.format(
                self.base_url, self.profile_id)
            request_bodies = [sp_body]
            self.headers['Accept'] = header
        else:
            header = 'application/vnd.createasyncreportrequest.v3+json'
            url = '{}/reporting/reports'.format(self.base_url)
            request_bodies = self.get_sponsored_bodies(base_body)
            self.headers['Accept'] = header
            if not self.export_id:
                self.export_id = self.request_export()
                self.campaign_export_id = self.request_export('campaigns')
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
                previous_merge_col = '{}_x'.format(id_col)
                if previous_merge_col in self.df.columns:
                    y_previous_merge_col = '{}_y'.format(id_col)
                    self.df[id_col] = (
                        self.df[previous_merge_col]
                        .fillna(self.df[y_previous_merge_col])
                    )
                    drop_cols = [previous_merge_col, y_previous_merge_col]
                    self.df.drop(columns=drop_cols, inplace=True)
                id_df = self.check_and_get_export(export_id, entity=entity)
                self.df = self.df.merge(id_df, on=id_col, how='left')
            if self.product_report:
                self.df['adGroupName'] = self.df['adGroupName_x']
                self.df['campaignName'] = self.df['campaignName_x']
                self.df = self.df.drop(columns=[
                    'adGroupName_x', 'adGroupName_y',
                    'campaignName_x', 'campaignName_y'
                ])
        return self.df

    @staticmethod
    def get_product_name_from_asin(asin):
        if pd.isna(asin):
            return None
        url = f'https://www.amazon.com/gp/product/{asin}'
        product_name = ''
        try:
            r = requests.get(url)
            if r.status_code != 200:
                return None
            html_text = r.text
            match = re.search(r'<span[^>]*id=["\']productTitle["\'][^>]*>(.*?)'
                              r'</span>', html_text, re.DOTALL)
            if match:
                product_name = html.unescape(match.group(1).strip())
        except Exception as e:
            logging.warning(f'Error for ASIN {asin}: {e}')
        logging.info(f'ASIN: {asin} - Product Name: {product_name}')
        return product_name

    def get_product_name_df(self, df):
        unique_asins = pd.concat([df['advertisedAsin'],
                                  df['purchasedAsin']]).dropna().unique()
        product_list = {}
        if os.path.exists(self.asin_path):
            existing_df = pd.read_csv(self.asin_path)
        else:
            existing_df = pd.DataFrame(columns=['asin', 'productName'])
        existing_asin = dict(zip(existing_df['asin'], existing_df['productName']))
        for asin in unique_asins:
            if asin in existing_asin:
                product_list[asin] = existing_asin[asin]
                logging.info(f'asin already found: {asin}-{product_list[asin]}')
                continue
            product_name = self.get_product_name_from_asin(asin)
            if product_name:
                product_list[asin] = product_name
        asin_df = self.asin_csv(product_list)
        asin_lookup = dict(zip(asin_df['asin'], asin_df['productName']))
        df['advertised_title'] = df['advertisedAsin'].map(asin_lookup)
        df['purchased_title'] = df['purchasedAsin'].map(asin_lookup)
        return df

    def asin_csv(self, product_list):
        if os.path.exists(self.asin_path):
            existing_df = pd.read_csv(self.asin_path)
        else:
            existing_df = pd.DataFrame(columns=['asin', 'productName'])
        new_df = pd.DataFrame(
            list(product_list.items()),
            columns=['asin', 'productName']
        )
        combined = pd.concat([existing_df, new_df])
        combined['productName'] = combined['productName'].astype(
            str).str.strip()
        combined = combined[
            combined['productName'].notna() &
            (combined['productName'] != '') &
            (combined['productName'].str.lower() != 'nan')
            ]
        combined = combined.drop_duplicates(subset=['asin'], keep='last')
        combined.to_csv(self.asin_path, index=False)
        return combined

    def check_report_status(self, report_id, attempts, wait):
        complete_status = 'COMPLETED'
        url_key = 'url'
        if self.product_report:
            url = '{}/reporting/reports/{}'.format(self.base_url, report_id)
        elif self.amazon_dsp:
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
                    if self.amazon_dsp and not self.product_report:
                        df = pd.DataFrame(r.json())
                    else:
                        df = pd.read_json(
                            io.BytesIO(r.content), compression='gzip'
                        )
                    if df.empty:
                        logging.warning('Dataframe empty, likely no data  - '
                                        'returning empty dataframe')
                    else:
                        if (self.amazon_dsp and 'date' in df.columns and
                                not self.product_report):
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
        if self.amazon_dsp and self.product_report:
            self.headers['Amazon-Advertising-API-Scope'] = self.dsp_id
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
            # An empty dict is a valid v1 query body and has to survive.
            if kwarg[0] is not None:
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
