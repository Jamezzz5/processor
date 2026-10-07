import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path
base_url = 'https://doubleclickbidmanager.googleapis.com/v2'


class DbApi(object):
    no_reach_field = 'No Reach'
    api_field_options = (
        ('YOUTUBE', 'YouTube groupings and metrics'),
        (no_reach_field,
         'Skip the whole range campaign, insertion order and line item '
         'reach and the daily advertiser reach'))
    campaign_groups = ['FILTER_MEDIA_PLAN', 'FILTER_MEDIA_PLAN_NAME']
    campaign_col = 'Campaign'
    campaign_id_col = 'Campaign ID'
    advertiser_id_col = 'Advertiser ID'
    date_col = 'Date'
    base_groups = [
        'FILTER_ADVERTISER', 'FILTER_ADVERTISER_NAME',
        'FILTER_ADVERTISER_CURRENCY',
        'FILTER_INSERTION_ORDER', 'FILTER_INSERTION_ORDER_NAME',
        'FILTER_LINE_ITEM', 'FILTER_LINE_ITEM_NAME',
        'FILTER_DATE',
        'FILTER_LINE_ITEM_TYPE',
        'FILTER_MEDIA_PLAN', 'FILTER_MEDIA_PLAN_NAME',
        'FILTER_CREATIVE_ID', 'FILTER_CREATIVE']
    youtube_groups = [
        'FILTER_DATE',
        'FILTER_TRUEVIEW_AD_GROUP',
        'FILTER_TRUEVIEW_AD',
        'FILTER_TRUEVIEW_AD_GROUP_ID']
    base_metrics = [
        'METRIC_IMPRESSIONS', 'METRIC_BILLABLE_IMPRESSIONS', 'METRIC_CLICKS',
        'METRIC_CTR', 'METRIC_TOTAL_CONVERSIONS', 'METRIC_LAST_CLICKS',
        'METRIC_LAST_IMPRESSIONS', 'METRIC_REVENUE_ADVERTISER',
        'METRIC_MEDIA_COST_ADVERTISER', 'METRIC_BILLABLE_COST_ADVERTISER',
        'METRIC_TOTAL_MEDIA_COST_ADVERTISER', 'METRIC_FEE16_ADVERTISER',
        'METRIC_RICH_MEDIA_VIDEO_FIRST_QUARTILE_COMPLETES',
        'METRIC_RICH_MEDIA_VIDEO_MIDPOINTS',
        'METRIC_RICH_MEDIA_VIDEO_THIRD_QUARTILE_COMPLETES',
        'METRIC_RICH_MEDIA_VIDEO_COMPLETIONS', 'METRIC_RICH_MEDIA_VIDEO_PLAYS']
    youtube_metrics = [
        'METRIC_TRUEVIEW_VIEWS',
        'METRIC_CLICKS',
        'METRIC_IMPRESSIONS',
        'METRIC_REVENUE_USD',
        'METRIC_RICH_MEDIA_VIDEO_FIRST_QUARTILE_COMPLETES',
        'METRIC_RICH_MEDIA_VIDEO_MIDPOINTS',
        'METRIC_RICH_MEDIA_VIDEO_THIRD_QUARTILE_COMPLETES',
        'METRIC_RICH_MEDIA_VIDEO_COMPLETIONS']
    view_metrics = [
        'METRIC_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS',
        'METRIC_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS',
        'METRIC_ACTIVE_VIEW_UNVIEWABLE_IMPRESSIONS']
    reach_query_type = 'REACH'
    reach_max_days = 93
    reach_metrics = {
        'METRIC_UNIQUE_REACH_IMPRESSION_REACH':
            'Unique Reach: Impression Reach',
        'METRIC_UNIQUE_REACH_AVERAGE_IMPRESSION_FREQUENCY':
            'Unique Reach: Average Impression Frequency',
        'METRIC_UNIQUE_REACH_CLICK_REACH': 'Unique Reach: Click Reach',
        'METRIC_UNIQUE_REACH_TOTAL_REACH': 'Unique Reach: Total Reach',
        'METRIC_UNIQUE_REACH_VIEWABLE_IMPRESSION_REACH':
            'Unique Reach: Viewable Impression Reach',
        'METRIC_UNIQUE_REACH_AVERAGE_VIEWABLE_IMPRESSION_FREQUENCY':
            'Unique Reach: Average Viewable Impression Frequency'}
    reach_groups = ['FILTER_ADVERTISER', 'FILTER_ADVERTISER_NAME']
    insertion_order_groups = ['FILTER_INSERTION_ORDER',
                              'FILTER_INSERTION_ORDER_NAME']
    line_item_groups = ['FILTER_LINE_ITEM', 'FILTER_LINE_ITEM_NAME']
    reach_reports = [
        {'groups': campaign_groups,
         'suffix': ' - no_date - campaign_report'},
        {'groups': campaign_groups + insertion_order_groups,
         'suffix': ' - no_date - insertion_order_report'},
        {'groups': (campaign_groups + insertion_order_groups +
                    line_item_groups),
         'suffix': ' - no_date - line_item_report'},
        {'groups': ['FILTER_DATE'], 'suffix': ' - vendor_report',
         'daily': True}]
    default_groups = base_groups
    default_metrics = base_metrics

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.advertiser_id = None
        self.campaign_id = None
        self.campaign_ids = []
        self.campaign_name_filter = []
        self.query_id = None
        self.report_id = None
        self.config_list = None
        self.client = None
        self.start_time = None
        self.end_time = None
        self.query_type = 'STANDARD'
        self.df = pd.DataFrame()
        self.r = None
        self.v = 1

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            return False
        logging.info('Loading DB config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()
        return True

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
        self.refresh_url = self.config['refresh_url']
        self.report_id = self.config['report_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url]
        if 'advertiser_id' in self.config:
            self.advertiser_id = self.config['advertiser_id']
        if 'campaign_id' in self.config:
            self.campaign_id = self.config['campaign_id']

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in DB config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    @staticmethod
    def get_data_default_check(sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        return sd, ed, fields

    def parse_fields(self, sd, ed, fields):
        """
        Sets the report window, groupings and metrics from the api fields.

        The lists are rebuilt from the class level templates on each call so
        a second api object does not inherit the last one's groupings.  The
        query type is reset too, since one api object serves every DV360
        card in an import and a reach pull leaves it on REACH.

        :param sd: The start date to pull from
        :param ed: The end date to pull to
        :param fields: The API_FIELDS values from the vendor matrix
        :return:
        """
        self.start_time = round((sd - dt.datetime.utcfromtimestamp(0))
                                .total_seconds() * 1000)
        self.end_time = round((ed - dt.datetime.utcfromtimestamp(0))
                              .total_seconds() * 1000)
        groups = list(self.base_groups)
        metrics = list(self.base_metrics)
        self.query_type = 'STANDARD'
        if fields and 'YOUTUBE' in fields:
            groups = list(self.youtube_groups) + list(self.campaign_groups)
            metrics = list(self.youtube_metrics)
            self.query_type = 'YOUTUBE'
        report_fields = [x for x in (fields or [])
                         if x not in ('nan', self.no_reach_field)]
        if report_fields:
            metrics += list(self.view_metrics)
        self.default_groups = groups
        self.default_metrics = metrics

    def parse_campaign_filter(self):
        """
        Splits the campaign filter into ids to send and values to match.

        FILTER_MEDIA_PLAN only accepts DV360 campaign ids, so an all numeric
        filter is also sent to the api to keep the report small.  It is kept
        for the post download match as well, since a numeric filter is as
        often the DCM campaign id the DV360 campaign is named for as it is
        the DV360 campaign's own id.

        :return: The list of campaign ids for the api
        """
        self.campaign_ids, self.campaign_name_filter = (
            utl.parse_campaign_filter(self.campaign_id))
        return self.campaign_ids

    def filter_df_on_campaign(self, keep_on_no_match=True):
        """
        Filters the downloaded report down to the campaigns filtered on.
        The advertiser's daily reach rows name no campaign, so they are
        kept.

        :param keep_on_no_match: Whether a filter that matches nothing keeps
            the unfiltered df
        :return: The filtered dataframe
        """
        self.df = utl.filter_df_on_campaign(
            self.df, self.campaign_name_filter, self.campaign_col,
            self.campaign_id_col, keep_on_no_match=keep_on_no_match,
            keep_cols=self.daily_reach_cols)
        return self.df

    def refresh_client_token(self, extra):
        token = None
        for x in range(10):
            try:
                token = self.client.refresh_token(self.refresh_url, **extra)
                break
            except requests.exceptions.ConnectionError as e:
                logging.warning('Connection error, retrying: {}'.format(e))
                time.sleep(1)
        return token

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.refresh_client_token(extra)
        self.client = OAuth2Session(self.client_id, token=token)

    @staticmethod
    def create_query_url():
        query_url = '{}/queries'.format(base_url)
        return query_url

    def create_run_url(self):
        query_url = self.create_query_url()
        full_url = '{}/{}:run'.format(query_url, self.query_id)
        return full_url

    def create_url(self):
        query_url = self.create_query_url()
        full_url = '{}/{}/reports/{}'.format(query_url, self.query_id,
                                             self.report_id)
        return full_url

    def get_data(self, sd=None, ed=None, fields=None):
        """
        Pulls the report, retrying once when a campaign id filter empties
        it, then adds the whole range and daily reach rows under it.

        :param sd: The start date to pull from
        :param ed: The end date to pull to
        :param fields: The API_FIELDS values from the vendor matrix
        :return: The report as a dataframe
        """
        self.parse_campaign_filter()
        self.get_report_df(sd, ed, fields)
        retried = bool(self.df.empty and self.campaign_ids and self.query_id)
        if retried:
            logging.warning(
                'No data for campaign ids {}, which may name the campaigns '
                'rather than identify them.  Retrying without the api '
                'filter.'.format(self.campaign_ids))
            self.campaign_ids = []
            self.query_id = None
            self.report_id = None
            self.get_report_df(sd, ed, fields)
        df = self.df
        if self.pull_reach(fields):
            df = pd.concat([df, self.request_reach(sd, ed)],
                           ignore_index=True)
        self.df = df
        self.filter_df_on_campaign(keep_on_no_match=not retried)
        return self.df

    def get_report_df(self, sd, ed, fields):
        """
        Creates, runs and downloads a single report.

        :param sd: The start date to pull from
        :param ed: The end date to pull to
        :param fields: The API_FIELDS values from the vendor matrix
        :return: The downloaded dataframe, empty when the report failed
        """
        self.df = pd.DataFrame()
        report_created = self.create_report(sd, ed, fields)
        if not report_created:
            logging.warning('Report was not created, check for errors.')
            return self.df
        self.download_report()
        self.remove_footer()
        return self.df

    def download_report(self):
        """
        Runs the created query and downloads the report it produces.

        :return: The downloaded dataframe, empty when the run or the
            download failed
        """
        self.df = pd.DataFrame()
        if not self.run_report():
            logging.warning('Report did not run, check for errors.')
            return self.df
        self.get_raw_data()
        self.check_empty_df()
        return self.df

    def check_empty_df(self):
        if self.df.empty:
            return
        if self.df.iloc[0, 0] == 'No data returned by the reporting service.':
            logging.warning('No data in response, returning empty df.')
            self.df = pd.DataFrame()

    def remove_footer(self):
        self.df = self.df[~self.df.isnull().any(axis=1)]

    def create_report(self, sd, ed, fields):
        """
        Creates the query for the report, retrying without the campaign
        grouping when the report type rejects it.

        DV360 does not document which groupings each report type accepts, so
        a rejected query is retried without the campaign columns rather than
        failing the pull outright.

        :param sd: The start date to pull from
        :param ed: The end date to pull to
        :param fields: The API_FIELDS values from the vendor matrix
        :return: Boolean of whether the query was created
        """
        if self.report_id:
            return True
        logging.info('No report specified, creating.')
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        self.parse_fields(sd, ed, fields)
        metadata = self.create_report_metadata(sd, ed)
        created = self.request_query(metadata)
        if not created and self.remove_campaign_groups():
            logging.warning('Retrying query creation without the campaign '
                            'grouping, campaign name will not be available.')
            created = self.request_query(metadata)
        return created

    def remove_campaign_groups(self):
        """
        Drops the campaign groupings from the query.

        :return: Boolean of whether any grouping was removed
        """
        groups = [x for x in self.default_groups
                  if x not in self.campaign_groups]
        if len(groups) == len(self.default_groups):
            return False
        self.default_groups = groups
        return True

    def request_query(self, metadata):
        """
        Posts the query to the api and stores the resulting query id.

        :param metadata: The report metadata as a dict
        :return: Boolean of whether the query was created
        """
        query_url = self.create_query_url()
        params = self.create_report_params()
        body = {
            'metadata': metadata,
            'params': params,
            'schedule': {
                'frequency': 'ONE_TIME'},
        }
        self.r = self.make_request(query_url, method='post', body=body)
        if 'queryId' not in self.r.json():
            logging.warning('queryId not in response:{}'.format(self.r.json()))
            return False
        self.query_id = self.r.json()['queryId']
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f)
        logging.info('Query created -- ID: {}.'.format(self.query_id))
        return True

    def make_request(self, url, method, body=None):
        self.get_client()
        try:
            self.r = self.raw_request(url, method, body=body)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url, method, body=body)
        return self.r

    def raw_request(self, url, method, body=None):
        if method == 'get':
            if body:
                self.r = self.client.get(url, json=body)
            else:
                self.r = self.client.get(url)
        elif method == 'post':
            if body:
                self.r = self.client.post(url, json=body)
            else:
                self.r = self.client.post(url)
        return self.r

    def run_report(self):
        """
        Runs the query, waiting out rate limits but not a rejection.

        :return: Boolean of whether the api started a report
        """
        run_url = self.create_run_url()
        for x in range(1, 101):
            self.r = self.make_request(run_url, method='post')
            response = self.r.json()
            if 'metadata' in response:
                self.report_id = response['key']['reportId']
                return True
            if self.request_rejected(response):
                return False
            logging.warning('Rate limit exceeded. Pausing. '
                            'Response: {}'.format(response))
            time.sleep(60)
        return False

    @staticmethod
    def request_rejected(response):
        """
        Whether a response is an error no retry will fix, so a query the
        api rejects does not hold the import for the whole retry budget.

        :param response: The decoded json response
        :return: Boolean of whether the request was rejected
        """
        error = response.get('error') if isinstance(response, dict) else None
        if not error or error.get('code') not in (400, 404):
            return False
        logging.warning('Request rejected: {}'.format(error))
        return True

    def report_failed(self, response):
        """
        Whether a report will never produce a file to download.

        :param response: The decoded json response for the report
        :return: Boolean of whether the report failed
        """
        if self.request_rejected(response):
            return True
        if not isinstance(response, dict):
            return False
        metadata = response.get('metadata') or {}
        status = metadata.get('status') or {}
        if status.get('state') != 'FAILED':
            return False
        logging.warning('Report failed: {}'.format(metadata))
        return True

    def get_raw_data(self):
        for x in range(1, 101):
            full_url = self.create_url()
            self.r = self.make_request(full_url, method='get')
            response = self.r.json()
            if (response and 'metadata' in response
                    and 'googleCloudStoragePath' in response['metadata']):
                report_url = response['metadata']['googleCloudStoragePath']
                logging.info('Found report url, downloading.')
                self.df = utl.import_read_csv(report_url, file_check=False,
                                              error_bad='warn')
                return self.df
            if self.report_failed(response):
                return self.df
            logging.info('Report unavailable.  Attempt {}.  '
                         'Response: {}'.format(x, self.r.json()))
            time.sleep(15)

    def create_report_params(self):
        params = {
            'filters': [{'type': 'FILTER_ADVERTISER',
                         'value': self.advertiser_id}],
            'groupBys': self.default_groups,
            'metrics': self.default_metrics,
            'type': self.query_type}
        if self.campaign_ids:
            campaign_filters = [
                {'type': 'FILTER_MEDIA_PLAN',
                 'value': x} for x in self.campaign_ids]
            params['filters'].extend(campaign_filters)
        return params

    def create_report_metadata(self, sd, ed, name='report'):
        """
        Builds the query metadata: a custom date range, csv output and a
        title naming the advertiser, the campaign filter and the report.

        :param sd: The start date to pull from
        :param ed: The end date to pull to
        :param name: The report's name, telling a reach report's title
            apart from the daily report's
        :return: The metadata dict
        """
        report_name = '{}_{}_{}'.format(
            self.advertiser_id, self.campaign_id, name)
        metadata = {
            'dataRange': {
                "range": "CUSTOM_DATES",
                "customStartDate": {
                    "year": sd.year,
                    "month": sd.month,
                    "day": sd.day
                },
                "customEndDate": {
                    "year": ed.year,
                    "month": ed.month,
                    "day": ed.day
                }
            },
            'format': 'CSV',
            'title': report_name}
        return metadata

    def pull_reach(self, fields):
        """
        Whether the card takes the whole range reach rows.

        :param fields: The API_FIELDS values from the vendor matrix
        :return: Boolean, False when the card carries No Reach
        """
        return self.no_reach_field not in (fields or [])

    @property
    def reach_cols(self):
        """
        Every column a reach row fills, so importhandler can find the reach
        rows a merged raw file already holds and replace them.

        :return: List of suffixed reach column names
        """
        return ['{}{}'.format(name, report['suffix'])
                for report in self.reach_reports
                for name in self.reach_metrics.values()]

    @property
    def daily_reach_cols(self):
        """
        The columns only an advertiser's daily reach row fills, which the
        campaign filter has to leave alone.

        :return: List of suffixed reach column names
        """
        return ['{}{}'.format(name, report['suffix'])
                for report in self.reach_reports if report.get('daily')
                for name in self.reach_metrics.values()]

    def get_reach(self, sd=None, ed=None, fields=None):
        """
        Only the reach rows, the whole range rows and the daily advertiser
        rows, for a caller whose delivery range is shorter than the range
        reach has to cover: importhandler trims a merge card's delivery to
        its merge window but pulls reach from the card's start date.

        :param sd: Start of the reach range
        :param ed: End of the reach range
        :param fields: The card's api fields, read only for No Reach
        :return: df of reach rows, their measures suffixed per level
        """
        if not self.pull_reach(fields):
            return pd.DataFrame()
        self.parse_campaign_filter()
        self.df = self.request_reach(sd, ed)
        return self.filter_df_on_campaign()

    def request_reach(self, sd, ed):
        """
        Pulls one row per campaign, insertion order and line item for the
        whole range, so DV360 counts each person once across it, and one
        row per day for the advertiser.  The daily STANDARD report cannot
        carry these metrics, which is why each level is a REACH query of
        its own.

        :param sd: Start of the range
        :param ed: End of the range
        :return: df of reach rows, their measures suffixed per level
        """
        sd, ed, _ = self.get_data_default_check(sd, ed, None)
        df = pd.DataFrame()
        for report in self.reach_reports:
            tdf = self.request_reach_report(sd, ed, report)
            df = pd.concat([df, tdf], ignore_index=True)
        return df

    def request_reach_report(self, sd, ed, report):
        """
        Pulls one level's reach, in as many queries as DV360 answers.

        DV360 only deduplicates reach within 93 days and two blocks cannot
        be added back together, so a longer range skips a whole range
        report; the daily advertiser rows each cover one day, so they are
        pulled in 93 day blocks whatever the range.  DV360 narrows the
        advertiser's reach to the card's campaigns only when the filter
        gives their ids, so a filter of names gets no daily advertiser
        reach rather than the whole advertiser's.

        :param sd: Start of the range
        :param ed: End of the range
        :param report: The reach_reports entry to pull
        :return: df of the level's rows, their measures suffixed
        """
        days = (ed - sd).days + 1
        if report.get('daily'):
            if self.campaign_name_filter and not self.campaign_ids:
                logging.warning(
                    'Skipping daily advertiser reach: DV360 cannot narrow '
                    'it to the campaign filter {}, which names campaigns '
                    'rather than giving their ids.'.format(
                        self.campaign_name_filter))
                return pd.DataFrame()
            blocks = utl.date_blocks(sd, ed, self.reach_max_days)
        elif days > self.reach_max_days:
            logging.warning(
                'DV360 only counts people once within {} days, skipping '
                'whole range reach for the {} day range.'.format(
                    self.reach_max_days, days))
            return pd.DataFrame()
        else:
            blocks = [(sd, ed)]
        df = pd.DataFrame()
        for block_sd, block_ed in blocks:
            tdf = self.get_reach_report_df(block_sd, block_ed, report)
            df = pd.concat([df, tdf], ignore_index=True)
        return df

    def get_reach_report_df(self, sd, ed, report):
        """
        Creates, runs and downloads one reach report.

        A rejected query is skipped rather than retried without its
        groupings, since the level is the point of the report.

        :param sd: Start of the range
        :param ed: End of the range
        :param report: The reach_reports entry to pull
        :return: The report's rows, empty when it failed
        """
        self.query_id = None
        self.report_id = None
        self.query_type = self.reach_query_type
        self.default_groups = list(self.reach_groups) + list(report['groups'])
        self.default_metrics = list(self.reach_metrics)
        name = 'reach{}'.format(report['suffix'].replace(' - ', '_'))
        metadata = self.create_report_metadata(sd, ed, name)
        if not self.request_query(metadata):
            logging.warning('Reach report {} was not created, check for '
                            'errors.'.format(name))
            return pd.DataFrame()
        self.download_report()
        self.remove_reach_footer()
        if report.get('daily'):
            return self.suffix_reach(self.df, report['suffix'])
        return self.suffix_reach(self.df, report['suffix'], sd)

    def remove_reach_footer(self):
        """
        Drops the footer under a reach report: the report time, date
        range, groupings, notes and filters DV360 writes beneath the rows,
        whose partner ids spill into the data columns.  Every reach report
        is grouped by advertiser first, so a row is kept on its advertiser
        id cell being a number rather than on every cell being filled,
        since DV360 leaves a reach figure it cannot model blank instead of
        writing 0.

        :return: The rows of the advertiser
        """
        if self.advertiser_id_col not in self.df.columns:
            return self.df
        ids = self.df[self.advertiser_id_col].astype(str).str.strip()
        ids = ids.str.replace(r'\.0$', '', regex=True)
        self.df = self.df[ids.str.isdigit()].reset_index(drop=True)
        return self.df

    def suffix_reach(self, df, suffix, sd=None):
        """
        Names a reach report's measures apart from any other level's and
        dates a whole range row to the start of the range, so a merge
        keeps it; a daily row keeps its own day.  The suffix follows
        dcapi's reach report columns.

        :param df: One reach report's rows
        :param suffix: The report's column suffix
        :param sd: Start of the range the reach covers, None for a daily
            report whose rows carry their day
        :return: The rows renamed and dated
        """
        if df.empty:
            return df
        df = df.rename(columns={
            x: '{}{}'.format(x, suffix) for x in self.reach_metrics.values()})
        if sd is not None:
            df[self.date_col] = sd.strftime('%Y/%m/%d')
        return df
