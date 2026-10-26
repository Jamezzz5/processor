import os
import re
import sys
import json
import time
import logging
import datetime as dt
import requests
import pandas as pd
from io import StringIO
import reporting.utils as utl
import reporting.vmcolumns as vmc
import oauthlib.oauth2.rfc6749.errors
from requests_oauthlib import OAuth2Session

config_path = utl.config_path
base_url = 'https://www.googleapis.com/dfareporting'


class DcApi(object):
    path_field = 'pathToConversion'
    path_all_field = 'pathAllInteractions'
    path_only_field = 'pathOnly'
    path_activity_prefix = 'pathActivity::'
    api_field_options = (
        ('platformType', 'Add the platform type dimension'),
        ('60', 'Relative date range of the last 60 days'),
        ('30', 'Relative date range of the last 30 days'),
        (path_field, 'Add a row per attributed conversion naming the ad '
                     'credited and its order ID, last 60 days only'),
        (path_all_field, 'Path to Conversion with every interaction in the '
                         'path, credited or not, up to 200 where CM360 '
                         'allows extended paths, otherwise the last 5'),
        (path_only_field, 'Path to Conversion rows only, without '
                          'impressions, clicks or spend'))
    default_fields = [
        'campaign', 'campaignId', 'site', 'placement',
        'date', 'placementId', 'creative', 'ad', 'creativeId', 'adId',
        'packageRoadblock', 'contentCategory', 'creativeType']
    pos = 'positionInContent'
    default_metrics = [
        'impressions', 'clicks', 'clickRate',
        'activeViewViewableImpressions',
        'activeViewMeasurableImpressions',
        'activeViewEligibleImpressions', 'totalConversions',
        'mediaCost', 'dv360Cost', 'dv360CostUsd',
        'totalConversionsRevenue', 'richMediaVideoViews',
        'richMediaTrueViewViews', 'richMediaVideoCompletions',
        'richMediaVideoThirdQuartileCompletes',
        'richMediaVideoMidpoints',
        'richMediaVideoFirstQuartileCompletes', 'richMediaVideoPlays']
    default_conversion_metrics = [
        'activityClickThroughConversions', 'totalConversions',
        'totalConversionsRevenue', 'activityViewThroughConversions',
        'activityViewThroughRevenue', 'activityClickThroughRevenue']
    col_rename_dict = {}
    reach_metrics = {
        'impressionsCoviewed':  'Impressions (Co-Viewed)',
        'uniqueReachAverageImpressionFrequency':
            'Unique Reach: Average Impression Frequency',
        'uniqueReachAverageImpressionFrequencyCoviewed':
            'Unique Reach: Average Impression Frequency (Co-Viewed)',
        'uniqueReachImpressionReach': 'Unique Reach: Impression Reach',
        'uniqueReachTotalReach': 'Unique Reach: Total Reach',
        'uniqueReachClickReach': 'Unique Reach: Click Reach',
        'uniqueReachAverageViewableImpressionFrequency':
            'Unique Reach: Average Viewable Impression Frequency',
        'uniqueReachImpressionReachCoviewed':
            'Unique Reach: Impression Reach (Co-Viewed)',
        'uniqueReachIncrementalClickReach':
            'Unique Reach: Incremental Click Reach',
        'uniqueReachIncrementalImpressionReach':
            'Unique Reach: Incremental Impression Reach',
        'uniqueReachIncrementalTotalReach':
            'Unique Reach: Incremental Total Reach',
        'uniqueReachIncrementalViewableImpressionReach':
            'Unique Reach: Incremental Viewable Impression Reach',
        'uniqueReachTotalReachCoviewed':
            'Unique Reach: Total Reach (Co-Viewed)',
        'uniqueReachViewableImpressionReach':
            'Unique Reach: Viewable Impression Reach'}
    report_path = 'reports/'
    ad_path = 'advertisers/'
    camp_path = 'campaigns/'
    reach_max_days = 93
    path_max_days = 60
    path_conversion_fields = [
        'conversionId', 'activity', 'activityId', 'activityTime',
        'ordValue', 'tranValue', 'numValue', 'floodlightAttributionType',
        'daysSinceAttributedInteraction', 'pathLength', 'impressionCount',
        'clickCount']
    path_interaction_fields = [
        'interactionNumber', 'interactionTime', 'interactionType',
        'interactionChannel', 'campaign', 'campaignId', 'site', 'siteId',
        'placement', 'placementId', 'ad', 'adId', 'creative', 'creativeId',
        'floodlightAttributedInteraction']
    path_metrics = ['totalConversions', 'totalConversionsRevenue']
    path_metric_cols = ['Total Conversions', 'Total Revenue']
    path_suffix = ' - path_report'
    path_attributed_col = 'Floodlight Attributed Interaction'
    path_conversion_id_col = 'Conversion ID'
    path_activity_time_col = 'Activity Date/Time'
    path_extended = {'maximumClickInteractions': 100,
                     'maximumImpressionInteractions': 100}
    final_errors = ("reach reports can't span more than 93 days.",
                    "Invalid 'maximumClickInteractions' value",
                    "Invalid 'maximumImpressionInteractions' value")
    default_config_file_name = 'dcapi.json'
    date_col = 'Date'
    campaign_col = 'Campaign'
    campaign_id_col = 'Campaign ID'

    def __init__(self):
        self.config = None
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.usr_id = None
        self.advertiser_id = None
        self.campaign_id = None
        self.campaign_ids = []
        self.campaign_name_filter = []
        self.original_report_id = None
        self.report_ids = []
        self.config_list = None
        self.client = None
        self.date_range = None
        self.version = '5'
        self.df = pd.DataFrame()
        self.r = None
        self.report_id_dict = {}
        self.vendor_dict = {}
        self.campaign_dict = {}
        self.path_report = False
        self.path_all_interactions = False
        self.path_only = False
        self.path_activities = []

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading DC config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.load_config_dict(config)

    def load_config_dict(self, config):
        """Populate credentials from an in-memory dict, bypassing the
        config file load (used by the app-layer credential vault).
        ``usr_id`` and ``report_id`` may be absent when the dict is a
        pooled credential rather than a card.

        :param config: dict in dcapi.json shape
        """
        self.config = config
        self.client_id = config.get('client_id', '')
        self.client_secret = config.get('client_secret', '')
        self.access_token = config.get('access_token', '')
        self.refresh_token = config.get('refresh_token', '')
        self.refresh_url = config.get('refresh_url', '')
        self.usr_id = config.get('usr_id', '')
        self.original_report_id = config.get('report_id', '')
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url, self.usr_id]
        if 'advertiser_id' in config:
            self.advertiser_id = self.remove_space(config['advertiser_id'])
        if 'campaign_id' in config:
            self.campaign_id = self.remove_space(config['campaign_id'])

    @staticmethod
    def remove_space(val):
        return str(val).replace(' ', '')

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in DC config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def refresh_client_token(self, extra, max_attempts=10):
        """
        Attempts to refresh the the client token catching some errors

        :param extra: Dict of client_id, client_secret
        :param max_attempts: Attempts to try to refresh
        :return: The token if refreshed or None if not
        """
        token = None
        for attempt in range(max_attempts):
            try:
                token = self.client.refresh_token(self.refresh_url, **extra)
                break
            except (requests.exceptions.ConnectionError,
                    oauthlib.oauth2.rfc6749.errors.CustomOAuth2Error) as e:
                attempt += 1
                time.sleep(1)
                msg = 'Connection error retrying 60s: {}'.format(e)
                logging.warning(msg)
        if not token:
            logging.warning('Max retries exceeded')
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

    def create_profiles_url(self):
        return '{}/v{}/userprofiles'.format(base_url, self.version)

    def create_user_url(self):
        return '{}/{}/'.format(self.create_profiles_url(), self.usr_id)

    def get_user_profiles(self):
        """Every Campaign Manager user profile the credential can act as.

        :returns: list of profile dicts (``profileId``, ``accountName``,
            ``userName``), empty when the call failed
        """
        self.get_client()
        r = self.client.get(self.create_profiles_url())
        return r.json().get('items', []) if r.status_code == 200 else []

    def create_url(self, arg, path=report_path):
        user_url = self.create_user_url()
        full_url = (user_url + path + str(arg))
        return full_url

    def parse_fields(self, sd, ed, fields):
        """
        Reads the card's dates and API fields into the reports' settings.

        The dimensions are copied from the class list before platformType
        is added, since one instance pulls every DCM card in a run and an
        appended class list carried the dimension into every later card.
        Any path field turns the path report on and only adds a report,
        so on its own it leaves the card's dates as they are where any
        other field moves the reports to a relative range.

        :param sd: The card's start date
        :param ed: The card's end date
        :param fields: The card's API_FIELDS values, None when it has none
        """
        sd, ed = utl.date_check(sd, ed)
        self.date_range = {
            'startDate': sd.strftime('%Y-%m-%d'),
            'endDate': ed.strftime('%Y-%m-%d')
        }
        self.default_fields = list(type(self).default_fields)
        prefix = self.path_activity_prefix
        toggles = (self.path_field, self.path_all_field, self.path_only_field)
        fields = fields or []
        path_fields = [x for x in fields
                       if x in toggles or x.startswith(prefix)]
        fields = [x for x in fields if x not in path_fields]
        self.path_report = bool(path_fields)
        self.path_all_interactions = self.path_all_field in path_fields
        self.path_only = self.path_only_field in path_fields
        self.path_activities = [x[len(prefix):] for x in path_fields
                                if x.startswith(prefix)]
        if fields and fields != ['nan']:
            self.date_range = {'kind': 'dfareporting#dateRange',
                               'relativeDateRange': 'LAST_365_DAYS'}
            for field in fields:
                if field == 'platformType':
                    self.default_fields.append('platformType')
                if field == '60':
                    self.date_range['relativeDateRange'] = 'LAST_60_DAYS'
                elif field == '30':
                    self.date_range['relativeDateRange'] = 'LAST_30_DAYS'

    def report_date_ranges(self, reach_report=False, no_date=False):
        """
        The date ranges one report type is requested over.

        :param reach_report: Whether the report is a reach report
        :param no_date: Whether the report drops the date dimension
        :return: List of CM360 dateRange dicts, one report each
        """
        bounds = self.explicit_date_range() if reach_report else None
        if not bounds or (bounds[1] - bounds[0]).days < self.reach_max_days:
            return [self.date_range]
        start, end = bounds
        span = dt.timedelta(days=self.reach_max_days - 1)
        if no_date:
            return [self.format_date_range(end - span, end)]
        ranges = []
        while start <= end:
            stop = min(start + span, end)
            ranges.append(self.format_date_range(start, stop))
            start = stop + dt.timedelta(days=1)
        return ranges

    def explicit_date_range(self):
        """
        The first and last day of the report date range. A relative
        range is read as the days ending yesterday, since CM360's
        LAST_N_DAYS counts only complete days.

        :return: Tuple of start and end dates, or None when unreadable
        """
        date_range = self.date_range or {}
        if 'startDate' in date_range and 'endDate' in date_range:
            return tuple(dt.date.fromisoformat(date_range[x])
                         for x in ('startDate', 'endDate'))
        days = re.fullmatch(r'LAST_(\d+)_DAYS',
                            date_range.get('relativeDateRange', ''))
        if not days:
            return None
        end = dt.date.today() - dt.timedelta(days=1)
        return end - dt.timedelta(days=int(days.group(1)) - 1), end

    @staticmethod
    def format_date_range(start, end):
        return {'startDate': start.strftime('%Y-%m-%d'),
                'endDate': end.strftime('%Y-%m-%d')}

    def path_date_range(self):
        """
        The card's date range cut to the days CM360 keeps Path to
        Conversion data for: it refuses the whole report when the range
        reaches back more than 60 days, so older days are left to the
        raw file a merge keeps.

        :return: CM360 dateRange dict, None when the card's range ends
            before the days CM360 keeps
        """
        today = dt.date.today()
        earliest = today - dt.timedelta(days=self.path_max_days - 1)
        start, end = self.explicit_date_range() or (earliest, today)
        start = max(start, earliest)
        end = min(end, today)
        if start > end:
            return None
        return self.format_date_range(start, end)

    def parse_campaign_filter(self):
        """
        Splits the campaign filter into ids to send and values to match.

        A campaign dimension filter only accepts ids, so an all numeric
        filter is sent to the api.  Anything else is a campaign name and is
        matched against the report once it has been downloaded.

        :return: The list of campaign ids for the api
        """
        self.campaign_ids, self.campaign_name_filter = (
            utl.parse_campaign_filter(self.campaign_id))
        return self.campaign_ids

    def get_data(self, sd=None, ed=None, fields=None):
        df = pd.DataFrame()
        # One instance pulls every DCM card in a run, so reports kept from
        # the last card would be downloaded again into this card's file.
        self.report_ids = []
        self.report_id_dict = {}
        self.parse_fields(sd, ed, fields)
        self.parse_campaign_filter()
        report_created = False
        report_types = [(False, False, False, False),
                        (True, False, False, False),
                        (True, True, False, False),
                        (True, True, True, False),
                        (True, True, False, True),
                        (True, False, True, False),
                        (True, False, False, True)]
        if self.original_report_id:
            self.report_ids.append(self.original_report_id)
            report_types = []
            report_created = True
        if self.path_only:
            report_types = []
        for report_type in report_types:
            reach_report = report_type[0]
            no_date = report_type[1]
            campaign_report = report_type[2]
            vendor_report = report_type[3]
            for date_range in self.report_date_ranges(reach_report, no_date):
                single_report_created = self.create_report(
                    reach_report=reach_report, no_date=no_date,
                    campaign_report=campaign_report,
                    vendor_report=vendor_report, date_range=date_range)
                if single_report_created:
                    report_created = True
        if self.path_report and self.create_path_report():
            report_created = True
        if not report_created:
            logging.warning('Report not created returning blank df.')
            return df
        for report_id in self.report_ids:
            logging.info('Getting report id: {}'.format(report_id))
            self.df = pd.DataFrame()
            files_url = self.get_files_url(report_id)
            if not files_url:
                logging.warning('Report not created returning blank df.')
                continue
            if report_id not in self.report_id_dict:
                self.report_id_dict[report_id] = {}
            self.report_id_dict[report_id]['url'] = files_url
        for report_id, report_param in self.report_id_dict.items():
            files_url = report_param['url']
            self.r = self.get_report(files_url)
            if not self.r:
                continue
            tdf = self.get_df_from_response()
            last_row = 1
            if 'A dash' in str(tdf.iloc[-1, 0]):
                last_row = 2
            tdf = utl.first_last_adj(tdf, first_row=1, last_row=last_row)
            if report_param.get('path_report'):
                tdf = self.attributed_path_rows(tdf)
            tdf = self.rename_cols(tdf, report_param)
            tdf = utl.filter_df_on_campaign(
                tdf, self.campaign_name_filter, self.campaign_col,
                self.campaign_id_col)
            if 'reach_report' in report_param:
                if not report_param['reach_report']:
                    self.check_for_campaign_vendor_dicts(tdf)
                else:
                    tdf = self.add_placement_name(tdf)
            df = pd.concat([df, tdf], ignore_index=True)
        df = self.drop_empty_conversion_cols(df)
        return df

    @staticmethod
    def drop_empty_conversion_cols(df):
        """Drop all-zero Floodlight activity conversion columns.

        DCM emits six conversion columns per Floodlight activity
        (default_conversion_metrics); the vast majority are
        entirely zero and bloat the raw file. Activity columns
        carry the ' : ' group separator that core metrics never
        use, so the filter can never drop a mapped metric or the
        account-level Total Conversions column.
        """
        sep = ' : '
        empty_cols = []
        for col in df.columns:
            if sep not in str(col):
                continue
            numeric = pd.to_numeric(df[col], errors='coerce').fillna(0)
            if numeric.eq(0).all():
                empty_cols.append(col)
        if empty_cols:
            df = df.drop(columns=empty_cols)
            logging.info('Dropped {} all-zero DCM conversion '
                         'columns.'.format(len(empty_cols)))
        return df

    def attributed_path_rows(self, df):
        """
        One row per attributed conversion out of a Path to Conversion
        report.

        CM360 writes each conversion as a row carrying its count and
        revenue, followed by a row per interaction in its path with both
        zero.  The row kept is the interaction CM360 credited: its ad,
        placement, and whether it was a click or an impression, with the
        conversion's count and revenue moved onto it, so a sum by
        placement gives each ad its attributed conversions once.  The
        measures are suffixed apart from the standard report's, which
        counts the same conversions, and each row is dated to its
        conversion so a date merge keeps it past the 60 days CM360 does.
        With every interaction asked for, the uncredited ones stay too,
        with zero count and revenue and the TRUE/FALSE column kept.

        :param df: The report's rows, header applied
        :return: The attributed conversions, empty when the report has none
        """
        flag_col = self.path_attributed_col
        if df.empty or flag_col not in df.columns:
            return pd.DataFrame()
        id_col = self.path_conversion_id_col
        metrics = [x for x in self.path_metric_cols if x in df.columns]
        totals = df.loc[df[flag_col].isna(), [id_col] + metrics]
        rows = df[df[flag_col].notna()].drop(columns=metrics)
        rows = rows.merge(totals, on=id_col, how='left')
        credited = rows[flag_col].astype(str).str.upper() == 'TRUE'
        if self.path_all_interactions:
            rows.loc[~credited, metrics] = 0
        else:
            rows = rows[credited].drop(columns=[flag_col])
        rows[self.date_col] = pd.to_datetime(
            rows[self.path_activity_time_col],
            errors='coerce').dt.strftime('%Y-%m-%d')
        return rows.rename(
            columns={x: '{}{}'.format(x, self.path_suffix) for x in metrics})

    def check_for_campaign_vendor_dicts(self, tdf):
        """
        Learns, from the delivery rows' placements, the vendor and campaign
        parts a reach row's invented Placement should carry, keyed by the
        site and campaign the report names.

        :param tdf: The delivery rows, with their placements
        """
        temp_col = 'PlacementPart'
        cols = [('Site (CM360)', utl.placement_vendor_part, self.vendor_dict),
                ('Campaign', utl.placement_campaign_part, self.campaign_dict)]
        for col_name, place_idx, storage_dict in cols:
            second_parts = tdf[utl.placement_col].str.split('_')
            second_parts = second_parts.str[place_idx]
            pair_df = tdf[[col_name]].copy()
            pair_df[temp_col] = second_parts
            unique_pairs = pair_df.drop_duplicates(subset=[col_name])
            res = unique_pairs.set_index(col_name)[temp_col].to_dict()
            storage_dict.update(res)

    def add_placement_name(self, tdf):
        """
        Invents a Placement for a reach report's rows, which name none:
        the vendor and campaign parts learned from the delivery rows, in
        the sixteen part shape every reach report's rows share.

        :param tdf: One reach report's rows
        :return: The rows with the Placement column filled
        """
        place_col = utl.placement_col
        if place_col in tdf.columns:
            return tdf
        campaigns = tdf['Campaign'].map(self.campaign_dict)
        vendors = pd.Series('', index=tdf.index)
        if 'Site (CM360)' in tdf.columns:
            vendors = tdf['Site (CM360)'].map(self.vendor_dict)
        tdf[place_col] = [utl.placement_name(vendor, campaign)
                          for vendor, campaign in zip(vendors, campaigns)]
        return tdf

    def find_first_line(self):
        for idx, x in enumerate(self.r.text.splitlines()):
            if idx > 1000:
                logging.warning('Could not find first line, returning empty df')
                return None
            if x == 'Report Fields':
                return idx

    def get_df_from_response(self):
        first_line = self.find_first_line()
        if first_line:
            self.df = pd.read_csv(StringIO(self.r.text), skiprows=first_line)
            self.df = self.df.reset_index().copy()
        else:
            self.df = pd.DataFrame()
        return self.df

    def get_report(self, files_url):
        report_status = False
        for x in range(1, 401):
            report_status = self.check_file(files_url, attempt=x)
            if report_status:
                break
        if not report_status:
            logging.warning('Report could not download returning blank df.')
            return None
        logging.info('Report available.  Downloading.')
        report_url = '{}?alt=media'.format(files_url)
        self.r = self.make_request(report_url, 'get', json_response=False)
        return self.r

    def check_file(self, files_url, attempt=1):
        r = self.make_request(files_url, 'get')
        if 'status' in r.json() and r.json()['status'] == 'REPORT_AVAILABLE':
            return True
        else:
            logging.info('Report unavailable.  Attempt {}.  '
                         'Response: {}'.format(attempt, r.json()))
            time.sleep(15)
            return False

    def get_files_url(self, report_id):
        full_url = self.create_url(report_id)
        self.r = self.make_request('{}/run'.format(full_url), 'post')
        if not self.r:
            logging.warning('No files URL returning.')
            return None
        file_id = self.r.json()['id']
        files_url = '{}/files/{}'.format(full_url, file_id)
        return files_url

    def make_request(self, url, method, body=None, params=None, attempt=1,
                     json_response=True):
        self.get_client()
        try:
            self.r = self.raw_request(url, method, body=body, params=params)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url=url, method=method, body=body,
                                       params=params, attempt=attempt,
                                       json_response=json_response)
        if json_response and 'error' in self.r.json():
            message = self.r.json()['error']['message']
            if any(x in message for x in self.final_errors):
                attempt = 100
            else:
                logging.warning(
                    'Request error.  Retrying {}'.format(self.r.json()))
                time.sleep(10)
                attempt += 1
            if attempt > 10:
                self.request_error()
                return None
            self.r = self.make_request(url=url, method=method, body=body,
                                       params=params, attempt=attempt,
                                       json_response=json_response)
        return self.r

    def raw_request(self, url, method, body=None, params=None):
        if method == 'get':
            if body:
                self.r = self.client.get(url, json=body, params=params)
            else:
                self.r = self.client.get(url, params=params)
        elif method == 'post':
            if body:
                self.r = self.client.post(url, json=body, params=params)
            else:
                self.r = self.client.post(url, params=params)
        return self.r

    def request_error(self):
        logging.warning('Unknown error: {}'.format(self.r.text))

    def create_report(self, reach_report=False, no_date=False,
                      campaign_report=False, vendor_report=False,
                      date_range=None):
        if self.original_report_id:
            return True
        report = self.create_report_params(reach_report, no_date,
                                           campaign_report, vendor_report,
                                           date_range)
        report_params = {'reach_report': reach_report,
                         'no_date': no_date,
                         'campaign_report': campaign_report,
                         'vendor_report': vendor_report,
                         'date_range': date_range}
        return self.queue_report(report, report_params)

    def queue_report(self, report, report_params):
        """
        Saves a report to the user profile and keeps its id with the
        settings its rows are read back under once it has run.

        :param report: The CM360 report body
        :param report_params: What get_data needs to read its rows
        :return: Whether CM360 accepted the report
        """
        full_url = self.create_url('')
        self.r = self.make_request(full_url, method='post', body=report)
        if not self.r:
            return False
        report_id = self.r.json()['id']
        self.report_ids.append(report_id)
        self.report_id_dict[report_id] = report_params
        return True

    def create_path_report(self):
        """
        Queues the card's Path to Conversion report, which CM360 keys on
        the advertiser's Floodlight configuration rather than on the
        advertiser, and only answers for the last 60 days.  Every
        interaction asks for extended paths, and the last 5 when the
        configuration doesn't allow them.

        :return: Whether a report was queued
        """
        date_range = self.path_date_range()
        if not date_range:
            logging.warning(
                'CM360 keeps Path to Conversion data for the last {} days '
                'and the date range ends before them.  Not pulling '
                'it.'.format(self.path_max_days))
            return False
        config_id = self.get_floodlight_config_id()
        if not config_id:
            logging.warning('No Floodlight configuration for advertiser {}.  '
                            'Not pulling Path to Conversion.'
                            ''.format(self.advertiser_id))
            return False
        activity_ids = self.path_activity_ids()
        if self.path_activities and not activity_ids:
            logging.warning('None of the card\'s Floodlight activities are '
                            'on advertiser {}.  Not pulling Path to '
                            'Conversion.'.format(self.advertiser_id))
            return False
        params = {'path_report': True, 'date_range': date_range}
        extended = self.path_all_interactions
        report = self.create_path_report_params(
            date_range, config_id, activity_ids, extended)
        queued = self.queue_report(report, params)
        if queued or not extended:
            return queued
        logging.warning('CM360 refused extended paths for advertiser {}.  '
                        'Pulling the last 5 interactions of each path.'
                        ''.format(self.advertiser_id))
        report = self.create_path_report_params(
            date_range, config_id, activity_ids)
        return self.queue_report(report, params)

    def get_floodlight_config_id(self):
        """
        The Floodlight configuration the advertiser's conversions are
        counted under, which may be another advertiser's it shares.

        :return: The configuration id, None when the advertiser can't be
            read
        """
        url = self.create_url(self.advertiser_id, self.ad_path)
        self.r = self.make_request(url, method='get')
        if not self.r:
            return None
        return self.r.json().get('floodlightConfigurationId')

    def create_path_report_params(self, date_range, config_id, activity_ids,
                                  extended=False):
        """
        The body of a Path to Conversion report of the attributed
        conversions only, CM360's default: the dimensions name each
        conversion and every interaction in its path, with the campaign
        id so the card's campaign filter can be matched after download,
        since this report type takes no campaign filter.

        :param date_range: CM360 dateRange dict, within the last 60 days
        :param config_id: The advertiser's Floodlight configuration id
        :param activity_ids: The activities to report, every one when
            empty
        :param extended: Whether to ask for up to 100 clicks and 100
            impressions per path, which CM360 refuses for a Floodlight
            configuration without extended paths
        :return: The report body
        """
        report_name = '{}path_report'.format(self.report_base_name())
        report = {
            'name': report_name,
            'type': 'PATH_TO_CONVERSION',
            'fileName': report_name,
            'format': 'CSV',
            'pathToConversionCriteria': {
                'floodlightConfigId': {
                    'dimensionName': 'floodlightConfigId',
                    'value': str(config_id),
                    'kind': 'dfareporting#dimensionValue'},
                'dateRange': date_range,
                'activityFilters': self.activity_filters(activity_ids),
                'conversionDimensions': self.sorted_dimensions(
                    self.path_conversion_fields),
                'perInteractionDimensions': self.sorted_dimensions(
                    self.path_interaction_fields),
                'metricNames': self.path_metrics}}
        if extended:
            report['pathToConversionCriteria']['reportProperties'] = dict(
                self.path_extended)
        return report

    @staticmethod
    def sorted_dimensions(names):
        return [{'kind': 'dfareporting#sortedDimension', 'name': x}
                for x in names]

    @staticmethod
    def activity_filters(activity_ids):
        return [{'dimensionName': 'activity',
                 'id': x,
                 'kind': 'dfareporting#dimensionValue'} for x in activity_ids]

    def get_floodlight_tag_ids(self):
        return [x['id'] for x in self.get_floodlight_activities()]

    def path_activity_ids(self):
        """
        The activities the path report asks for: the ones the card names
        by id or exact name, or all of the advertiser's when it names none.

        :return: List of activity ids, empty when none of the card's match
        """
        activities = self.get_floodlight_activities()
        wanted = set(self.path_activities)
        if not wanted:
            return [x['id'] for x in activities]
        known = ({x['id'] for x in activities}
                 | {x.get('name') for x in activities})
        missing = wanted - known
        if missing:
            logging.warning('No Floodlight activity {} on advertiser {}.'
                            ''.format(', '.join(sorted(missing)),
                                      self.advertiser_id))
        return [x['id'] for x in activities
                if x['id'] in wanted or x.get('name') in wanted]

    def get_floodlight_activities(self, activities=None, next_page=None):
        if not activities:
            activities = []
        fl_url = self.create_user_url()
        params = {'advertiserId': self.advertiser_id}
        fl_url = '{}floodlightActivities'.format(fl_url)
        if next_page:
            params['pageToken'] = next_page
        self.r = self.make_request(fl_url, method='get', params=params)
        if not self.r:
            logging.warning('Could not make floodlights returning blank list.')
            return activities
        if 'floodlightActivities' not in self.r.json():
            logging.warning('floodlightActivities not in response as follows: '
                            '\n{}'.format(self.r.json()))
        activities.extend(self.r.json()['floodlightActivities'])
        if 'nextPageToken' in self.r.json() and self.r.json()['nextPageToken']:
            activities = self.get_floodlight_activities(
                activities, next_page=self.r.json()['nextPageToken'])
        return activities

    def create_report_params(self, reach_report=False, no_date=False,
                             campaign_report=False, vendor_report=False,
                             date_range=None):
        report_name = '{}standard_report'.format(self.report_base_name())
        report = {
            'name': report_name,
            'type': 'STANDARD',
            'fileName': report_name,
            'format': 'CSV'
        }
        if reach_report:
            report['type'] = 'REACH'
            report['name'] = '{}_reach'.format(report['name'])
        if no_date:
            report['name'] = '{}_no_date'.format(report['name'])
        if campaign_report:
            report['name'] = '{}_campaign'.format(report['name'])
        if vendor_report:
            report['name'] = '{}_vendor'.format(report['name'])
        criteria = self.create_report_criteria(reach_report, no_date,
                                               campaign_report, vendor_report,
                                               date_range)
        criteria_str = 'criteria'
        if reach_report:
            criteria_str = 'reachCriteria'
        report[criteria_str] = criteria
        return report

    def report_base_name(self):
        """The advertiser and campaign filter every report's name starts
        with, so the reports a card saves in CM360 read as its own."""
        report_name = ''
        for name in [self.advertiser_id, self.campaign_id]:
            name = re.sub(r'\W+', '', name)
            report_name += '{}_'.format(name)
        return report_name

    def create_report_criteria(self, reach_report=False, no_date=False,
                               campaign_report=False, vendor_report=False,
                               date_range=None):
        metrics = self.default_metrics
        dimensions = self.default_fields
        if reach_report:
            metrics = list(self.reach_metrics.keys())
            dimensions = [x for x in dimensions
                          if 'creative' not in x and 'ad' not in x]
        if no_date:
            dimensions = [x for x in dimensions if 'date' not in x]
        if campaign_report:
            dimensions = [x for x in dimensions if x in ('campaign', 'date')]
        if vendor_report:
            dimensions = [x for x in dimensions
                          if x in ('campaign', 'site', 'date')]
        criteria = {
            'dateRange': date_range or self.date_range,
            'dimensions': self.sorted_dimensions(dimensions),
            'metricNames': metrics,
            'dimensionFilters': [{
                'dimensionName': 'advertiser',
                'id': self.advertiser_id,
                'kind': 'dfareporting#dimensionValue'}]
        }
        if not reach_report:
            criteria['activities'] = {
                'metricNames': self.default_conversion_metrics,
                'kind': 'dfareporting#activities',
                'filters': []
            }
            fl_ids = self.get_floodlight_tag_ids()
            if fl_ids:
                criteria['activities']['filters'] = self.activity_filters(
                    fl_ids)
            else:
                logging.warning('No floodlight conversions found.')
                criteria.pop('activities')
        if self.campaign_ids:
            campaign_filters = [
                {'dimensionName': 'campaign',
                 'id': x,
                 'kind': 'dfareporting#dimensionValue'}
                for x in self.campaign_ids]
            criteria['dimensionFilters'].extend(campaign_filters)
        return criteria

    def rename_cols(self, tdf, report_param):
        rename_dict = self.col_rename_dict.copy()
        if 'reach_report' in report_param and report_param['reach_report']:
            level = ''
            if report_param['campaign_report']:
                level = 'campaign'
            elif report_param['vendor_report']:
                level = utl.platform_report_level
            col_str = utl.reach_suffix(
                level, daily=not report_param['no_date'])
            if col_str:
                for k, v in self.reach_metrics.items():
                    rename_dict[v] = '{}{}'.format(v, col_str)
            date_range = report_param.get('date_range') or self.date_range
            for boundary in ('startDate', 'endDate'):
                if boundary in (date_range or {}):
                    tdf[f'CM360 {boundary}{col_str}'] = date_range[boundary]
        tdf.rename(columns=rename_dict, inplace=True)
        return tdf

    def test_connection(self, acc_col, camp_col, pre_col):
        success_msg = 'SUCCESS -- ID:'
        failure_msg = 'FAILURE:'
        self.get_client()
        self.parse_campaign_filter()
        results = []
        query_url = self.create_url(self.advertiser_id, self.ad_path)
        r = self.client.get(url=query_url)
        if r.status_code == 200:
            row = [acc_col, ' '.join([success_msg, str(self.advertiser_id)]),
                   True]
            results.append(row)
        else:
            r = r.json()
            row = [acc_col, ' '.join([failure_msg, r['error']['message']]),
                   False]
            results.append(row)
            return pd.DataFrame(data=results, columns=vmc.r_cols)
        if self.campaign_name_filter and not self.campaign_ids:
            row = [camp_col, ' '.join(
                [success_msg, str(self.campaign_name_filter),
                 '-- matched on the campaign name after download']), True]
            results.append(row)
        for campaign in self.campaign_ids:
            query_url = self.create_url(campaign, self.camp_path)
            r = self.client.get(url=query_url)
            if r.status_code == 200:
                row = [camp_col, ' '.join([success_msg, str(campaign)]), True]
                results.append(row)
            else:
                r = r.json()
                row = [camp_col, ' '.join([failure_msg, r['error']['message']]),
                       False]
                results.append(row)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
