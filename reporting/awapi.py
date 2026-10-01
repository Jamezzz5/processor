import os
import ast
import sys
import yaml
import time
import logging
import requests
import json.decoder
import pandas as pd
import datetime as dt
import reporting.utils as utl
import reporting.vmcolumns as vmc
from requests_oauthlib import OAuth2Session
from urllib3.exceptions import ConnectionError, NewConnectionError

config_path = utl.config_path
campaign_col = 'Campaign'


class ReportColumn(object):
    def __init__(self, name, display_name, column_type, column_subtype=None,
                 column_subtype_2=None):
        self.name = name
        self.display_name = display_name
        self.column_type = column_type
        self.column_subtype = column_subtype
        self.column_subtype_2 = column_subtype_2
        self.full_name = self.set_full_name()
        self.return_name = self.set_return_name()

    def set_full_name(self):
        if self.column_subtype:
            if self.column_subtype_2:
                self.full_name = '{}.{}.{}.{}'.format(
                    self.column_type, self.column_subtype,
                    self.column_subtype_2, self.name)
            else:
                self.full_name = '{}.{}.{}'.format(
                    self.column_type, self.column_subtype, self.name)
        else:
            self.full_name = '{}.{}'.format(self.column_type, self.name)
        return self.full_name

    def set_return_name(self):
        self.return_name = ''.join(
            word.title() for word in self.full_name.split('_'))
        self.return_name = '.'.join(
            w[0].lower() + w[1:] for w in self.return_name.split('.'))
        return self.return_name


class AwApiReportBuilder(object):
    date = ReportColumn('date', 'Day', 'segments')
    impressions = ReportColumn('impressions', 'Impressions', 'metrics')
    clicks = ReportColumn('clicks', 'Clicks', 'metrics')
    cost = ReportColumn('cost_micros', 'Cost', 'metrics')
    views = ReportColumn('video_trueview_views', 'Views', 'metrics')
    views25_rate = ReportColumn(
        'video_quartile_p25_rate', 'Video played to 25%', 'metrics')
    views50_rate = ReportColumn(
        'video_quartile_p50_rate', 'Video played to 50%', 'metrics')
    views75_rate = ReportColumn(
        'video_quartile_p75_rate', 'Video played to 75%', 'metrics')
    views100_rate = ReportColumn(
        'video_quartile_p100_rate', 'Video played to 100%', 'metrics')
    engagements = ReportColumn('engagements', 'Engagements', 'metrics')
    account = ReportColumn('descriptive_name', 'Account', 'customer')
    campaign = ReportColumn('name', 'Campaign', 'campaign')
    ad_group = ReportColumn('name', 'Ad group', 'ad_group')
    image_ad = ReportColumn(
        'name', 'Image ad name', 'ad_group_ad', 'ad', 'image_ad')
    ad = ReportColumn('name', 'Ad', 'ad_group_ad', 'ad')
    display_url = ReportColumn(
        'display_url', 'Display URL', 'ad_group_ad', 'ad')
    headline_1 = ReportColumn(
        'headline_part1', 'Headline 1', 'ad_group_ad', 'ad', 'expanded_text_ad')
    headline_2 = ReportColumn(
        'headline_part2', 'Headline 2', 'ad_group_ad', 'ad', 'expanded_text_ad')
    headline_3 = ReportColumn(
        'headline_part3', 'Headline 3', 'ad_group_ad', 'ad', 'expanded_text_ad')
    headline_resp_search = ReportColumn(
        'headlines', 'Responsive Search Ad descriptions', 'ad_group_ad',
        'ad', 'responsive_search_ad')
    description_resp_search = ReportColumn(
        'descriptions', 'Responsive Search Ad headlines', 'ad_group_ad',
        'ad', 'responsive_search_ad')
    headline_text = ReportColumn(
        'headline', 'Headline - Text', 'ad_group_ad', 'ad', 'text_ad')
    description = ReportColumn(
        'description', 'Description', 'ad_group_ad', 'ad', 'expanded_text_ad')
    description_2 = ReportColumn(
        'description2', 'Description line 2', 'ad_group_ad', 'ad',
        'expanded_text_ad')
    description_text = ReportColumn(
        'description1', 'Description 1 - text', 'ad_group_ad', 'ad', 'text_ad')
    description_text_2 = ReportColumn(
        'description2', 'Description 2 - text', 'ad_group_ad', 'ad', 'text_ad')
    conversions = ReportColumn('conversions', 'Conversions', 'metrics')
    all_conversions = ReportColumn(
        'all_conversions', 'All conv.', 'metrics')
    view_conversions = ReportColumn(
        'view_through_conversions', 'View-through conv.', 'metrics')
    conversion_name = ReportColumn(
        'conversion_action_name', 'Conversion name', 'segments')
    conversion_value = ReportColumn('conversions_value', 'Conversions Value',
                                    'metrics')
    device = ReportColumn('device', 'Device', 'segments')
    campaign_id = ReportColumn('id', 'Campaign ID', 'campaign')
    unique_users = ReportColumn('unique_users', 'Unique users', 'metrics')
    frequency = ReportColumn(
        'average_impression_frequency_per_user', 'Avg. impr. freq. per user',
        'metrics')

    def __init__(self):
        self.date_params = [self.date]
        self.camp_params = [self.account, self.campaign]
        self.ag_params = [self.ad_group]
        self.ad_params = [
            self.image_ad, self.headline_1, self.headline_2, self.headline_3,
            self.display_url, self.description, self.description_2,
            self.description_text, self.description_text_2, self.headline_text,
            self.headline_resp_search, self.description_resp_search, self.ad]
        self.no_date_params = self.camp_params + self.ag_params + self.ad_params
        self.def_params = self.date_params + self.no_date_params
        self.def_metrics = [self.impressions, self.clicks, self.cost,
                            self.views, self.views25_rate, self.views50_rate,
                            self.views75_rate, self.views100_rate,
                            self.engagements]
        self.base_conv_metrics = [self.conversions, self.conversion_value]
        self.ext_conv_metrics = [self.conversion_name, self.all_conversions,
                                 self.view_conversions]
        self.conv_metrics = self.base_conv_metrics + self.ext_conv_metrics
        self.reach_metrics = [self.unique_users, self.frequency]
        self.reach_fields = (self.camp_params + [self.campaign_id] +
                             self.reach_metrics)
        self.daily_reach_fields = self.date_params + self.reach_fields
        self.def_fields = self.def_params + self.def_metrics
        self.conv_fields = self.def_params + self.conv_metrics
        self.uac_fields = (self.date_params + self.camp_params +
                           self.def_metrics + self.base_conv_metrics)
        self.no_date_fields = self.no_date_params + self.def_metrics
        self.view_metrics = [self.views25_rate, self.views50_rate,
                             self.views75_rate, self.views100_rate]


class AwApi(object):
    no_reach_field = 'No Reach'
    api_field_options = (
        ('Conversions', 'Conversion metrics in place of the defaults'),
        ('Campaign', 'Campaign level report'),
        ('UAC', 'App campaign report with base conversions'),
        ('no_date', 'Drop the date parameters'),
        ('Device', 'Add the device dimension'),
        (no_reach_field,
         'Skip the whole range and the daily campaign reach'))
    version = 24
    base_url = 'https://googleads.googleapis.com/v{}/customers/'.format(version)
    report_url = '/googleAds:searchStream'
    refresh_url = 'https://www.googleapis.com/oauth2/v3/token'
    access_url = '{}:listAccessibleCustomers'.format(base_url[:-1])
    default_config_file_name = 'awconfig.yaml'
    reach_report_type = 'campaign'
    reach_max_days = 92
    reach_suffix = ' - no_date - campaign_report'
    daily_reach_suffix = ' - campaign_report'

    def __init__(self):
        self.df = pd.DataFrame()
        self.config = None
        self.configfile = None
        self.client_id = None
        self.client_secret = None
        self.developer_token = None
        self.refresh_token = None
        self.client_customer_id = None
        self.campaign_filter = None
        self.config_list = []
        self.client = None
        self.report_type = None
        self.access_token = None
        self.login_customer_id = ''
        self.rb = AwApiReportBuilder()

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix. '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Adwords config file: {}'.format(config))
        self.configfile = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                config = yaml.safe_load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.configfile))
            sys.exit(0)
        self.load_config_dict(config.get('adwords', config))

    def load_config_dict(self, config):
        """Populate credentials from the flat ``adwords`` dict, bypassing
        the config file load (used by the app-layer credential vault).
        ``client_customer_id`` may be absent when the dict is a pooled
        credential rather than a card, and is read as a string whatever
        the file holds: an id written to the yaml without quotes loads
        as an int, which the report url cannot strip dashes from.

        A config file that has lost its ``adwords:`` section arrives
        here as its own root, so the card still runs; the next write of
        the file nests it again.

        :param config: dict in awconfig.yaml's ``adwords`` shape
        """
        self.config = config
        self.client_id = config.get('client_id', '')
        self.client_secret = config.get('client_secret', '')
        self.developer_token = config.get('developer_token', '')
        self.refresh_token = config.get('refresh_token', '')
        self.client_customer_id = str(config.get('client_customer_id') or '')
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.developer_token, self.refresh_token,
                            self.client_customer_id]
        self.campaign_filter = config.get('campaign_filter',
                                          self.campaign_filter)
        self.login_customer_id = config.get('login_customer_id',
                                            self.login_customer_id)

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in AW config file.  '
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
        header = self.get_headers()
        return header

    def get_headers(self):
        login_customer_id = str(self.login_customer_id).replace('-', '')
        header = {"Content-Type": "application/json",
                  "developer-token": self.developer_token,
                  "Authorization": "Bearer {}".format(self.refresh_token)}
        if login_customer_id:
            header["login-customer-id"] = login_customer_id
        return header

    def video_calc(self, df):
        for metric in self.rb.view_metrics:
            column = metric.display_name
            if column in df.columns:
                df[column] = (df[column] * 100).round(2)
                percent_col = '{} - Percent'.format(column)
                df[percent_col] = df[column]
                df[column] = ((df[column] / 100) *
                              df[self.rb.views.display_name].astype(float))
                imp_view_col = '{} - Impressions'.format(column)
                df[imp_view_col] = (
                        (df[percent_col] / 100) *
                        df[self.rb.impressions.display_name].astype(float))
        return df

    def get_accessible_customers(self):
        """Bare ids of every customer the credential directly accesses.

        :returns: list of customer id strings, empty when the call failed
        """
        headers = self.get_client()
        r = self.client.get(self.access_url, headers=headers)
        if 'resourceNames' not in r.json():
            logging.warning(r.json())
            return []
        return [x.replace('customers/', '') for x in r.json()['resourceNames']]

    def get_customer_clients(self, customer_id):
        """Every account under one accessible customer: itself, and for a
        manager its whole client tree.

        :param customer_id: bare customer id to query and log in as
        :returns: list of dicts with ``id``, ``name`` and ``manager``
        """
        self.login_customer_id = customer_id
        self.client_customer_id = customer_id
        query = """
            SELECT customer_client.id, customer_client.descriptive_name, 
                customer_client.manager 
            FROM customer_client
        """
        report = {'query': query}
        r = self.request_report(report)
        pages = r.json() if r is not None and r.status_code == 200 else []
        clients = [x['customerClient'] for page in pages
                   for x in page.get('results', [])]
        return [{'id': str(x.get('id', '')),
                 'name': x.get('descriptiveName', ''),
                 'manager': bool(x.get('manager'))} for x in clients]

    def find_correct_login_customer_id(self, report):
        for customer_id in self.get_accessible_customers():
            logging.info('Attempting customer id: {}'.format(customer_id))
            self.login_customer_id = customer_id
            r = self.request_report(report)
            if r.json() == [] or 'results' in r.json()[0]:
                self.config['login_customer_id'] = self.login_customer_id
                with open(self.configfile, 'w') as f:
                    yaml.dump({'adwords': self.config}, f)
                return r
        logging.warning('Could not find customer ID exiting.')
        return None

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        return sd, ed

    def parse_fields(self, fields):
        params = self.rb.def_params[:]
        metrics = self.rb.def_metrics[:]
        self.report_type = 'ad_group_ad'
        if fields is not None:
            if 'Conversions' in fields:
                metrics = self.rb.conv_metrics[:]
            if 'Campaign' in fields:
                self.report_type = 'campaign'
                params = self.rb.camp_params
            if 'UAC' in fields:
                params = self.rb.camp_params + self.rb.date_params
                metrics += self.rb.base_conv_metrics
                self.report_type = 'campaign'
            if 'no_date' in fields:
                params = [x for x in params if x not in self.rb.date_params]
            if 'no_date' in fields and 'Conversions' in fields:
                params = self.rb.no_date_params + self.rb.conv_metrics
            for field in fields:
                if field == 'Device':
                    metrics += [self.rb.device]
        api_fields = metrics + params
        return api_fields

    def get_report_request_dict(self, sd, ed, fields, report_type=None):
        """
        Builds the search stream query.  Selecting no segments.date makes
        the api total the range in one row per object, which is what a
        reach report needs.

        :param sd: Start of the range
        :param ed: End of the range
        :param fields: ReportColumns to select
        :param report_type: Resource to query, the parsed one by default
        :return: The request body
        """
        select_str = ','.join([x.full_name for x in fields])
        report = {
            "query": """
                SELECT {}
                FROM {}
                WHERE segments.date >= '{}'
                AND segments.date <= '{}'""".format(
                    select_str, report_type or self.report_type,
                    sd.strftime('%Y-%m-%d'), ed.strftime('%Y-%m-%d'))}
        return report

    def date_range(self, sd, ed):
        """
        Dates a pull runs over: yesterday when not given, and a start past
        the end pulled back to it.

        :param sd: Start of the range or None
        :param ed: End of the range or None
        :return: Tuple of start and end dates
        """
        sd, ed = self.get_data_default_check(sd, ed)
        sd = sd.date()
        ed = ed.date()
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date'
                            'was set to end date.')
            sd = ed
        return sd, ed

    def get_data(self, sd=None, ed=None, fields=None):
        """
        Pulls the daily report, then adds the whole range and the daily
        campaign reach rows under it.

        :param sd: Start of the range
        :param ed: End of the range
        :param fields: The API_FIELDS values from the vendor matrix
        :return: df of the daily rows and the reach rows
        """
        self.df = pd.DataFrame()
        sd, ed = self.date_range(sd, ed)
        api_fields = self.parse_fields(fields)
        logging.info('Getting Adwords data from {} until {}'.format(sd, ed))
        report = self.get_report_request_dict(sd, ed, api_fields)
        self.df = self.request_report_df(report, api_fields)
        if self.pull_reach(fields):
            self.df = pd.concat([self.df, self.request_reach(sd, ed)],
                                ignore_index=True)
        return self.df

    def request_report_df(self, report, fields):
        """
        Requests one report and converts it, blank when nothing came back.

        :param report: The request body
        :param fields: ReportColumns the response columns are named by
        :return: df of the report rows
        """
        r = self.request_report(report)
        if not r:
            logging.warning('No response returning blank df.')
            return pd.DataFrame()
        r = self.check_report(r, report)
        if not r:
            logging.warning('No response returning blank df.')
            return pd.DataFrame()
        return self.report_to_df(r, fields)

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
        rows a merged raw file already holds and replace them.  The daily
        report cannot select these metrics, so the suffixes only keep the
        whole range rows and the daily campaign rows apart from each other.

        :return: List of the suffixed reach and frequency column names
        """
        return ['{}{}'.format(x.display_name, suffix)
                for suffix in (self.reach_suffix, self.daily_reach_suffix)
                for x in self.rb.reach_metrics]

    def get_reach(self, sd=None, ed=None, fields=None):
        """
        Only the reach rows, the whole range and the daily campaign rows,
        for a caller whose delivery range is shorter than the range reach
        has to cover: importhandler trims a merge card's delivery to its
        merge window but pulls reach from the card's start date.

        :param sd: Start of the reach range
        :param ed: End of the reach range
        :param fields: The card's api fields, read only for No Reach
        :return: df of reach rows
        """
        if not self.pull_reach(fields):
            return pd.DataFrame()
        sd, ed = self.date_range(sd, ed)
        self.df = self.request_reach(sd, ed)
        return self.df

    def request_reach(self, sd, ed):
        """
        Pulls one row per campaign for the whole range, so Google counts
        each person once across it, and one row per campaign per day.
        Google Ads reports unique users on the campaign resource alone,
        so the account has no daily row of its own.

        Google Ads totals the metrics only within 92 days.  Two blocks of
        whole range reach cannot be added back together, so a longer
        range skips that report; the daily rows each cover one day, so
        they are pulled in 92 day blocks whatever the range.

        :param sd: Start of the range
        :param ed: End of the range
        :return: df of reach rows, their measures suffixed per report
        """
        frames = []
        days = (ed - sd).days + 1
        if days > self.reach_max_days:
            logging.warning(
                'Google Ads only counts people once within {} days, '
                'skipping whole range reach for the {} day range.'.format(
                    self.reach_max_days, days))
        else:
            frames.append(self.request_reach_rows(
                sd, ed, self.rb.reach_fields, self.reach_suffix, sd))
        for block_sd, block_ed in utl.date_blocks(sd, ed,
                                                  self.reach_max_days):
            frames.append(self.request_reach_rows(
                block_sd, block_ed, self.rb.daily_reach_fields,
                self.daily_reach_suffix))
        return pd.concat(frames, ignore_index=True)

    def request_reach_rows(self, sd, ed, fields, suffix, date=None):
        """
        Runs one campaign reach query and names its rows.

        :param sd: Start of the range
        :param ed: End of the range
        :param fields: ReportColumns to select
        :param suffix: Column suffix naming the report
        :param date: Day to date every row to, for a whole range report
        :return: df of the report's rows, suffixed and dated
        """
        report = self.get_report_request_dict(sd, ed, fields,
                                              self.reach_report_type)
        df = self.request_report_df(report, fields)
        return self.suffix_reach(df, suffix, date)

    def suffix_reach(self, df, suffix, date=None):
        """
        Names a reach report's measures apart from the other report's,
        dates a whole range row to the start of its range so a merge keeps
        it, and drops a campaign type without the metrics (Search,
        Shopping), which comes back with both blank.

        :param df: The reach report's rows
        :param suffix: Column suffix naming the report
        :param date: Day to date every row to, None to keep the row's own
        :return: The rows renamed and dated
        """
        metrics = [x.display_name for x in self.rb.reach_metrics
                   if x.display_name in df.columns]
        if not metrics:
            return pd.DataFrame()
        df = df.dropna(subset=metrics, how='all').reset_index(drop=True)
        if date is not None:
            df[self.rb.date.display_name] = date.strftime('%Y-%m-%d')
        return df.rename(columns={
            x: '{}{}'.format(x, suffix) for x in metrics})

    def get_report_url(self):
        cid = self.client_customer_id.replace('-', '')
        url = '{}{}{}'.format(self.base_url, cid, self.report_url)
        return url

    def request_report(self, report):
        if self.login_customer_id:
            logging.info('Requesting Report.')
            headers = self.get_client()
            report_url = self.get_report_url()
            r = None
            for x in range(10):
                try:
                    r = self.client.post(report_url, json=report,
                                         headers=headers)
                except (ConnectionError, NewConnectionError) as e:
                    logging.warning('Connection error, retrying: \n{}'.format(e))
                if r and r.status_code == 200:
                    break
                else:
                    logging.warning(r.json())
                time.sleep(0.1)
        else:
            logging.warning('No login customer id, attempting to find.')
            r = self.find_correct_login_customer_id(report)
        return r

    def check_report(self, r, report):
        try:
            json_resp = r.json()
        except json.decoder.JSONDecoder as e:
            logging.warning('No JSON in response retrying: {}'.format(e))
            return None
        if json_resp and 'error' in json_resp[0]:
            if json_resp[0]['error']['status'] == 'PERMISSION_DENIED':
                logging.warning('Permission denied, trying all customers.')
                r = self.find_correct_login_customer_id(report)
            elif json_resp[0]['error']['status'] == 'INTERNAL':
                logging.warning('Google internal error - retrying.')
                time.sleep(30)
                r = self.find_correct_login_customer_id(report)
            else:
                logging.warning('Unknown response: {}'.format(json_resp))
                return None
        return r

    def report_to_df(self, r, fields, filter_camp=True):
        logging.info('Response received converting to df.')
        if not r.json():
            logging.warning('No results in response returning blank df.')
            df = pd.DataFrame()
        else:
            total_pages = len(r.json())
            df = pd.DataFrame()
            for idx, page in enumerate(r.json()):
                logging.info('Parsing results page: {} of {}'.format(
                    idx + 1, total_pages))
                if 'results' not in page:
                    continue
                results = page['results']
                tdf = pd.json_normalize(results)
                replace_dict = {x.return_name: x.display_name for x in fields}
                tdf = tdf.rename(columns=replace_dict)
                if filter_camp:
                    tdf = self.filter_on_campaign(tdf)
                tdf = self.clean_up_columns(tdf)
                tdf = tdf.loc[:, ~tdf.columns.duplicated()].copy()
                df = pd.concat([df, tdf], sort=False, ignore_index=True)
            logging.info('Returning data as df.')
        return df

    def filter_on_campaign(self, df):
        if self.campaign_filter:
            df = df[df['Campaign'].str.contains(str(self.campaign_filter))]
            df = df.reset_index(drop=True)
        return df

    def clean_up_columns(self, df):
        if 'Cost' in df.columns:
            df = utl.data_to_type(df, float_col=['Cost'])
            df['Cost'] /= 1000000
        if 'Views' in df.columns:
            df = self.video_calc(df)
        for col in ['Responsive Search Ad descriptions',
                    'Responsive Search Ad headlines']:
            if (col in df.columns and len(df[col]) > 0 and
                    df[col][0] != ' --'):
                df = self.convert_search_ad_descriptions(col, df)
        return df

    def convert_search_ad_descriptions(self, col, df):
        df[col] = df[col].replace(' --', '[{}]')
        df[col] = df[col].apply(lambda x: self.convert_dictionary(x))
        ndf = df[col].apply(pd.Series).fillna(0)
        for idx, new_col in enumerate(ndf.columns):
            tdf = ndf[new_col].apply(pd.Series)
            tdf = tdf.fillna(0)
            if 'pinnedField' in tdf.columns:
                tdf.columns = ['{}-{}'.format(x, tdf['pinnedField'][0])
                               for x in tdf.columns]
            else:
                tdf.columns = ['{}-{}'.format(x, idx)
                               for x in tdf.columns]
            df = pd.concat([df, tdf], axis=1)
        df = df.drop(col, axis=1)
        return df

    @staticmethod
    def convert_dictionary(x):
        if str(x) == str('nan'):
            return 0
        x = str(x).strip('[]')
        return ast.literal_eval(x)

    def return_row(self, col, result=False, acc_filter='Account'):
        msg = 'FAILURE: '
        row = []
        if result:
            msg = 'SUCCESS -- ID: '
        if acc_filter == 'Account':
            msg_id = str(self.client_customer_id)
            row = [col, ''.join([msg, msg_id]), result]
            return row
        if acc_filter == 'Campaign':
            msg_id = str(self.campaign_filter)
            row = [col, ''.join([msg, msg_id]), result]
            return row
        return row

    def get_campaign_and_client_ids(self):
        sd = dt.datetime.today()
        ed = dt.datetime.today()
        report = {
            "query": """
                        SELECT 
                            customer.id,
                            campaign.id,
                            campaign.name
                        FROM campaign
                        WHERE segments.date >= '{}'
                        AND segments.date <= '{}'""".format(
                sd.strftime('%Y-%m-%d'),
                ed.strftime('%Y-%m-%d'))}
        r = self.request_report(report)
        r = self.check_report(r, report)
        if not r:
            logging.warning('No response returning blank df.')
            return self.df
        fields = self.parse_fields(fields=None)
        self.df = self.report_to_df(r, fields, filter_camp=False)
        return self.df

    @staticmethod
    def check_df(df, str_filter, column='customer.id'):
        if df.empty:
            return False
        else:
            str_filter = str(str_filter).replace('-', '')
            df = df[df[column].str.contains(str_filter)]
            df = df.reset_index(drop=True)
            if df.empty:
                return False
        return True

    def test_connection(self, acc_col, camp_col, acc_pre):
        results = []
        headers = self.get_client()
        try:
            for x in range(10):
                r = self.client.get(self.access_url, headers=headers)
                if r and r.status_code == 200:
                    break
                time.sleep(0.1)
            df = self.get_campaign_and_client_ids()
        except (ConnectionError, NewConnectionError, TypeError) as e:
            row = self.return_row(acc_col, False,
                                  acc_filter='Account')
            results.append(row)
            results = pd.DataFrame(data=results, columns=vmc.r_cols)
            return results
        if self.check_df(df, self.client_customer_id):
            row = self.return_row(acc_col, True,
                                  acc_filter='Account')
            results.append(row)
        else:
            row = self.return_row(acc_col, False,
                                  acc_filter='Account')
            results.append(row)
            results = pd.DataFrame(data=results, columns=vmc.r_cols)
            return results
        if self.campaign_filter:
            if self.check_df(df, self.campaign_filter, column='Campaign'):
                row = self.return_row(camp_col, True,
                                      acc_filter='Campaign')
                results.append(row)
            else:
                row = self.return_row(camp_col, False,
                                      acc_filter='Campaign')
                results.append(row)
        results = pd.DataFrame(data=results, columns=vmc.r_cols)
        return results
