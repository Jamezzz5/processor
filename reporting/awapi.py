import json.decoder
import os
import ast
import sys
import yaml
import time
import logging
import requests
import numpy as np
import pandas as pd
import datetime as dt
import reporting.utils as utl
from requests_oauthlib import OAuth2Session
from urllib3.exceptions import ConnectionError, NewConnectionError

config_path = utl.config_path


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
    views = ReportColumn('video_views', 'Views', 'metrics')
    views25_rate = ReportColumn(
        'video_quartile_p25_rate', 'Video played to 25%', 'metrics')
    views50_rate = ReportColumn(
        'video_quartile_p50_rate', 'Video played to 50%', 'metrics')
    views75_rate = ReportColumn(
        'video_quartile_p75_rate', 'Video played to 75%', 'metrics')
    views100_rate = ReportColumn(
        'video_quartile_p100_rate', 'Video played to 100%', 'metrics')
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
    device = ReportColumn('device', 'Device', 'segments')

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
                            self.views75_rate, self.views100_rate]
        self.base_conv_metrics = [self.conversions]
        self.ext_conv_metrics = [self.conversion_name, self.all_conversions,
                                 self.view_conversions]
        self.conv_metrics = self.base_conv_metrics + self.ext_conv_metrics
        self.rf_metrics = ['ImpressionReach', 'AverageFrequency']
        self.def_fields = self.def_params + self.def_metrics
        self.conv_fields = self.def_params + self.conv_metrics
        self.uac_fields = (self.date_params + self.camp_params +
                           self.def_metrics + self.base_conv_metrics)
        self.no_date_fields = self.no_date_params + self.def_metrics
        self.view_metrics = [self.views25_rate, self.views50_rate,
                             self.views75_rate, self.views100_rate]


class AwApi(object):
    version = 10
    base_url = 'https://googleads.googleapis.com/v{}/customers/'.format(version)
    report_url = '/googleAds:searchStream'
    refresh_url = 'https://www.googleapis.com/oauth2/v3/token'
    access_url = '{}:listAccessibleCustomers'.format(base_url[:-1])

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
                self.config = yaml.safe_load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.configfile))
            sys.exit(0)
        self.config = self.config['adwords']
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.developer_token = self.config['developer_token']
        self.refresh_token = self.config['refresh_token']
        self.client_customer_id = self.config['client_customer_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.developer_token, self.refresh_token,
                            self.client_customer_id]
        if 'campaign_filter' in self.config:
            self.campaign_filter = self.config['campaign_filter']
        if 'login_customer_id' in self.config:
            self.login_customer_id = self.config['login_customer_id']

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
                df['{} - Percent'.format(column)] = df[column]
                df[column] = ((df[column] / 100) *
                              df[self.rb.views.display_name].astype(np.float))
        return df

    def find_correct_login_customer_id(self, report):
        headers = self.get_client()
        r = self.client.get(self.access_url, headers=headers)
        customer_ids = r.json()['resourceNames']
        for customer_id in customer_ids:
            customer_id = customer_id.replace('customers/', '')
            logging.info('Attempting customer id: {}'.format(customer_id))
            self.login_customer_id = customer_id
            r = self.request_report(report)
            if r.json() == [] or 'results' in r.json()[0]:
                self.config['login_customer_id'] = self.login_customer_id
                with open(self.configfile, 'w') as f:
                    yaml.dump({'adwords': self.config}, f)
                return r
        logging.warning('Could not find customer ID exiting.')
        sys.exit(0)

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
                metrics = self.rb.conv_metrics
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
                if field == 'RF':
                    metrics += self.rb.rf_metrics
        api_fields = metrics + params
        return api_fields

    def get_report_request_dict(self, sd, ed, fields):
        select_str = ','.join([x.full_name for x in fields])
        report = {
            "query": """
                SELECT {}
                FROM {}
                WHERE segments.date >= '{}'
                AND segments.date <= '{}'""".format(
                    select_str, self.report_type, sd.strftime('%Y-%m-%d'),
                    ed.strftime('%Y-%m-%d'))}
        return report

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        sd = sd.date()
        ed = ed.date()
        fields = self.parse_fields(fields)
        if sd > ed:
            logging.warning('Start date greater than end date.  Start date'
                            'was set to end date.')
            sd = ed
        logging.info('Getting Adwords data from {} until {}'.format(sd, ed))
        report = self.get_report_request_dict(sd, ed, fields)
        r = self.request_report(report)
        r = self.check_report(r, report)
        if not r:
            logging.warning('No response returning blank df.')
            return self.df
        self.df = self.report_to_df(r, fields)
        return self.df

    def get_report_url(self):
        cid = self.client_customer_id.replace('-', '')
        url = '{}{}{}'.format(self.base_url, cid, self.report_url)
        return url

    def request_report(self, report):
        if self.login_customer_id:
            logging.info('Requesting Report.')
            headers = self.get_client()
            report_url = self.get_report_url()
            try:
                r = self.client.post(report_url, json=report, headers=headers)
            except (ConnectionError, NewConnectionError) as e:
                logging.warning('Connection error, retrying: \n{}'.format(e))
                r = self.request_report(report)
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
        if json_resp != [] and 'results' not in json_resp[0]:
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

    def report_to_df(self, r, fields):
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
                results = page['results']
                tdf = pd.io.json.json_normalize(results)
                replace_dict = {x.return_name: x.display_name for x in fields}
                tdf = tdf.rename(columns=replace_dict)
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
