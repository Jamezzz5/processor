import io
import os
import re
import sys
import pytz
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
import reporting.vmcolumns as vmc

config_path = utl.config_path


class TtdApi(object):
    ttd_url = 'https://api.thetradedesk.com/v3'
    walmart_url = 'https://api.dsp.walmart.com/v3'

    def __init__(self):
        self.df = pd.DataFrame()
        self.configfile = None
        self.config = None
        self.config_list = None
        self.login = None
        self.password = None
        self.ad_id = None
        self.report_name = None
        self.auth_token = None
        self.query_token = None
        self.query_url = 'https://api.gen.adsrvr.org/graphql'
        self.headers = None
        self.default_config_file_name = 'ttdconfig.json'
        self.campaign_dict = {}
        self.report_name_is_advertiser = False
        self.is_authorized = True

    def input_config(self, config):
        logging.info('Loading TTD config file: {}'.format(config))
        self.configfile = os.path.join(config_path + config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.configfile))
            sys.exit(0)
        if 'LOGIN' in self.config:
            self.login = self.config['LOGIN']
        if 'PASS' in self.config:
            self.password = self.config['PASS']
        self.ad_id = self.config['ADID']
        self.report_name = self.config['Report Name']
        if 'Token' in self.config and self.config['Token']:
            self.query_token = self.config['Token']
            logging.info('Loaded token from config file.')
        else:
            self.query_token = self.get_new_token(self.login, self.password)
            if self.query_token:
                logging.info('New token retrieved and saved to config file.')
                self.config['Token'] = self.query_token
                try:
                    with open(self.configfile, 'w') as f:
                        json.dump(self.config, f, indent=4)
                except IOError:
                    logging.warning('Failed to write token to config file:'
                                    ' {}'.format(self.configfile))
        self.config_list = [self.login, self.password]

    def get_new_token(self, login, password):
        token = ""
        url = self.walmart_check()
        auth_url = '{0}/authentication'.format(url)
        payload = {'Login': login, 'Password': password}
        headers = {'Content-Type': 'application/json'}
        try:
            r = requests.post(auth_url, headers=headers, json=payload)
            if r.status_code == 200:
                token = r.json().get('Token')
                if not token:
                    logging.warning('Token not present in authentication'
                                    ' response.')
            else:
                logging.warning('Failed to retrieve token: {} - {}'.format(
                    r.status_code, r.text))
        except Exception as e:
            logging.warning('Exception occurred while retrieving token:'
                            ' {}'.format(str(e)))
        return token

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in TTD config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def authenticate(self):
        url = self.walmart_check()
        auth_url = "{0}/authentication".format(url)
        userpass = {'Login': self.login, 'Password': self.password}
        self.headers = {'Content-Type': 'application/json'}
        r = requests.post(auth_url, headers=self.headers, json=userpass)
        if r.status_code != 200:
            logging.error('Failed to authenticate with error code: {} '
                          'Error: {}'.format(r.status_code, r.content))
            auth_token = None
        else:
            auth_token = json.loads(r.text)['Token']
        return auth_token

    def set_headers(self):
        auth_token = self.authenticate()
        self.headers = {'Content-Type': 'application/json',
                        'TTD-Auth': auth_token}
        auth_success = True
        if not auth_token:
            auth_success = False
        return auth_success

    def get_download_url(self):
        auth_success = self.set_headers()
        if not auth_success:
            return None
        url = self.walmart_check()
        rep_url = '{0}/myreports/reportexecution/query/advertisers'.format(url)
        data = []
        i = 0
        error_response_count = 0
        result_data = [1]
        while len(result_data) != 0 and error_response_count < 100:
            payload = {
                'AdvertiserIds': [self.ad_id],
                'PageStartIndex': i * 99,
                'PageSize': 100
            }
            r = requests.post(rep_url, headers=self.headers, json=payload)
            raw_data = json.loads(r.content)
            if 'Result' in raw_data:
                result_data = raw_data['Result']
                match_data = [x for x in result_data if
                              x['ReportScheduleName'].replace(' ', '') ==
                              self.report_name.replace(' ', '') and
                              x['ReportExecutionState'] == 'Complete']
                data.extend(match_data)
                i += 1
            elif ('Message' in raw_data and
                  raw_data['Message'] == 'Too Many Requests'):
                logging.warning('Rate limit exceeded, '
                                'pausing for 300s: {}'.format(raw_data))
                time.sleep(300)
                error_response_count = self.response_error(error_response_count)
            else:
                logging.warning('Retrying.  Unknown response :'
                                '{}'.format(raw_data))
                error_response_count = self.response_error(error_response_count)
        try:
            last_completed = max(data,
                                 key=lambda x: x['ReportEndDateExclusive'])
        except ValueError as e:
            logging.warning('Report does not contain any data: {}'.format(e))
            return None
        dl_url = last_completed['ReportDeliveries'][0]['DownloadURL']
        return dl_url

    @staticmethod
    def response_error(error_response_count, max_errors=100):
        error_response_count += 1
        if error_response_count >= max_errors:
            logging.error(
                'Error count exceeded {}. Aborting.'.format(max_errors))
            sys.exit(0)
        return error_response_count

    def get_df_from_response(self, r):
        try:
            self.df = pd.read_csv(io.StringIO(r.content.decode('utf-8')))
        except pd.errors.EmptyDataError:
            logging.warning('Report is empty, returning blank df.')
            self.df = pd.DataFrame()
        except UnicodeDecodeError:
            self.df = pd.read_excel(io.BytesIO(r.content))
        return self.df

    def get_data(self, sd=None, ed=None, fields=None):
        """
        gets data from TTD.
        check if report name is 5-10 characters long and contains no symbols
        or spaces, if so use campaign IDs and graphQL
        otherwise it will assume the default and pull using report name
        :returns: dataframe
        """
        if self.check_report_type():
            self.df = self.get_report_for_multiple_campaigns(sd, ed)
        else:
            logging.info(
                'Getting TTD data for report: {}'.format(self.report_name))
            dl_url = self.get_download_url()
            if not dl_url:
                logging.warning('Could not retrieve url, returning blank df.')
                self.df = pd.DataFrame()
            else:
                r = requests.get(dl_url, headers=self.headers)
                self.df = self.get_df_from_response(r)
        return self.df

    def check_report_type(self, val_to_check=''):
        """
        Check if `self.report_name` matches one of the expected ID patterns:
        - Single alphanumeric ID (5–10 chars)
        - Multiple comma-separated IDs

        Returns:
            bool: True if the report_name is a valid ID or list of IDs,
            else False.
        """
        if not val_to_check:
            val_to_check = self.report_name
        check = False
        if ' ' in val_to_check:
            return check
        id_pattern = r'[A-Za-z0-9]{7}'
        single_match = re.fullmatch(id_pattern, val_to_check)
        multi_match = re.fullmatch(
            fr'{id_pattern}(,{id_pattern})+', val_to_check)
        if bool(single_match or multi_match):
            check = True
        return check

    def check_partner_id(self):
        self.set_headers()
        url = self.walmart_check()
        rep_url = '{0}/partner/query'.format(url)
        i = 0
        error_response_count = 0
        partner_id = None
        for i in range(5):
            if partner_id or error_response_count >= 5:
                break
            payload = {
                'searchTerms': [],
                'PageStartIndex': i * 99,
                'PageSize': 100
            }
            r = requests.post(rep_url, headers=self.headers, json=payload)
            raw_data = json.loads(r.content)
            if 'Result' in raw_data:
                partner_id = raw_data['Result'][0]['PartnerId']
            elif ('Message' in raw_data and
                  raw_data['Message'] == 'Too Many Requests'):
                logging.warning('Rate limit exceeded, '
                                'pausing for 5s: {}'.format(raw_data))
                time.sleep(5)
                error_response_count = self.response_error(
                    error_response_count)
            else:
                logging.warning('Retrying.  Unknown response :'
                                '{}'.format(raw_data))
                error_response_count = self.response_error(
                    error_response_count)
        return partner_id

    def check_advertiser_id(self, results, acc_col, success_msg, failure_msg):
        self.set_headers()
        url = self.walmart_check()
        rep_url = '{0}/advertiser/query/partner'.format(url)
        i = 0
        error_response_count = 0
        check_advertiser_id = ""
        partner_id = self.check_partner_id()
        r = None
        for i in range(5):
            payload = {
                'partnerId': partner_id,
                'PageStartIndex': i * 99,
                'PageSize': 100
            }
            r = requests.post(rep_url, headers=self.headers, json=payload)
            raw_data = json.loads(r.content)
            if 'Result' in raw_data:
                result_list = raw_data['Result']
                df = pd.DataFrame(data=result_list)
                check_advertiser_id = df['AdvertiserId'].eq(self.ad_id).any()
                if check_advertiser_id:
                    check_advertiser_id = self.ad_id
                break
            elif ('Message' in raw_data and
                  raw_data['Message'] == 'Too Many Requests'):
                logging.warning('Rate limit exceeded, '
                                'pausing for 5s: {}'.format(raw_data))
                time.sleep(5)
                error_response_count = self.response_error(
                    error_response_count)
            else:
                logging.warning('Retrying.  Unknown response :'
                                '{}'.format(raw_data))
                error_response_count = self.response_error(
                    error_response_count)
        if check_advertiser_id:
            row = [acc_col, ' '.join([success_msg, str(self.ad_id)]),
                   True]
            results.append(row)
        else:
            msg = ('Advertiser ID NOT Found. '
                   'Double Check ID and Ensure Permissions were granted.'
                   '\n Error Msg:')
            r = r.json()
            row = [acc_col, ' '.join([failure_msg, msg]), False]
            results.append(row)
        return results, r

    def check_reports_id(self, results, acc_col, success_msg, failure_msg):
        self.set_headers()
        url = self.walmart_check()
        rep_url = '{0}/myreports/reportschedule/query'.format(url)
        i = 0
        error_response_count = 0
        check_reports_id = ""
        r = None
        for i in range(5):
            if check_reports_id or error_response_count >= 5:
                break
            payload = {
                'sortFields': [],
                'PageStartIndex': i * 99,
                'NameContains': self.report_name,
                'PageSize': 100
            }
            r = requests.post(rep_url, headers=self.headers, json=payload)
            raw_data = json.loads(r.content)
            if 'Result' in raw_data:
                result_list = raw_data['Result']
                df = pd.DataFrame(data=result_list)
                if not df.empty:
                    if 'ReportScheduleName' in df.columns:
                        check_reports_id = \
                            df['ReportScheduleName'].eq(self.report_name).any()
                        if check_reports_id:
                            check_reports_id = self.report_name
                        break
            elif ('Result' in raw_data and
                  raw_data['Message'] == 'Too Many Requests'):
                logging.warning('Rate limit exceeded, '
                                'pausing for 5s: {}'.format(raw_data))
                time.sleep(5)
                error_response_count = self.response_error(
                    error_response_count)
            else:
                logging.warning('Retrying.  Unknown response :'
                                '{}'.format(raw_data))
                error_response_count = self.response_error(
                    error_response_count)
        if check_reports_id:
            row = [acc_col, ' '.join([success_msg, str(self.report_name)]),
                   True]
            results.append(row)
        else:
            msg = ('Report Name NOT Found.'
                   'Double Check name and Ensure Permissions were granted.'
                   '\n Error Msg:')
            r = r.json()
            row = [acc_col, ' '.join([failure_msg, msg]), False]
            results.append(row)
        return results, r

    def test_connection(self, acc_col, camp_col, acc_pre):
        success_msg = 'SUCCESS:'
        failure_msg = 'FAILURE:'
        self.set_headers()
        results, r = self.check_advertiser_id(
            [], camp_col, success_msg, failure_msg)
        if False in results[0]:
            return pd.DataFrame(data=results, columns=vmc.r_cols)
        if self.check_report_type():
            campaign_list = self.report_name.split(',')
            for campaign_id in campaign_list:
                if self.test_graphql_connection(campaign_id):
                    row = [acc_col,
                           ' '.join([success_msg, str(campaign_id)]),
                           True]
                    results.append(row)
                else:
                    row = [acc_col,
                           ' '.join([failure_msg, str(campaign_id)]),
                           False]
                    results.append(row)
        else:
            results, r = self.check_reports_id(
                results, acc_col, success_msg, failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)

    def test_graphql_connection(self, campaign_id):
        check = True
        query = """
                query ValidateCampaign($campaignId: ID!) {
                  campaign(id: $campaignId) {
                    id
                    name
                    status
                  }
                }
            """
        variables = {'campaignId': campaign_id}
        headers = {'TTD-Auth': self.query_token}
        r = requests.post(self.query_url,
                          json={'query': query, 'variables': variables},
                          headers=headers)
        if r.status_code != 200 or not r.json()['data']:
            check = False
        return check

    def walmart_check(self):
        if 'ttd_api' not in self.login and 'wmt_api' in self.login:
            url = self.walmart_url
        else:
            url = self.ttd_url
        return url

    @staticmethod
    def get_graphql_query_for_creative_reporting():
        query = """
            query MyQuery($creativeId: ID!, 
                          $fromDate: DateTime, 
                          $toDate: DateTime, 
                          $reportingCursor: String) {
              creative(id: $creativeId) {
                reporting {
                  creativePerformanceReporting(
                    where: {date: {gte: $fromDate, lte: $toDate}}
                    first: 1000
                    after: $reportingCursor
                  ) {
                    edges {
                      node {
                        metrics {
                          impressions
                          clicks
                          spend {
                            advertiserCurrency
                          }
                        }
                        dimensions {
                          time {
                            day
                          }
                        }
                      }
                    }
                    pageInfo {
                      hasNextPage
                      endCursor
                    }
                  }
                }
              }
            }
            """
        return query

    @staticmethod
    def get_graphql_query_for_all_advertiser_creatives():
        """
        Query to get all creatives for a given advertiser

        :return: The query as a string
        """
        query = """
        query GetArchivedCreatives($advertiserId: ID!,
                                   $cursor: String
            ) {
              advertiser(id: $advertiserId) {
                creatives(
                  first: 1000
                  after: $cursor
                ) {
                  edges {
                    node {
                      id
                      name
                      isArchived
                      adGroups(first: 500) {
                        edges {
                          node {
                            id
                            name
                            campaign { id name }
                          }
                        }
                        pageInfo { hasNextPage endCursor }
                      }
                    }
                  }
                  pageInfo { hasNextPage endCursor }
                }
              }
            }
            """
        return query

    @staticmethod
    def create_graphql_campaign_query():
        """
        Query used to get data using GraphQL.
        Gets Campaign Name, Ad Group Name, Creative Names, impressions, clicks,
        Advertiser Currency (Net Spend), and Date
        """
        query = """
            query MyQuery($campaignId: ID!, 
                          $fromDate: DateTime, 
                          $toDate: DateTime, 
                          $adGroupCursor: String, 
                          $creativeCursor: String, 
                          $reportingCursor: String) {
              campaign(id: $campaignId) {
                name
                adGroups(first: 1000, after: $adGroupCursor) {
                  nodes {
                    id
                    name
                    creatives(first: 1000, after: $creativeCursor) {
                      nodes {
                        name
                        reporting {
                          creativePerformanceReporting(
                            where: {date: {gte: $fromDate, lte: $toDate}}
                            first: 1000
                            after: $reportingCursor
                          ) {
                            edges {
                              node {
                                metrics {
                                  impressions
                                  clicks
                                  spend {
                                    advertiserCurrency
                                  }
                                }
                                dimensions {
                                  time {
                                    day
                                  }
                                }
                              }
                            }
                            pageInfo {
                              hasNextPage
                              endCursor
                            }
                          }
                        }
                      }
                      pageInfo {
                        hasNextPage
                        endCursor
                      }
                    }
                  }
                  pageInfo {
                    hasNextPage
                    endCursor
                  }
                }
              }
            }
        """
        return query

    def check_is_advertiser(self, candidate_id):
        """
        Checks the provided candidate_id string is an advertiser

        :param candidate_id: String to check
        :return: Boolean if the candidate_id string is an advertiser or not
        """
        is_advertiser_id = False
        query = """
            query ValidateAdvertiser($advId: ID!) {
              advertiser(id: $advId) { id name }
            }
        """
        headers = {'TTD-Auth': self.query_token}
        body = {'query': query, 'variables': {'advId': candidate_id}}
        r = requests.post(self.query_url, json=body, headers=headers)
        request = r.json()
        if request.get('data') is None:
            self.query_token = self.get_new_token(self.login, self.password)
            headers = {'TTD-Auth': self.query_token}
            r = requests.post(self.query_url, json=body, headers=headers)
            request = r.json()
        if r.status_code != 200 and 'Unauthorized' in r.text:
            logging.warning('Checking if {} is advertiser failed with '
                            'status code {}'.format(
                                candidate_id, r.text))
            self.is_authorized = False
        else:
            advertiser = request.get('data', {}).get('advertiser')
            if advertiser and self.is_authorized:
                is_advertiser_id = True
        return is_advertiser_id

    def get_child_object_ids(self, parent_id, parent_name='advertiser',
                             child_name='campaigns'):
        """
        Gets id and names of all objects associated with the parent id

        :param parent_id: The id of the parent object
        :param parent_name: The name for the query of the parent object
        :param child_name: The name for the query of the object to get
        :return: Dictionary with key as id and name as value
        """
        headers = {'TTD-Auth': self.query_token}
        object_ids = {}
        cursor = None
        query = f"""
        query FetchChildren($parentId: ID!, $cursor: String) {{
          {parent_name}(id: $parentId) {{
            {child_name}(first: 1000, after: $cursor) {{
              nodes {{ id name }}
              pageInfo {{ hasNextPage endCursor }}
            }}
          }}
        }}
        """
        for _ in range(10000):
            if not self.is_authorized:
                break
            variables = {
                'parentId': parent_id,
                'cursor': cursor}
            body = {'query': query, 'variables': variables}
            r = requests.post(self.query_url, json=body,  headers=headers)
            if r.status_code != 200:
                msg = 'Getting {} for {} {} failed, retrying.'.format(
                    child_name, parent_name, parent_id)
                logging.warning(msg)
                self.response_error(0, max_errors=5)
            data = r.json()['data']
            page = data[parent_name][child_name]
            page_info = page['pageInfo']
            nodes = page['nodes']
            for node in nodes:
                object_id = node['id']
                object_name = node['name']
                object_ids[object_id] = object_name
            if page_info['hasNextPage']:
                cursor = page_info['endCursor']
            else:
                break
        return object_ids

    def get_report_for_creative_id(self, creative_id, sd, ed, campaign_name,
                                   adgroup_name, creative_name):
        """
        Queries a given creative_id for reporting data for the sd and ed

        :param creative_id: The creative id to query
        :param sd: The start date to query form
        :param ed: The end date to query to
        :param campaign_name: The name of the campaign to append to the dict
        :param adgroup_name: The name of the ad group to append to the dict
        :param creative_name:
        :return: A list of dicts containing report data
        """
        headers = {'TTD-Auth': self.query_token}
        results = []
        cursor = None
        query = self.get_graphql_query_for_creative_reporting()
        tz = pytz.timezone("America/Los_Angeles")
        from_dt = tz.localize(
            dt.datetime(sd.year, sd.month, sd.day, 0, 0, 0))
        to_dt = tz.localize(
            dt.datetime(ed.year, ed.month, ed.day, 23, 59, 59, 999999))
        variables = {
            'creativeId': creative_id,
            'fromDate': from_dt.isoformat(),
            'toDate': to_dt.isoformat(),
        }
        for _ in range(10000):
            variables['reportingCursor'] = cursor
            body = {'query': query, 'variables': variables}
            r = requests.post(self.query_url, json=body,  headers=headers)
            data = r.json()['data']
            page = data['creative']['reporting']['creativePerformanceReporting']
            page_info = page['pageInfo']
            edges = page['edges']
            for edge in edges:
                node = edge['node']
                result = {
                    'Campaign Name': campaign_name,
                    'Adgroup': adgroup_name,
                    'Creative': creative_name,
                }
                metrics = node['metrics']
                dimensions = node['dimensions']
                result['Impressions'] = metrics['impressions']
                result['Clicks'] = metrics['clicks']
                result['Advertiser Cost (Adv Currency)'] = metrics[
                    'spend']['advertiserCurrency']
                result['Date'] = dimensions['time']['day']
                results.append(result)
            if page_info['hasNextPage']:
                cursor = page_info['endCursor']
            else:
                break
        return results

    def get_loose_creatives(self, advertiser_id, sd, ed):
        """
        Gets all creative not associated with campaign and returns data

        :param advertiser_id:
        :param sd:
        :param ed:
        :return:
        """
        headers = {'TTD-Auth': self.query_token}
        df = pd.DataFrame()
        cursor = None
        query = self.get_graphql_query_for_all_advertiser_creatives()
        variables = {
            'advertiserId': advertiser_id,
            'cursor': cursor}
        body = {'query': query, 'variables': variables}
        r = requests.post(self.query_url, json=body, headers=headers)
        edges = r.json()['data']['advertiser']['creatives']['edges']
        for creative_edge in edges:
            creative_node = creative_edge['node']
            if not creative_node['adGroups']['edges']:
                creative_id = creative_node['id']
                creative_name = creative_node['name']
                tdf = self.get_report_for_creative_id(
                    creative_id, sd, ed, '', '',
                    creative_name)
                tdf = pd.DataFrame(tdf)
                df = pd.concat([tdf, df], ignore_index=True)
        return df

    def loop_campaigns_get_report(self, campaign_list, sd, ed):
        """
        For a list of campaign ids loops campaigns/adgroups/creatives
        to get the report as a df filtered by the start date and end date

        :param campaign_list: List of campaign ids to loop
        :param sd:  Start date for the report
        :param ed: End Date for the report
        :return: The full report as a df
        """
        df = pd.DataFrame()
        for campaign_id in campaign_list:
            if not self.is_authorized:
                break
            campaign_name = self.campaign_dict[campaign_id]
            logging.info('Getting data for campaign {}'.format(campaign_id))
            adgroup_ids = self.get_child_object_ids(
                campaign_id, parent_name='campaign', child_name='adGroups')
            for adgroup_id, adgroup_name in adgroup_ids.items():
                creative_ids = self.get_child_object_ids(
                    adgroup_id, parent_name='adGroup', child_name='creatives')
                for creative_id, creative_name in creative_ids.items():
                    tdf = self.get_report_for_creative_id(
                        creative_id, sd, ed, campaign_name, adgroup_name,
                        creative_name)
                    tdf = pd.DataFrame(tdf)
                    df = pd.concat([tdf, df], ignore_index=True)
        if self.report_name_is_advertiser and not self.ad_id:
            tdf = self.get_loose_creatives(self.report_name, sd, ed)
            df = pd.concat([tdf, df], ignore_index=True)
        return df

    def get_report_for_multiple_campaigns(self, sd, ed):
        """
        Loop through campaign ids and query metrics for each, appending to
        results df as it goes
        :returns: dataframe containing placement and metric data for multiple
            campaigns
        """
        comma_in_name = ',' in self.report_name
        if not comma_in_name and self.check_is_advertiser(self.report_name):
            self.campaign_dict = self.get_child_object_ids(self.report_name)
            campaign_list = list(self.campaign_dict.keys())
            if self.ad_id:
                campaign_list = [x for x in self.campaign_dict
                                 if self.ad_id in self.campaign_dict[x]]
            self.report_name_is_advertiser = True
        else:
            if self.ad_id:
                self.campaign_dict = self.get_child_object_ids(self.ad_id)
            campaign_list = self.report_name.split(',')
        df = self.loop_campaigns_get_report(campaign_list, sd, ed)
        return df

    def get_paginated_report(self, campaign_id, sd=None, ed=None):
        """
        Get data using GraphQL Query, checks for extra pages
        :param campaign_id: (str) campaign ID from ttd platform of data to be
            pulled
        :param sd: (DateTime) The start date to pull form
        :param ed: (DateTime): The end date to pull from
            pagination from. Defaults to None (fetches from the first page).
        :returns: list of adgroups and their associated creatives with data
        """
        results = []
        error_response_count = 0
        query = self.create_graphql_campaign_query()
        headers = {
            'TTD-Auth': self.query_token
        }
        ad_group_cursor = creative_cursor = reporting_cursor = ''
        for x in range(9999):
            tz = pytz.timezone("America/Los_Angeles")
            from_dt = tz.localize(
                dt.datetime(sd.year, sd.month, sd.day, 0, 0, 0))
            to_dt = tz.localize(
                dt.datetime(ed.year, ed.month, ed.day, 23, 59, 59, 999999))
            variables = {
                'campaignId': campaign_id,
                'fromDate': from_dt.isoformat(),
                'toDate': to_dt.isoformat(),
                'adGroupCursor': ad_group_cursor,
                'creativeCursor': creative_cursor,
                'reportingCursor': reporting_cursor,
            }
            body = {'query': query, 'variables': variables}
            r = requests.post(self.query_url, json=body, headers=headers)
            if r.status_code != 200:
                logging.warning('Request Failed: {}'.format(r.status_code))
                error_response_count = self.response_error(
                    error_response_count, max_errors=10)
                continue
            data = r.json()
            if not data['data']:
                msg = 'Empty for campaign {} retrying'.format(campaign_id)
                logging.warning(msg)
                error_response_count = self.response_error(
                    error_response_count,  max_errors=10)
                continue
            campaign = data['data']['campaign']
            campaign_name = campaign['name']
            page = campaign['adGroups']
            nodes = page['nodes']
            for node in nodes:
                adgroup_name = node['name']
                creatives = node['creatives']
                for creative in creatives['nodes']:
                    creative_name = creative['name']
                    reporting = creative['reporting']
                    reporting = reporting['creativePerformanceReporting']
                    creative_edges = reporting['edges']
                    for creative_edge in creative_edges:
                        result = {
                            'Campaign Name': campaign_name,
                            'Adgroup': adgroup_name,
                            'Creative': creative_name,
                        }
                        creative_node = creative_edge['node']
                        metrics = creative_node['metrics']
                        dimensions = creative_node['dimensions']
                        result['Impressions'] = metrics['impressions']
                        result['Clicks'] = metrics['clicks']
                        result['Advertiser Cost (Adv Currency)'] = metrics[
                            'spend']['advertiserCurrency']
                        result['Date'] = dimensions['time']['day']
                        results.append(result)
                    reporting_page = reporting['pageInfo']
                    if reporting_page['hasNextPage']:
                        reporting_cursor = reporting_page['endCursor']
                        continue
                    else:
                        reporting_cursor = None
                creative_page = creatives['pageInfo']
                if creative_page['hasNextPage']:
                    creative_cursor = creative_page['endCursor']
                    continue
                else:
                    creative_cursor = None
            page_info = page['pageInfo']
            if page_info['hasNextPage']:
                ad_group_cursor = page_info['endCursor']
            else:
                break
        results = pd.DataFrame(results)
        return results

    @staticmethod
    def parse_graphql_result(result):
        """
        Parses json result from GraphQL query, creating a dataframe similar to
        reports in TTD platform
        :param result: GraphQL json result
        :returns: dataframe
        """
        rows = []
        for data in result:
            campaign = data['campaign'].get('name')
            adgroup = data['node']
            adgroup_name = adgroup['name']
            creatives_data = adgroup.get('creatives')
            creatives = creatives_data.get(
                'nodes', []) if creatives_data else []
            for creative in creatives:
                creative_name = creative['name']
                performance_edges = creative.get('reporting', {}).get(
                    'creativePerformanceReporting', {}).get('edges', [])
                for edge in performance_edges:
                    node = edge['node']
                    metrics = node.get('metrics', {})
                    dimensions = node.get('dimensions', {})
                    date = dimensions['time']['day']
                    impressions = metrics.get('impressions', 0)
                    clicks = metrics.get('clicks', 0)
                    cost = metrics.get('spend', {}).get('advertiserCurrency',
                                                        0.0)
                    rows.append({
                        'Date': date,
                        'Campaign Name': campaign,
                        'Adgroup': adgroup_name,
                        'Creative': creative_name,
                        'Clicks': clicks,
                        'Impressions': impressions,
                        'Advertiser Cost (Adv Currency)': cost
                    })
        df = pd.DataFrame(rows)
        return df
