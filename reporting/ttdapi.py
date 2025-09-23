import io
import re
import sys
import json
import time
import logging
import requests
import pandas as pd
import reporting.utils as utl
import reporting.vmcolumns as vmc


ttd_url = 'https://api.thetradedesk.com/v3'
walmart_url = 'https://api.dsp.walmart.com/v3'
configpath = utl.config_path


class TtdApi(object):
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

    def input_config(self, config):
        logging.info('Loading TTD config file: {}'.format(config))
        self.configfile = configpath + config
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.configfile))
            sys.exit(0)
        self.login = self.config['LOGIN']
        self.password = self.config['PASS']
        self.ad_id = self.config['ADID']
        self.report_name = self.config['Report Name']
        if 'Token' in self.config and self.config['Token']:
            self.query_token = self.config['Token']
            logging.info("Loaded token from config file.")
        else:
            self.query_token = self.get_new_token(self.login, self.password)
            logging.info("New token retrieved and saved to config file.")
            self.config['Token'] = self.query_token
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        self.config_list = [self.login, self.password]

    def get_new_token(self, login, password):
        """
        Requests a new TTD token using login & password
        """
        url = self.walmart_check()
        auth_url = "{0}/authentication".format(url)
        payload = {"Login": login, "Password": password}
        headers = {"Content-Type": "application/json"}
        r = requests.post(auth_url, headers=headers, json=payload)
        if r.status_code == 200:
            token = r.json().get("Token")
            if not token:
                logging.error("Token not present in authentication"
                              " response.")
                sys.exit(1)
            return token
        else:
            logging.error("Failed to retrieve token: {} - {}".format(
                r.status_code, r.text))
            sys.exit(1)

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
            sys.exit(0)
        auth_token = json.loads(r.text)['Token']
        return auth_token

    def set_headers(self):
        auth_token = self.authenticate()
        self.headers = {'Content-Type': 'application/json',
                        'TTD-Auth': auth_token}

    def get_download_url(self):
        self.set_headers()
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
            last_completed = max(data, key=lambda x: x['ReportEndDateExclusive'])
        except ValueError as e:
            logging.warning('Report does not contain any data: {}'.format(e))
            return None
        dl_url = last_completed['ReportDeliveries'][0]['DownloadURL']
        return dl_url

    @staticmethod
    def response_error(error_response_count):
        error_response_count += 1
        if error_response_count >= 100:
            logging.error('Error count exceeded 100.  Aborting.')
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
        id_pattern = r'[A-Za-z0-9]{5,10}'
        if (re.fullmatch(id_pattern, self.report_name) and
                ' ' not in self.report_name):
            self.df = self.get_report_for_multiple_campaigns()
        elif re.fullmatch(fr'{id_pattern}(,{id_pattern})+',
                          self.report_name):
            self.df = self.get_report_for_multiple_campaigns()
        else:
            logging.info(
                'Getting TTD data for report: {}'.format(self.report_name))
            dl_url = self.get_download_url()
            if dl_url is None:
                logging.warning('Could not retrieve url, returning blank df.')
                self.df = pd.DataFrame()
            else:
                r = requests.get(dl_url, headers=self.headers)
                self.df = self.get_df_from_response(r)
        return self.df

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
            [], acc_col, success_msg, failure_msg)
        if False in results[0]:
            return pd.DataFrame(data=results, columns=vmc.r_cols)
        results, r = self.check_reports_id(
            results, camp_col, success_msg, failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)

    def walmart_check(self):
        if 'ttd_api' not in self.login and 'wmt_api' in self.login:
            url = walmart_url
        else:
            url = ttd_url
        return url

    @staticmethod
    def create_graphql_campaign_query():
        """
        Query used to get data using GraphQL.
        Gets Campaign Name, Ad Group Name, Creative Names, impressions, clicks,
        Advertiser Currency (Net Spend), and Date
        """
        query = """
            query MyQuery($campaignId: ID!) {
              campaign(id: $campaignId) {
                name
                adGroups {
                  nodes {
                    name
                    creatives {
                      nodes {
                        name
                        reporting {
                          creativePerformanceReporting {
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
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
        """
        return query

    def get_report_for_multiple_campaigns(self):
        """
        Loop through campaign ids and query metrics for each, appending to
        results df as it goes
        :returns: dataframe containing placement and metric data for multiple
        campaigns
        """
        results = pd.DataFrame()
        campaign_list = self.report_name.split(',')
        for campaign_id in campaign_list:
            logging.info('Getting data for campaign {}'.format(campaign_id))
            result = self.get_report_using_graphql(campaign_id)
            df = self.parse_graphql_result(result)
            results = pd.concat([results, df], ignore_index=True)
        return results

    def get_report_using_graphql(self, campaign_id):
        """
        Runs GraphQL query to get campaign ID metric and placement data
        :param campaign_id: campaign id found in TTD platform
        :returns: json result from query
        """
        result = []
        query = self.create_graphql_campaign_query()
        variables = {
            "campaignId": campaign_id
        }
        data = {
            'query': query,
            'variables': variables
        }
        headers = {
            'TTD-Auth': self.query_token
        }
        r = requests.post(url=self.query_url, json=data, headers=headers)
        if r.status_code == 200:
            result = r.json()
        else:
            logging.warning(
                'Request failed with status code: {}'.format(r.status_code))
        return result

    @staticmethod
    def parse_graphql_result(result):
        """
        Parses json result from GraphQL query, creating a dataframe similar to
        reports in TTD platform
        :param result: GraphQL json result
        :returns: dataframe
        """
        data = result
        rows = []
        campaign = data['data']['campaign']
        campaign_name = campaign['name']
        adgroups = campaign['adGroups']['nodes']
        for adgroup in adgroups:
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
                        'Campaign': campaign_name,
                        'Adgroup': adgroup_name,
                        'Creative': creative_name,
                        'Clicks': clicks,
                        'Impressions': impressions,
                        'Advertiser Cost (Adv Currency)': cost
                    })
        df = pd.DataFrame(rows)
        return df
