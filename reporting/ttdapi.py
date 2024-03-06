import io
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
        self.config_list = [self.login, self.password]

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
        logging.info('Getting TTD data for: {}'.format(self.report_name))
        dl_url = self.get_download_url()
        if dl_url is None:
            logging.warning('Could not retrieve url, returning blank df.')
            return pd.DataFrame()
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
            return pd.DataFrame(data=results, columns=vmc.r_cols[:2])
        results = self.check_reports_id(
            results, camp_col, success_msg, failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols[:2])

    def walmart_check(self):
        if 'ttd_api' not in self.login and 'wmt_api' in self.login:
            url = walmart_url
        else:
            url = ttd_url
        return url

