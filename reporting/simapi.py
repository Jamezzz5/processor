import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import reporting.utils as utl
import reporting.vmcolumns as vmc


class SimApi(object):
    config_path = utl.config_path
    url = 'https://api.similarweb.com'
    batch_url = '/v3/batch'
    batch_v4_url = '/batch/v4'
    rest_url = '/v4/website'
    validate_url = '/request-validate'
    website_url = '/traffic_and_engagement'
    request_url = '/request-report'
    status_url = '/request-status/'
    retry_url = '/retry/'
    vtable = 'traffic_and_engagement'
    default_config_file_name = 'simconfig.json'
    default_metrics = [
        'all_traffic_visits', 'desktop_visits', 'mobile_visits',
        'all_traffic_bounce_rate', 'desktop_bounce_rate',
        'mobile_bounce_rate', 'all_traffic_pages_per_visit',
        'desktop_pages_per_visit', 'mobile_pages_per_visit',
        'all_traffic_average_visit_duration',
        'desktop_average_visit_duration',
        'mobile_average_visit_duration', 'desktop_unique_visitors',
        'mobile_unique_visitors', 'deduplicated_audience']

    def __init__(self):
        self.config = None
        self.config_file = None
        self.config_list = None
        self.api_key = None
        self.headers = None
        self.domains = None
        self.countries = None
        self.report_id = None
        self.use_v4 = None
        self.r = None
        self.df = pd.DataFrame()

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix. '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Sim config file: {}'.format(config))
        self.config_file = os.path.join(self.config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found. Aborting.'.format(self.config_file))
            sys.exit(0)
        self.domains = self.config['domains']
        self.countries = self.config['countries']
        self.api_key = self.config['api_key']
        self.config_list = [self.config, self.api_key, self.domains,
                            self.countries]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Sim config file'
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        headers = {
            'accept': 'application/json',
            'content-type': 'application/json',
            'api-key': self.api_key
        }
        return headers

    def get_metrics(self):
        """Metrics for the report request.

        :returns: `metrics` list from the config when present (a comma
            separated string also works), otherwise default_metrics
        """
        metrics = (self.config or {}).get('metrics')
        if metrics:
            if isinstance(metrics, str):
                metrics = metrics.split(',')
            return metrics
        return self.default_metrics

    def construct_payload(self, sd, ed):
        payload = {'metrics': self.get_metrics(),
                   'filters': {
                       'domains': self.domains.split(','),
                       'countries': self.countries.split(','),
                       'include_subdomains': True
                   },
                   'granularity': 'monthly',
                   'start_date': sd.strftime("%Y-%m-%d"),
                   'end_date': ed.strftime("%Y-%m-%d"),
                   'response_format': 'csv',
                   'delivery_method': 'download_link'
                   }
        return payload

    def construct_payload_v4(self, sd, ed):
        """Build the batch/v4 request body.

        v4 nests the report definition under report_query/tables with an
        explicit vtable and takes monthly dates as YYYY-MM.

        :param sd: start date as a datetime
        :param ed: end date as a datetime
        :returns: dict request body
        """
        table = {
            'vtable': self.vtable,
            'granularity': 'monthly',
            'filters': {
                'domains': self.domains.split(','),
                'countries': self.countries.split(','),
                'include_subdomains': True
            },
            'metrics': self.get_metrics(),
            'start_date': sd.strftime('%Y-%m'),
            'end_date': ed.strftime('%Y-%m')
        }
        payload = {
            'report_name': 'lqapp_{}'.format(self.vtable),
            'report_query': {'tables': [table]},
            'delivery_information': {
                'delivery_method': 'download_link',
                'response_format': 'csv'
            }
        }
        return payload

    def request_with_retry(self, url, method='GET', json_body=None,
                           attempts=3, timeout=120):
        """Make an http request retrying dropped connections.

        Non-200 responses are returned to the caller untouched; only
        transport level failures are retried.

        :param url: string url to request
        :param method: 'GET' or 'POST'
        :param json_body: dict body sent as json for POST requests
        :param attempts: integer maximum connection attempts
        :param timeout: integer socket timeout in seconds
        :returns: requests response, or None when the connection never
            succeeded
        """
        headers = self.set_headers()
        for x in range(attempts):
            try:
                if method == 'POST':
                    return requests.post(url, headers=headers,
                                         json=json_body, timeout=timeout)
                return requests.get(url, headers=headers, timeout=timeout)
            except requests.exceptions.RequestException as e:
                logging.warning('Connection error retrying 60s: {}'.format(e))
                time.sleep(60)
        logging.warning('Connection failed after {} attempts.'.format(
            attempts))
        return None

    def build_request_url(self, endpoint, use_v4):
        if use_v4:
            return '{}{}{}'.format(self.url, self.batch_v4_url, endpoint)
        return '{}{}{}{}'.format(self.url, self.batch_url, self.website_url,
                                 endpoint)

    def send_report_request(self, endpoint, sd, ed):
        """POST a report payload, falling back from v3 to v4.

        The first request that is not rejected as a missing endpoint
        pins the api version for the rest of the run.

        :param endpoint: request_url or validate_url
        :param sd: start date as a datetime
        :param ed: end date as a datetime
        :returns: requests response, or None on connection failure
        """
        if self.use_v4 is None:
            versions = [False, True]
        else:
            versions = [self.use_v4]
        r = None
        for use_v4 in versions:
            url = self.build_request_url(endpoint, use_v4)
            if use_v4:
                payload = self.construct_payload_v4(sd, ed)
            else:
                payload = self.construct_payload(sd, ed)
            r = self.request_with_retry(url, method='POST',
                                        json_body=payload)
            if r is None:
                return None
            if r.status_code in (404, 410) and not use_v4:
                logging.warning('v3 endpoint missing (code {}), retrying '
                                'with v4.'.format(r.status_code))
                continue
            self.use_v4 = use_v4
            return r
        return r

    # Uses Data Credits
    def make_request(self, sd, ed):
        r = self.send_report_request(self.request_url, sd, ed)
        if r is None:
            return None
        if r.status_code == 200:
            report_id = r.json().get('report_id')
            if not report_id:
                logging.warning('No report id in response: {}'.format(r.text))
            return report_id
        elif r.status_code == 400:
            logging.warning('Metrics are not available in table: {}'.format(
                r.text))
        else:
            logging.warning('Unexpected status code '
                            '{}: {}'.format(r.status_code, r.text))
        return None

    def check_request_valid(self, sd, ed):
        """Pre-flight the report against the free validate endpoint.

        Advisory only: a validation that cannot be reached never blocks
        the pull, but an explicit is_valid false does, since submitting
        it would waste the monthly data credit budget.

        :param sd: start date as a datetime
        :param ed: end date as a datetime
        :returns: boolean of whether to proceed with the report request
        """
        results = self.make_validate_request(sd, ed)
        if not results:
            return True
        if results.get('is_valid') is False:
            logging.warning('Report request invalid: {}'.format(
                results.get('warnings')))
            return False
        if 'estimated_credits' in results:
            logging.info('Report estimated credits: {}'.format(
                results['estimated_credits']))
        return True

    # Uses Data Credits
    def get_data(self, sd=None, ed=None, fields=None):
        """Pull traffic and engagement data for the configured domains.

        Validates the request for free first, reuses a stored report id
        only while it is still usable and otherwise builds a fresh
        report request from the configured metrics, so a stale id can
        never wedge the pull.

        :param sd: start date, defaults to 35 days ago
        :param ed: end date, defaults to 30 days ago
        :param fields: unused, kept for the api interface
        :returns: dataframe of the report, empty on failure
        """
        sd, ed = self.get_data_default_check(sd, ed)
        if self.config is None:
            self.config = {}
        self.df = pd.DataFrame()
        if not self.check_request_valid(sd, ed):
            logging.warning('Request invalid, returning empty df.')
            return self.df
        report_id = self.config.get('report_id')
        if report_id:
            df = self.check_report_status(report_id)
            if df is None:
                logging.warning('Stored report id {} unusable, requesting '
                                'a new report.'.format(report_id))
                self.config['report_id'] = None
                report_id = None
            else:
                self.df = df
        if not report_id:
            report_id = self.make_request(sd, ed)
            if not report_id:
                logging.warning('No report id, returning empty df.')
                return self.df
            self.config['report_id'] = report_id
            df = self.check_report_status(report_id)
            if df is not None:
                self.df = df
        self.check_empty_df()
        return self.df

    def request_report_retry(self, report_id):
        """POST the free retry endpoint after an internal_error.

        Retrying a failed report does not consume data credits.

        :param report_id: string id of the failed report
        """
        url = '{}{}{}{}'.format(self.url, self.batch_url, self.retry_url,
                                report_id)
        logging.info('Report hit internal error, requesting free retry.')
        self.request_with_retry(url, method='POST')

    def check_report_status(self, report_id, max_attempts=60,
                            initial_delay=5, backoff_factor=1.1):
        """Poll a report until it completes, handling every status.

        Statuses pending/processing/retry keep polling with exponential
        backoff, internal_error triggers the free retry endpoint and
        bad_request or an unknown report id give up immediately.

        :param report_id: string id of the requested report
        :param max_attempts: integer maximum status polls
        :param initial_delay: integer seconds before the first re-poll
        :param backoff_factor: float multiplier per poll, capped at 60s
        :returns: dataframe of the report (empty when it never
            completed), or None when the report id is unusable and a
            fresh request may succeed
        """
        url = '{}{}{}{}'.format(self.url, self.batch_url, self.status_url,
                                report_id)
        delay = initial_delay
        error_count = 0
        retries_requested = 0
        for x in range(1, max_attempts + 1):
            r = self.request_with_retry(url)
            if r is None:
                return None
            if r.status_code != 200:
                logging.warning('Report status request failed with code '
                                '{}: {}'.format(r.status_code, r.text))
                return None
            status = r.json().get('status')
            if status == 'completed':
                return self.download_report(r.json()['download_url'])
            elif status == 'bad_request':
                logging.warning('Report request was invalid: {}'.format(
                    r.text))
                return None
            elif status == 'internal_error':
                if retries_requested >= 2:
                    logging.warning('Report failed after retries: {}'.format(
                        r.text))
                    return None
                retries_requested += 1
                self.request_report_retry(report_id)
            elif status not in ('pending', 'processing', 'retry'):
                error_count += 1
                logging.warning('Unexpected report status: {}'.format(
                    status))
                if error_count > 5:
                    return None
            logging.info('Report not ready (status {}), attempt {}, '
                         'waiting {:.0f}s.'.format(status, x, delay))
            time.sleep(delay)
            delay = min(delay * backoff_factor, 60)
        logging.warning('Report still not ready after {} '
                        'attempts.'.format(max_attempts))
        return pd.DataFrame()

    def download_report(self, download_url):
        """Download a completed report, retrying transient failures.

        :param download_url: string url of the generated report, valid
            for 30 days
        :returns: dataframe of the report, empty when unreadable
        """
        logging.info('Found report url, downloading.')
        for x in range(3):
            try:
                return pd.read_csv(download_url)
            except Exception as e:
                logging.warning('Could not download report, retrying: '
                                '{}'.format(e))
                time.sleep(30)
        logging.warning('Download failed, returning empty df.')
        return pd.DataFrame()

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=35)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=30)
        return sd, ed

    def check_empty_df(self):
        if (self.df is None or self.df.empty or self.df.iloc[0, 0] ==
                'No data returned'):
            logging.warning('No data in response, returning empty df.')
            self.df = pd.DataFrame()

    def make_validate_request(self, sd=None, ed=None):
        sd, ed = self.get_data_default_check(sd, ed)
        r = self.send_report_request(self.validate_url, sd, ed)
        if r is None or r.status_code != 200:
            if r is not None:
                logging.warning('Validate request failed with code '
                                '{}: {}'.format(r.status_code, r.text))
            return {}
        return r.json()

    def check_estimated_credits(self, acc_col, success_msg, failure_msg):
        r = self.make_validate_request()
        results = []
        if 'estimated_credits' in r:
            results_text = r['estimated_credits']
            if results_text <= 3000:
                row = [acc_col, ' '.join([success_msg, json.dumps(r)]),
                       True]
                results.append(row)
        else:
            msg = ('This request is using over 3000 credits,'
                   ' Be aware that we only have 25000 per month.'
                   ' Double check your settings and'
                   ' if everything is correct continue'
                   '\n Warning:')
            row = [acc_col, ' '.join([failure_msg, msg]), False]
            results.append(row)
        return results

    def test_connection(self, acc_col=None, camp_col=None, acc_pre=None):
        success_msg = 'SUCCESS:'
        failure_msg = 'WARNING:'
        self.set_headers()
        results = self.check_estimated_credits(acc_col, success_msg,
                                               failure_msg)
        return pd.DataFrame(data=results, columns=vmc.r_cols)
