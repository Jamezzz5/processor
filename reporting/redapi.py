import os
import pytz
import time
import json
import logging
import operator
import calendar
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import datetime as dt
import selenium.common.exceptions
import reporting.utils as utl
import selenium.common.exceptions as ex
import reporting.vmcolumns as vmc
from selenium.webdriver.common.keys import Keys


class RedApi(object):
    config_path = utl.config_path
    default_config_file_name = 'redapi.json'
    base_url = 'https://ads.reddit.com'
    temp_path = 'tmp'
    base_metric = '//*[@id="metrics.'
    video_metrics = [
        'videoViewableImpressions', 'videoFullyViewableImpressions',
        'videoPlaysWithSound', 'videoPlaysExpanded', 'videoWatches25',
        'videoWatches50', 'videoWatches75', 'videoWatches95', 'videoWatches100',
        'videoWatches3Secs', 'videoWatches10Secs']
    username_str = 'username'
    password_str = 'password'
    code_str = 'code'
    client_id_str = 'client_id'
    client_secret_str = 'client_secret'
    redirect_uri_str = 'redirect_uri'
    refresh_token_str = 'refresh_token'
    access_token_str = 'access_token'
    access_token_url = 'https://www.reddit.com/api/v1/access_token'
    version = '3'
    base_api_url = 'https://ads-api.reddit.com/api/v{}/'.format(version)
    business_url = '{}me/businesses'.format(base_api_url)
    spend_col = 'spend'
    cols = {
        'clicks': 'Clicks',
        'date': 'Date',
        'impressions': 'Impressions',
        'spend': 'Amount Spent (USD)',
        'video_started': 'Video Starts',
        'video_viewable_impressions': 'Video Views',
        'video_watched_100_percent': 'Watches at 100%',
        'video_watched_25_percent': 'Watches at 25%',
        'video_watched_50_percent': 'Watches at 50%',
        'video_watched_75_percent': 'Watches at 75%',
        'ad': 'Ad Name',
        'ad_group': 'Ad Group Name',
        'campaign': 'Campaign Name'
    }

    def __init__(self, headless=True):
        self.headless = headless
        self.sw = None
        self.browser = None
        self.config_file = None
        self.username = None
        self.password = None
        self.account = None
        self.config_list = None
        self.config = None
        self.key_list = [self.username_str, self.password_str]
        self.aborted = False
        self.api = True
        self.code = None
        self.client_id = None
        self.client_secret = None
        self.redirect_uri = None
        self.access_token = None
        self.refresh_token = None
        self.time_zone_id = 'America/Los_Angeles'
        self.headers = {}

    def input_config(self, config):
        logging.info('Loading Reddit config file: {}.'.format(config))
        self.config_file = os.path.join(self.config_path, config)
        self.load_config()
        if not self.aborted:
            self.check_config()

    def load_config(self):
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)
        self.username = self.config.get(self.username_str)
        self.password = self.config.get(self.password_str)
        self.code = self.config.get(self.code)
        self.client_id = self.config.get(self.client_id_str)
        self.client_secret = self.config.get(self.client_secret_str)
        self.redirect_uri = self.config.get(self.redirect_uri_str)
        self.code = self.config.get(self.code_str)
        self.access_token = self.config.get(self.access_token_str)
        self.refresh_token = self.config.get(self.refresh_token_str)
        self.config_list = [ self.username, self.password]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in config file.'.format(item))

    def get_data_default_check(self, sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None or ed.date() == dt.datetime.today().date():
            ed = dt.datetime.today()
        if fields:
            for val in fields:
                if str(val) != 'nan':
                    self.account = val
        return sd, ed

    def authorize_api(self, username, password, new_email):
        self.username = username
        self.password = password
        self.sw = utl.SeleniumWrapper(headless=self.headless)
        self.browser = self.sw.browser
        self.sw.go_to_url(self.base_url)
        sign_in_result = self.sign_in()
        if sign_in_result:
            ham_xpath = '//*[@id="app"]/div/div/div[1]/div/div[1]/button[1]'
            users_xpath = ('/html/body/div[12]/div/div[2]/div/nav/div/'
                           'div[2]/div/div[1]/a/div/span')
            invite_xpath = ('//*[@id="app"]/div/div/div[2]/div[2]/div/'
                            'div/div[1]/div/button/div')
            admin_xpath = ('/html/body/div[5]/div/div/div/div/div[2]'
                           '/div[2]/div/div/div[2]/div')
            xpaths = [ham_xpath, users_xpath, invite_xpath, admin_xpath]
            for xpath in xpaths:
                self.sw.click_on_xpath(xpath)
            email_xpath = """//*[@id="react-select-9-input"]"""
            elem = self.browser.find_element_by_xpath(email_xpath)
            elem.send_keys(new_email)
            next_xpath = ('/html/body/div[5]/div/div/div/div/'
                          'span[2]/button[2]/div')
            account_xpath = ('/html/body/div[5]/div/div/div/div/div[1]/div[1]/'
                             'div[2]/div[2]/div[1]/div[2]/div/div/label/input')
            new_admin_xpath = ('/html/body/div[5]/div/div/div/div/div[1]/'
                               'div[2]/div[2]/div/div/div[3]')
            confirm_xpath = ('/html/body/div[5]/div/div/div/div/span[2]/'
                             'button[3]/div')
            xpaths = [admin_xpath, next_xpath, account_xpath, new_admin_xpath,
                      confirm_xpath]
            for xpath in xpaths:
                self.sw.click_on_xpath(xpath)
            logging.info('Successfully invited for {}'.format(username))
        else:
            logging.warning('Sign in failed for {}'.format(username))
        self.sw.quit()

    def sign_in(self, attempt=0):
        logging.info('Signing in.: Attempt {}'.format(attempt))
        login_sel = ['log in', 'sign in']
        login_sel = ["[translate(normalize-space(text()), "
                     "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                     "'abcdefghijklmnopqrstuvwxyz')='{}']".format(x)
                     for x in login_sel]
        try:
            self.sw.click_on_xpath('//*[@id="Content"]/h2/a')
            self.sw.random_delay()
        except ex.NoSuchElementException as e:
            logging.warning('No logo, attempting footer.  Error: {}'.format(e))
            try:
                self.sw.click_on_xpath('//*[@id="Footer"]/p[2]/a')
                self.sw.random_delay()
            except ex.NoSuchElementException as e:
                logging.warning(
                    'No footer, attempting log in link.  Error: {}'.format(e))
                try:
                    self.sw.click_on_xpath('//a{}'.format(login_sel[0]))
                    self.sw.random_delay()
                except ex.NoSuchElementException as e:
                    logging.warning('Could not find Log In, rechecking.'
                                    '  Error: {}'.format(e))
                    self.sw.click_on_xpath("//*{}".format(login_sel[0]))
                    self.browser.switch_to.window(
                        self.browser.window_handles[-1])
        try:
            self.sw.browser.switch_to_alert().accept()
        except selenium.common.exceptions.NoAlertPresentException as e:
            logging.info('No alert: {}'.format(e))
        user_pass = [(self.username, '//*[@id="login-username"]'),
                     (self.password, '//*[@id="login-password"]')]
        for item in user_pass:
            elem = self.browser.find_element_by_xpath(item[1])
            elem.send_keys(item[0])
            self.sw.random_delay(0.3, 1)
            if item[0] == self.password:
                try:
                    elem.send_keys(Keys.ENTER)
                    self.sw.random_delay(1, 2)
                except ex.ElementNotInteractableException:
                    logging.info('Could not find field for {}'.format(item))
                except ex.StaleElementReferenceException:
                    logging.info('Could not find field for {}'.format(item))
        elem_id = 'automation-dashboard-viewSetUp'
        elem_load = self.sw.wait_for_elem_load(
            elem_id=elem_id, raise_exception=False)
        if not elem_load:
            logging.warning('{} did not load'.format(elem_id))
            self.sw.take_screenshot(file_name='reddit_error.jpg')
            self.sw.browser.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            self.sw.take_screenshot(file_name='reddit_error_2.jpg')
            return False
        error_xpath = '/html/body/div/div/div[2]/div/form/fieldset[2]/div'
        try:
            self.browser.find_element_by_xpath(error_xpath)
            logging.warning('Incorrect password, returning empty df.')
            return False
        except:
            pass
        if self.browser.current_url[:len(self.base_url)] != self.base_url:
            self.sw.go_to_url(self.base_url)
        else:
            logo_xpath = '//*[@id="app"]/div/div/div[1]/div/div[2]/a'
            self.sw.click_on_xpath(logo_xpath, sleep=5)
        if 'adsregister' in self.browser.current_url:
            logging.warning('Could not log in check username and password.')
            return False
        return True

    def set_breakdowns(self):
        logging.info('Setting breakdowns.')
        bd_xpath = '//button[contains(normalize-space(),"Breakdown")]'
        elem_found = self.sw.wait_for_elem_load(elem_id=bd_xpath,
                                                selector=self.sw.select_xpath)
        try:
            self.sw.click_on_xpath(bd_xpath)
        except ex.NoSuchElementException as e:
            msg = 'Could not click elem_found {}: {}'.format(elem_found, e)
            logging.warning(msg)
            self.sw.take_screenshot(file_name='reddit_error.jpg')
        bd_date_xpath = '//button[contains(normalize-space(),"Date")]'
        self.sw.click_on_xpath(bd_date_xpath)

    def get_cal_month(self, lr=1):
        cal_class = 'DayPicker-Caption'
        month = self.browser.find_elements_by_class_name(cal_class)
        month = month[lr - 1].text
        month = dt.datetime.strptime(month, '%B %Y')
        if lr == 2:
            last_day = calendar.monthrange(month.year, month.month)[1]
            month = month.replace(day=last_day)
        return month

    @staticmethod
    def get_comparison(lr=1):
        if lr == 1:
            comp = operator.gt
        else:
            comp = operator.lt
        return comp

    def change_month(self, date, lr, month):
        cal_el = self.browser.find_elements_by_class_name("DayPicker-NavButton")
        cal_el = cal_el[lr - 1]
        month_diff = abs((((month.year - date.year) * 12) +
                          month.month - date.month))
        for x in range(month_diff):
            self.sw.click_on_elem(cal_el, sleep=1)

    def go_to_month(self, date, left_month, right_month):
        if date < left_month:
            self.change_month(date, 1, left_month)
        if date > right_month:
            self.change_month(date, 2, right_month)

    def click_on_date(self, date):
        date = dt.datetime.strftime(date, '%a %b %d %Y')
        cal_date_xpath = "//div[@aria-label='{}']".format(date)
        self.sw.click_on_xpath(cal_date_xpath)

    def find_and_click_date(self, date, left_month, right_month):
        self.go_to_month(date, left_month, right_month)
        self.click_on_date(date)

    def set_date(self, date):
        left_month = self.get_cal_month(lr=1)
        right_month = self.get_cal_month(lr=2)
        self.find_and_click_date(date, left_month, right_month)

    def open_calendar(self, base_xpath):
        cal_button_xpath = '/div/div/div'
        cal_xpath = base_xpath + cal_button_xpath
        self.sw.click_on_xpath(cal_xpath)
        cal_table_xpath = '/html/body/div[8]/div/table/tbody/tr'
        return cal_table_xpath

    def set_dates(self, sd, ed, base_xpath=None):
        logging.info('Setting dates to {} and {}.'.format(sd, ed))
        self.open_calendar(base_xpath)
        self.set_date(sd)
        self.set_date(ed)
        elem = self.browser.find_elements_by_xpath(
            "//*[contains(text(), 'Update')]")
        if len(elem) > 1:
            elem = elem[1]
        else:
            elem = elem[0]
        self.sw.click_on_elem(elem)

    def click_individual_metrics(self):
        for metric in self.video_metrics:
            xpath = '{}{}"]'.format(self.base_metric, metric)
            self.sw.click_on_xpath(xpath, sleep=1)

    def click_grouped_metrics(self):
        xpath = '//span[contains(normalize-space(), "All metrics")]'
        elems = self.sw.browser.find_elements_by_xpath(xpath)
        for elem in elems[1:]:
            elem.click()

    def set_metrics(self):
        logging.info('Setting metrics.')
        columns_xpath = '//div[text()="Columns"]'
        customize_columns_xpath = ('//button[contains(normalize-space(),'
                                   '"Customize Columns")]')
        self.sw.click_on_xpath(columns_xpath)
        self.sw.click_on_xpath(customize_columns_xpath)
        self.click_grouped_metrics()
        apply_button_xpath = '//div[text()="Apply"]'
        self.sw.click_on_xpath(apply_button_xpath)

    def export_to_csv(self):
        logging.info('Downloading created report.')
        utl.dir_check(self.temp_path)
        export_xpath = '//button[contains(normalize-space(), "Export report")]'
        self.sw.click_on_xpath(export_xpath)
        download_xpath = (
            '//button[contains(normalize-space(), "Download .csv")]')
        try:
            self.sw.click_on_xpath(download_xpath)
        except ex.TimeoutException as e:
            logging.warning('Timed out - attempting again. {}'.format(e))
            self.sw.click_on_xpath(download_xpath)

    def get_base_xpath(self):
        base_app_xpath = '//*[@id="app"]/div/div[1]/div[2]/div'
        try:
            self.browser.find_element_by_xpath(base_app_xpath)
        except ex.NoSuchElementException:
            base_app_xpath = base_app_xpath[:-3]
        base_app_xpath += '/'
        return base_app_xpath

    def create_report(self, sd, ed):
        logging.info('Creating report.')
        base_app_xpath = self.get_base_xpath()
        self.set_breakdowns()
        self.set_dates(sd, ed, base_xpath=base_app_xpath)
        self.set_metrics()
        self.export_to_csv()

    def change_account(self):
        drop_class = 'automation-account-name'
        elem = self.browser.find_elements_by_class_name(drop_class)
        elem[0].click()
        account_xpath = '//a[text()="{}"]'.format(self.account)
        self.sw.click_on_xpath(account_xpath)

    def get_access_token(self):
        url = self.access_token_url
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': self.redirect_uri
        }
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token
        }
        auth = HTTPBasicAuth(self.client_id, self.client_secret)
        r = requests.post(url, headers=headers, data=data, auth=auth)
        self.access_token = r.json()['access_token']
        self.headers = {'Accept': 'application/json',
                        'Authorization': 'Bearer {}'.format(self.access_token)}

    def get_all_business_ids(self):
        """
        Get request to business url to retrieve list of business ids

        :return: business ids as a list
        """
        business_ids = []
        if not self.headers:
            self.get_access_token()
        url = self.business_url
        errors = 0
        for x in range(100):
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                if 'data' in r.json():
                    new_business_ids = [x['id'] for x in r.json()['data']]
                    business_ids.extend(new_business_ids)
                    url = r.json()['pagination']['next_url']
                    if not url:
                        break
                else:
                    logging.warning('Data not in response: {}'.format(r.json()))
                    errors += 1
                    if errors > 10:
                        break
                    time.sleep(5)
        return business_ids

    def get_ad_accounts_by_business(self, business_ids):
        """
        Loops through provided business ids list to get account id of username

        :param business_ids: List of business ids
        :return: account_id that matches username or blank
        """
        account_id = ''
        for business_id in business_ids:
            url = '{}businesses/{}/ad_accounts'.format(
                self.base_api_url, business_id)
            r = None
            for x in range(5):
                r = requests.get(url, headers=self.headers)
                if 'data' in r.json():
                    break
                else:
                    logging.warning('Data not in response: {}'.format(r.json()))
                    time.sleep(5)
            if not r:
                logging.warning('Could not get business {}'.format(business_id))
                continue
            account_ids = [x['id'] for x in r.json()['data']
                           if x['name'].lower() == self.username.lower()]
            if account_ids:
                account_id = account_ids[0]
                self.time_zone_id = r.json()['data'][0]['time_zone_id']
                break
        return account_id

    @staticmethod
    def timezone_to_utc(dt_naive, timezone='America/Los_Angeles'):
        """
        Converts a datetime to the specified timezone dt str

        :param dt_naive: A datetime without timezone information
        :param timezone: Str of the timezone to convert
        :return: The full str of the datetime with timezone
        """
        tz = pytz.timezone(timezone)
        zero_time = dt_naive.replace(minute=0, second=0, microsecond=0)
        dt_local = tz.localize(zero_time)
        dt_utc = dt_local.astimezone(pytz.utc)
        return dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    def get_report(self, account_id, sd, ed):
        """
        Given an account_id requests a report for the start and end dates.

        :param account_id: Account ID to pull as a str
        :param sd: Start date to pull from
        :param ed: End date to pull from
        :return:
        """
        response_list = []
        next_url = '{}ad_accounts/{}/reports'.format(self.base_api_url, account_id)
        breakdowns = ['AD_ID', 'DATE']
        starts_at = self.timezone_to_utc(sd, timezone=self.time_zone_id)
        ends_at = self.timezone_to_utc(ed, timezone=self.time_zone_id)
        fields = ['IMPRESSIONS', 'CLICKS', 'SPEND', 'VIDEO_STARTED',
                  'VIDEO_WATCHED_25_PERCENT', 'VIDEO_WATCHED_50_PERCENT',
                  'VIDEO_WATCHED_75_PERCENT', 'VIDEO_WATCHED_100_PERCENT',
                  'VIDEO_WATCHED_3_SECONDS', 'VIDEO_WATCHED_5_SECONDS',
                  'VIDEO_WATCHED_10_SECONDS',
                  'VIDEO_VIEW_RATE', 'VIDEO_VIEWABLE_IMPRESSIONS',
                  'VIDEO_PLAYS_EXPANDED']
        data = {'starts_at': starts_at,
                'ends_at': ends_at,
                'breakdowns': breakdowns,
                'fields': fields,
                'time_zone_id': self.time_zone_id}
        data = {'data': data}
        params = {'page.size': 1000}
        for attempt in range(1000):
            msg = ('Getting account {} data from {} to {} {}.  '
                   'Attempt {}').format(
                account_id, sd, ed, self.time_zone_id, attempt + 1)
            logging.info(msg)
            r = requests.post(next_url, headers=self.headers, json=data,
                              params=params)
            response = r.json()
            if 'data' not in response:
                logging.warning('Data not in response: {}'.format(response))
                break
            response_list.extend(response['data']['metrics'])
            next_url = response['pagination']['next_url']
            if not next_url:
                break
        if next_url:
            logging.warning('Additional pages not retrieved.')
        logging.info('All reports retrieved.')
        return response_list

    def get_object_names_from_df(self, df, col='ad_id', new_col='ad_group_id'):
        """
        Loops through unique values in col of dataframe to get names from api

        :param df: Dataframe to loop over
        :param col: Column with id to get names from
        :param new_col: New id column created
        :return: Dataframe with the name added
        """
        ad_info_list = []
        url_str = col.replace('_id', 's')
        new_col_name = col.replace('_id', '')
        if col not in df.columns:
            logging.warning('Column {} not in dataframe'.format(col))
            return df
        ad_ids = df[col].unique()
        logging.info('Getting names for {} {}'.format(len(ad_ids), url_str))
        for ad_id in ad_ids:
            url = '{}{}/{}'.format(self.base_api_url, url_str, ad_id)
            name = ''
            ad_data = {}
            for x in range(5):
                r = requests.get(url, headers=self.headers)
                resp_json = r.json()
                ad_data = resp_json.get('data', {})
                name = ad_data.get('name')
                if name:
                    break
                logging.warning('No name in response: {}'.format(resp_json))
                time.sleep(1)
            ad_info = {
                col: ad_id,
                new_col_name: name
            }
            if new_col:
                ad_info[new_col] = ad_data.get(new_col)
            ad_info_list.append(ad_info)
        tdf = pd.DataFrame(ad_info_list)
        df = pd.merge(df, tdf, on=col, how='left')
        return df

    def add_names_to_df(self, df):
        """
        Takes a dataframe with ad_id, adds names for ad, adgroup and campaign

        :param df: Dataframe with ad_id
        :return: Dataframe with names added
        """
        object_types = [
            ('ad_id', 'ad_group_id'),
            ('ad_group_id', 'campaign_id'),
            ('campaign_id', '')]
        for object_type in object_types:
            col = object_type[0]
            new_col = object_type[1]
            df = self.get_object_names_from_df(df, col=col, new_col=new_col)
        return df

    def report_to_df(self, response_list):
        """
        Creates a dataframe from a list of dicts.

        :param response_list: List of dicts with ad_id date and metrics
        :return: The dataframe with all requested data
        """
        logging.info('Attempting to create df of {} rows'.format(
            len(response_list)))
        df = pd.DataFrame(response_list)
        if df.empty:
            df = pd.DataFrame(columns=list(self.cols.keys()))
        if self.spend_col in df.columns:
            df[self.spend_col] = df[self.spend_col] / 1_000_000
        df = self.add_names_to_df(df)
        df = self.rename_columns(df)
        return df

    def rename_columns(self, df):
        """
        Renames the df columns to match manual export

        :param df: The dataframe for columns to change
        :return: Dataframe with renamed columns
        """
        df = df.rename(columns=self.cols)
        return df

    def get_data_api(self, sd, ed):
        """
        Pulls data for specified start and end date from api using requests.

        :param sd: The start date to pull from
        :param ed: The end date to pull to
        :return: df The dataframe of data from the platform
        """
        df = pd.DataFrame()
        self.get_access_token()
        business_ids = self.get_all_business_ids()
        account_id = self.get_ad_accounts_by_business(business_ids)
        if account_id:
            r = self.get_report(account_id, sd, ed)
            df = self.report_to_df(r)
        return df

    def get_data(self, sd=None, ed=None, fields=None):
        """
        Main function to get data. Checks whether to use api or selenium.

        :param sd: The start date to pull from
        :param ed: The end date to pull to
        :param fields: Additional field information
        :return: The dataframe of data from the platform.
        """
        sd, ed = self.get_data_default_check(sd, ed, fields)
        if self.api:
            df = self.get_data_api(sd, ed)
        else:
            df = self.get_data_selenium(sd, ed)
        return df

    def get_data_selenium(self, sd, ed):
        self.sw = utl.SeleniumWrapper(headless=self.headless)
        self.browser = self.sw.browser
        sign_in_result = False
        for x in range(3):
            self.sw.go_to_url(self.base_url)
            sign_in_result = self.sign_in(attempt=x + 1)
            if sign_in_result:
                break
        if not sign_in_result:
            self.sw.quit()
            return pd.DataFrame()
        if self.account:
            self.change_account()
        self.create_report(sd, ed)
        df = self.sw.get_file_as_df(self.temp_path)
        self.sw.quit()
        return df

    def check_credentials(self, results, camp_col, success_msg, failure_msg):
        self.sw = utl.SeleniumWrapper(headless=self.headless)
        self.browser = self.sw.browser
        self.sw.go_to_url(self.base_url)
        sign_in_check = self.sign_in()
        self.sw.quit()
        if not sign_in_check:
            msg = ' '.join([failure_msg, 'Incorrect User or password. '
                                         'Check Active and Permissions.'])
            row = [camp_col, msg, False]
            results.append(row)
        else:
            msg = ' '.join(
                [success_msg, 'User or password are corrects:'])
            row = [camp_col, msg, True]
            results.append(row)
        return results

    def test_connection(self, acc_col, camp_col, acc_pre):
        success_msg = 'SUCCESS:'
        failure_msg = 'FAILURE:'
        results = self.check_credentials(
            [], acc_col, success_msg, failure_msg)
        if False in results[0]:
            return pd.DataFrame(data=results, columns=vmc.r_cols)
