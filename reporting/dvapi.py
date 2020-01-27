import os
import sys
import json
import time
import shutil
import logging
import pandas as pd
import datetime as dt
import reporting.utils as utl
import selenium.webdriver as wd
import selenium.common.exceptions as ex


class DvApi(object):
    config_path = utl.config_path
    base_url = 'https://pinnacle.doubleverify.com/login'
    report_url = '{}/report-builder/my-reports'.format(
        base_url.replace('/login', ''))
    temp_path = 'tmp'
    def_dimensions = ['Advertiser Name', 'Date',
                      'Placement Code', 'Placement Name', 'Placement Size']
    def_metrics = ['Brand Safe Impressions', 'Brand Safety Blocks',
                   'Brand Safety Incidents', 'Fraud/SIVT Blocks',
                   'Fraud/SIVT Free Impressions', 'Fraud/SIVT Incidents']
    fb_dimensions = [
        'Date', 'FB Account ID', 'FB Account Name', 'FB Ad Name',
        'FB Ad Set Name', 'FB Campaign ID', 'FB Campaign Name',
        'FB Creative Name', 'FB Page Type', 'Inventory Type', 'Media Type']
    fb_metrics = [
        'Eligible Impressions', 'Measured Impressions', 'Monitored Impressions',
        'Fraud/SIVT Free Impressions', 'Fraud/SIVT Incidents',
        'Authentic Display Viewable Impressions',
        'Display Authentic Impressions', 'Display Eligible Impressions',
        'Display Measured Impressions', 'Display Monitored Impressions',
        'Display Passthrough Impressions',
        'Display Passthrough Measured Impressions',
        'Display Viewable Impressions', 'FB Display Net Served Impressions',
        'Video Eligible Impressions', 'Video Measured Impressions',
        'Video Viewable Impressions']

    def __init__(self):
        self.browser = self.init_browser()
        self.base_window = self.browser.window_handles[0]
        self.config_file = None
        self.username = None
        self.password = None
        self.advertiser = None
        self.campaign = None
        self.config_list = None
        self.config = None
        self.report_name = None
        self.report_type = None
        self.dimensions = self.def_dimensions
        self.metrics = self.def_metrics
        self.dim_end = 40
        self.metric_start = 41
        self.metric_end = 300
        self.metric_tab = 5
        self.path_to_report = ((1, 1), (2, 3), (3, 1))
        self.campaign_name = 'Campaign'
        self.advertiser_name = 'Advertiser Name'

    def input_config(self, config):
        logging.info('Loading DV config file: {}.'.format(config))
        self.config_file = os.path.join(self.config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.username = self.config['username']
        self.password = self.config['password']
        self.advertiser = self.config['advertiser']
        self.campaign = self.config['campaign']
        self.config_list = [self.username, self.password]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in config file. '
                                ' Aborting.'.format(item))
                sys.exit(0)

    def get_data_default_check(self, sd, ed, fields=None):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields and fields != ['nan']:
            for field in fields:
                if field == 'Facebook':
                    self.dimensions = self.fb_dimensions
                    self.metrics = self.fb_metrics
                    self.report_type = field
                    self.dim_end = 22
                    self.metric_start = 23
                    self.metric_end = 109
                    self.metric_tab = 3
                    self.path_to_report = ((1, 3), (10, 1), (11, 1))
                    self.advertiser_name = 'FB Account ID'
                    self.campaign_name = 'FB Campaign Name'
        return sd, ed, fields

    def init_browser(self):
        download_path = os.path.join(os.getcwd(), 'tmp')
        co = wd.chrome.options.Options()
        co.headless = True
        co.add_argument('--disable-features=VizDisplayCompositor')
        co.add_argument('--window-size=1920,1080')
        co.add_argument('--start-maximized')
        co.add_argument('--no-sandbox')
        co.add_argument('--disable-gpu')
        prefs = {'download.default_directory': download_path}
        co.add_experimental_option('prefs', prefs)
        browser = wd.Chrome(options=co)
        browser.maximize_window()
        browser.set_script_timeout(10)
        self.enable_download_in_headless_chrome(browser, download_path)
        return browser

    def enable_download_in_headless_chrome(self, driver, download_dir):
        # add missing support for chrome "send_command"  to selenium webdriver
        driver.command_executor._commands["send_command"] = \
            ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow', 'downloadPath': download_dir}}
        command_result = driver.execute("send_command", params)

    def go_to_url(self, url):
        logging.info('Going to url {}.'.format(url))
        try:
            self.browser.get(url)
        except ex.TimeoutException:
            logging.warning('Timeout exception, retrying.')
            self.go_to_url(url)
        time.sleep(5)

    def sign_in(self):
        logging.info('Signing in.')
        user_pass = [(self.username, '//*[@id="username"]'),
                     (self.password, '//*[@id="password"]')]
        for item in user_pass:
            elem = self.browser.find_element_by_xpath(item[1])
            elem.send_keys(item[0])
        login_xpaths = ['//*[@id="Login"]']
        for xpath in login_xpaths:
            self.click_on_xpath(xpath, sleep=5)

    def click_on_xpath(self, xpath, sleep=2):
        self.browser.find_element_by_xpath(xpath).click()
        time.sleep(sleep)

    def get_cal_month(self, lr=2, desired_date=None):
        month_xpath = '//*[@id="mat-datepicker-0"]/div[1]/div/button[2]' \
                      ''.format(lr)
        cal_date_xpath = '//*[@id="mat-datepicker-0"]/div[1]/div/button[1]'
        for x in range(12):
            cur_month = self.browser.find_element_by_xpath(cal_date_xpath).text
            if cur_month != desired_date.strftime('%b %Y').upper():
                self.click_on_xpath(month_xpath)
            else:
                break

    def set_date(self, date):
        self.get_cal_month(lr=2, desired_date=date)
        for row in range(1, 6):
            for col in range(1, 8):
                xpath = '//*[@id="mat-datepicker-0"]/div[2]/mat-month-view/' \
                        'table/tbody/tr[{}]/td[{}]'.format(row, col)
                try:
                    value = self.browser.find_element_by_xpath(xpath).text
                except:
                    break
                if str(value) == str(date.day):
                    self.click_on_xpath(xpath)
                    return

    def set_dates(self, sd, ed):
        logging.info('Setting dates to {} and {}.'.format(sd, ed))
        xpaths = ['//*[@id="reportBuilderForm"]/mat-card/rc-date-range/'
                  'mat-card/div[1]/dv-date-range-picker/div/div/span',
                  '//*[@id="cdk-overlay-1"]/div/div[9]/span',
                  '//*[@id="mat-input-2"]']
        for xpath in xpaths:
            self.click_on_xpath(xpath)
        self.set_date(sd)

    def find_report_in_table(self):
        for x in range(1, 10):
            xpath = ('//*[@id="myReportsWrapper"]/div[2]/dv-table/'
                     'dv-data-table/div/div[2]/mat-table/mat-row[{}]/'
                     'mat-cell[2]/a').format(x)
            value = self.browser.find_element_by_xpath(xpath).text
            if value == self.report_name:
                logging.info('Found report.')
                time.sleep(10)
                return x

    def click_on_report(self, row):
        xpath = ('/html/body/div[4]/rc-root/div/div/rc-my-reports/mat-card/'
                 'div[2]/dv-table/dv-data-table/div/div[2]/mat-table/'
                 'mat-row[{}]/mat-cell[8]/div/span[6]/a/i').format(row)
        self.click_on_xpath(xpath, sleep=5)

    def export_to_csv(self):
        logging.info('Downloading created report.')
        utl.dir_check(self.temp_path)
        row = self.find_report_in_table()
        xpath = ('/html/body/div[4]/rc-root/div/div/rc-my-reports/mat-card/'
                 'div[2]/dv-table/dv-data-table/div/div[2]/mat-table/'
                 'mat-row[{}]/mat-cell[8]/div/span[6]/a/i').format(row)
        for x in range(100):
            try:
                self.click_on_xpath(xpath, sleep=5)
                break
            except:
                logging.warning('Report not ready, attempt {}'.format(x + 1))
                time.sleep(5)

    def go_to_report_creation(self):
        xpath = '//*[@id="myReportsWrapper"]/div[1]/dv-deep-menu/button/span'
        self.click_on_xpath(xpath)
        # click standard
        for path in self.path_to_report:
            xpath = ('//*[@id="cdk-overlay-0"]/div/ul[{}]/li[{}]/'
                     'div/div'.format(path[0], path[1]))
            self.click_on_xpath(xpath)
        time.sleep(5)

    def click_on_filters(self, value):
        logging.info('Setting filters for {}'.format(value))
        campaigns = value.split(',')
        xpath = '//*[@id="mat-input-4"]'
        elem = self.browser.find_element_by_xpath(xpath)
        for c in campaigns:
            elem.send_keys(c)
            xpath = '//*[@id="mat-checkbox-{}"]/label/span'.format(
                self.metric_end + 1)
            self.click_on_xpath(xpath, sleep=5)
            elem.clear()
        xpath = ('//*[@id="mat-dialog-0"]/dv-popup-template/'
                 'div[3]/div/div/span/button')
        self.click_on_xpath(xpath, sleep=5)

    def click_on_dimensions(self, start_check=1, end_check=40):
        logging.info('Setting dimensions for report.')
        for x in range(start_check, end_check):
            xpath = '//*[@id="mat-checkbox-{}"]/label/span'.format(x)
            try:
                value = self.browser.find_element_by_xpath(xpath).text
            except:
                break
            if str(value) in self.dimensions:
                self.click_on_xpath(xpath, sleep=5)
            for filter_check in [(self.campaign, self.campaign_name),
                                 (self.advertiser, self.advertiser_name)]:
                if filter_check[0] and str(value) == filter_check[1]:
                    xpath = ('//*[@id="dimensionsComponent"]/div/div[2]/'
                             'div[{}]/rc-filters/div'.format(x))
                    self.click_on_xpath(xpath, sleep=5)
                    self.click_on_filters(filter_check[0])

    def click_on_metrics(self, start_check=41, end_check=300, last_tab=5):
        logging.info('Setting metrics for report.')
        tab = 0
        for x in range(start_check, end_check):
            xpath = '//*[@id="mat-checkbox-{}"]/label/span'.format(x)
            try:
                value = self.browser.find_element_by_xpath(xpath).text
            except:
                if tab > last_tab:
                    break
                else:
                    tab += 1
                    xpath = '//*[@id="mat-tab-label-0-{}"]/div'.format(tab)
                    self.click_on_xpath(xpath, sleep=5)
                    value = self.browser.find_element_by_xpath(xpath).text
            if str(value) in self.metrics:
                self.click_on_xpath(xpath, sleep=5)

    def give_report_name(self, sd):
        xpath = ('//*[@id="reportBuilderForm"]/mat-card/rc-title-bar/div[1]/'
                 'dv-header/div[2]/div[1]/input')
        report = self.browser.find_element_by_xpath(xpath)
        today = dt.datetime.today().strftime('%Y%m%d')
        sd = sd.strftime('%Y%m%d')
        self.report_name = '{}_{}_{}_{}'.format(today, sd, self.campaign,
                                                self.report_type)
        report.send_keys(self.report_name)
        time.sleep(5)

    def click_and_run(self):
        self.browser.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")
        xpath = '//*[@id="formButtons"]/div/span[2]/button/span'
        self.click_on_xpath(xpath, sleep=5)

    def create_report(self, sd, ed):
        logging.info('Creating report.')
        self.go_to_report_creation()
        self.set_dates(sd, ed)
        self.click_on_dimensions(start_check=1,
                                 end_check=self.dim_end)
        self.click_on_metrics(start_check=self.metric_start,
                              end_check=self.metric_end,
                              last_tab=self.metric_tab)
        self.give_report_name(sd)
        self.click_and_run()
        self.export_to_csv()

    @staticmethod
    def get_file_as_df(temp_path=None):
        df = pd.DataFrame()
        for x in range(100):
            logging.info('Checking for file.  Attempt {}.'.format(x + 1))
            files = os.listdir(temp_path)
            files = [x for x in files if x[-4:] == '.csv']
            if files:
                files = files[-1]
                logging.info('File downloaded.')
                temp_file = os.path.join(temp_path, files)
                time.sleep(5)
                df = utl.import_read_csv(temp_file)
                os.remove(temp_file)
                break
            time.sleep(5)
        shutil.rmtree(temp_path)
        return df

    def get_data(self, sd=None, ed=None, fields=None):
        self.quit()
        self.browser = self.init_browser()
        self.base_window = self.browser.window_handles[0]
        sd, ed, fields = self.get_data_default_check(sd, ed, fields)
        self.go_to_url(self.base_url)
        self.sign_in()
        self.go_to_url(self.report_url)
        self.create_report(sd, ed)
        df = self.get_file_as_df(self.temp_path)
        self.quit()
        return df

    def quit(self):
        self.browser.quit()
