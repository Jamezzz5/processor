import os
import sys
import json
import time
import shutil
import logging
import operator
import calendar
import pandas as pd
import datetime as dt
import reporting.utils as utl
import selenium.webdriver as wd
import selenium.common.exceptions as ex


class RedApi(object):
    config_path = utl.config_path
    base_url = 'https://ads.reddit.com'
    temp_path = 'tmp'
    base_metric = '//*[@id="metrics.'
    video_metrics = [
        'videoViewableImpressions', 'videoFullyViewableImpressions',
        'videoPlaysWithSound', 'videoPlaysExpanded', 'videoWatches25',
        'videoWatches50', 'videoWatches75', 'videoWatches95', 'videoWatches100',
        'videoWatches3Secs', 'videoWatches10Secs']

    def __init__(self):
        self.browser = self.init_browser()
        self.base_window = self.browser.window_handles[0]
        self.config_file = None
        self.username = None
        self.password = None
        self.account = None
        self.config_list = None
        self.config = None

    def input_config(self, config):
        logging.info('Loading Reddit config file: {}.'.format(config))
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
        self.config_list = [self.username, self.password]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in config file. '
                                ' Aborting.'.format(item))
                sys.exit(0)

    def get_data_default_check(self, sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None or ed.date() == dt.datetime.today().date():
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields:
            for val in fields:
                if str(val) != 'nan':
                    self.account = val
        return sd, ed

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

    @staticmethod
    def enable_download_in_headless_chrome(driver, download_dir):
        # add missing support for chrome "send_command"  to selenium webdriver
        driver.command_executor._commands["send_command"] = \
            ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow', 'downloadPath': download_dir}}
        driver.execute("send_command", params)

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
        try:
            self.click_on_xpath('//*[@id="Content"]/h2/a')
        except ex.NoSuchElementException as e:
            logging.warning('No logo, attempting footer.  Error: {}'.format(e))
            try:
                self.click_on_xpath('//*[@id="Footer"]/p[2]/a')
            except ex.NoSuchElementException as e:
                logging.warning(
                    'No footer, attempting log in link.  Error: {}'.format(e))
                self.click_on_xpath("//a[text()='Log In']")
        user_pass = [(self.username, '//*[@id="loginUsername"]'),
                     (self.password, '//*[@id="loginPassword"]')]
        for item in user_pass:
            elem = self.browser.find_element_by_xpath(item[1])
            elem.send_keys(item[0])
        time.sleep(2)
        login_xpaths = ['//button[normalize-space(text())="{}"]'.format(x)
                        for x in ['Sign in', 'Log in', 'Log In']]
        for xpath in login_xpaths:
            try:
                self.click_on_xpath(xpath, sleep=5)
                break
            except:
                logging.warning('Could not click xpath: {}'.format(xpath))
        error_xpath = '/html/body/div/div/div[2]/div/form/fieldset[2]/div'
        try:
            self.browser.find_element_by_xpath(error_xpath)
            logging.warning('Incorrect password, returning empty df.')
            return False
        except:
            pass
        if self.browser.current_url[:len(self.base_url)] != self.base_url:
            self.go_to_url(self.base_url)
        else:
            logo_xpath = '//*[@id="app"]/div/div[1]/div[1]/div/a/img'
            self.click_on_xpath(logo_xpath, sleep=5)
        return True

    def click_on_xpath(self, xpath, sleep=2):
        self.browser.find_element_by_xpath(xpath).click()
        time.sleep(sleep)

    def set_breakdowns(self):
        logging.info('Setting breakdowns.')
        bd_xpath = '//div[text()="Breakdown"]'
        self.click_on_xpath(bd_xpath)
        bd_date_xpath = '//button[contains(normalize-space(),"Date")]'
        self.click_on_xpath(bd_date_xpath)

    def get_cal_month(self, lr=1, cal_xpath=None):
        month_xpath = '[2]/div[{}]/div[1]/div'.format(lr)
        cal_month_xpath = cal_xpath + month_xpath
        try:
            month = self.browser.find_element_by_xpath(cal_month_xpath).text
        except ex.NoSuchElementException as e:
            logging.warning('Could not click update trying another selector.'
                            '  Error: {}'.format(e))
            cal_month_xpath = cal_month_xpath.replace('7', '8')
            month = self.browser.find_element_by_xpath(cal_month_xpath).text
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

    def change_month(self, date, lr, cal_xpath, month):
        cal_sel_xpath = cal_xpath + '[1]/span[{}]'.format(lr)
        month_diff = abs((((month.year - date.year) * 12) +
                          month.month - date.month))
        for x in range(month_diff):
            self.click_on_xpath(cal_sel_xpath, sleep=1)

    def go_to_month(self, date, left_month, right_month, cal_xpath):
        if date < left_month:
            self.change_month(date, 1, cal_xpath, left_month)
        if date > right_month:
            self.change_month(date, 2, cal_xpath, right_month)

    def click_on_date(self, date):
        date = dt.datetime.strftime(date, '%a %b %d %Y')
        cal_date_xpath = "//div[@aria-label='{}']".format(date)
        self.click_on_xpath(cal_date_xpath)

    def find_and_click_date(self, date, left_month, right_month, cal_xpath):
        self.go_to_month(date, left_month, right_month, cal_xpath)
        self.click_on_date(date)

    def set_date(self, date, cal_xpath=None):
        cal_xpath = cal_xpath + '[1]/td[1]/div/div/div'
        left_month = self.get_cal_month(lr=1, cal_xpath=cal_xpath)
        right_month = self.get_cal_month(lr=2, cal_xpath=cal_xpath)
        self.find_and_click_date(date, left_month, right_month, cal_xpath)

    def open_calendar(self, base_xpath):
        cal_button_xpath = '/div/div/div'
        cal_xpath = base_xpath + cal_button_xpath
        self.click_on_xpath(cal_xpath)
        cal_table_xpath = '/html/body/div[8]/div/table/tbody/tr'
        return cal_table_xpath

    def set_dates(self, sd, ed, base_xpath=None):
        logging.info('Setting dates to {} and {}.'.format(sd, ed))
        cal_xpath = self.open_calendar(base_xpath)
        self.set_date(sd, cal_xpath=cal_xpath)
        self.set_date(ed, cal_xpath=cal_xpath)
        try:
            self.click_on_xpath(
                cal_xpath + '[2]/td/div/div[2]/div[2]/button[2]')
        except ex.NoSuchElementException as e:
            logging.warning('Could not click update trying another selector.'
                            '  Error: {}'.format(e))
            self.click_on_xpath(cal_xpath + '[2]/td/div/div[2]/button[2]/div')

    def click_individual_metrics(self):
        for metric in self.video_metrics:
            xpath = '{}{}"]'.format(self.base_metric, metric)
            self.click_on_xpath(xpath, sleep=1)

    def click_grouped_metrics(self):
        for x in range(2, 6):
            metric_xpath = ('/html/body/div[4]/div/div/div/div/div[2]/div[2]/'
                            'div/ul/li[{}]/div/div/label/i'.format(x))
            self.click_on_xpath(metric_xpath, sleep=1)

    def set_metrics(self, base_xpath):
        logging.info('Setting metrics.')
        metric_button_xpath = 'button/div'
        metric_xpath = base_xpath + metric_button_xpath
        self.click_on_xpath(metric_xpath)
        self.click_grouped_metrics()
        apply_button_xpath = '//div[text()="Apply"]'
        self.click_on_xpath(apply_button_xpath)

    def export_to_csv(self):
        logging.info('Downloading created report.')
        utl.dir_check(self.temp_path)
        export_xpath = '//div[normalize-space(text())="Export report"]'
        self.click_on_xpath(export_xpath)
        download_xpath = '//button[normalize-space(text())="Download .csv"]'
        self.click_on_xpath(download_xpath)

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
        self.set_metrics(base_xpath=base_app_xpath)
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
                df = utl.import_read_csv(temp_file, empty_df=True)
                os.remove(temp_file)
                break
            time.sleep(5)
        shutil.rmtree(temp_path)
        return df

    def change_account(self):
        account_url = self.browser.current_url.replace(
            'dashboard?entity=campaigns', 'accounts')
        self.go_to_url(account_url)
        account_xpath = '//a[text()="{}"]'.format(self.account)
        self.click_on_xpath(account_xpath)

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed, fields)
        self.go_to_url(self.base_url)
        sign_in_result = self.sign_in()
        if not sign_in_result:
            self.quit()
            return pd.DataFrame()
        if self.account:
            self.change_account()
        self.create_report(sd, ed)
        df = self.get_file_as_df(self.temp_path)
        self.quit()
        return df

    def quit(self):
        self.browser.quit()
