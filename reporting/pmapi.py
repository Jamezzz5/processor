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
from selenium.webdriver.common.keys import Keys


class PmApi(object):
    config_path = utl.config_path
    base_url = 'https://explorer.pathmatics.com'
    temp_path = 'tmp'
    base_metric = '//*[@id="export-menu-grid"]/div/div/'
    metrics = {'daily_spend': 'div[3]/div', 'daily_imps': 'div[4]/div',
               'top_sites': 'div[6]/div'}

    def __init__(self):
        self.browser = self.init_browser()
        self.base_window = self.browser.window_handles[0]
        self.config_file = None
        self.username = None
        self.password = None
        self.account = None
        self.config_list = None
        self.config = None
        self.title = None

    def input_config(self, config):
        logging.info('Loading Pathmatics config file: {}.'.format(config))
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
        self.title = self.config['campaign_filter']

    def check_config(self):
        if self.config['campaign_filter'] == '':
            logging.warning('{} not in config file. '
                            ' Aborting.'.format(self.config['campaign_filter']))
            sys.exit(0)

    def get_data_default_check(self, sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
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
        browser.set_page_load_timeout(3)
        browser.set_script_timeout(10)
        self.enable_download_in_headless_chrome(browser, download_path)
        return browser

    @staticmethod
    def enable_download_in_headless_chrome(driver, download_dir):
        # add missing support for chrome "send_command"  to selenium webdriver
        driver.command_executor._commands["send_command"] = \
            ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow',
                             'downloadPath': download_dir}}
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
        user_pass = [(self.username, '//*[@id="signin-username"]'),
                     (self.password, '//*[@id=\"signin-password\"]')]
        submit_path = '//*[@id=\"signin-button\"]'

        for item in user_pass:
            elem = self.browser.find_element_by_xpath(item[1])
            elem.send_keys(item[0])
            self.click_on_xpath(submit_path, sleep=5)
        time.sleep(2)

    def find_elem(self, xpath):
        elem = self.browser.find_element_by_xpath(xpath)
        return elem

    def click_on_xpath(self, xpath, sleep=2):
        self.find_elem(xpath).click()
        time.sleep(sleep)

    def search_title(self, title):
        self.browser.implicitly_wait(10)
        title_bar = \
            self.browser.find_element_by_xpath('//*[@id=\"omnibox-text\"]')
        title_bar.send_keys(title)
        top_result = '//a[@class=\"top-result top-result-0 brandtag\"]'
        self.click_on_xpath(top_result)

    def open_calendar(self):
        cal_button_xpath = '//*[@id="dates-filter-button"]/a'
        self.click_on_xpath(cal_button_xpath)
        custom_date_xpath = '//*[@id="date-filter-menu"]/div/div[1]/div[10]'
        self.click_on_xpath(custom_date_xpath)

    def delete_contents(self, date_path):
        elem = self.find_elem(date_path)
        elem.send_keys(Keys.CONTROL + "a")
        elem.send_keys(Keys.DELETE)

    def change_dates(self, date, date_path):
        self.delete_contents(date_path)
        self.find_elem(date_path).send_keys(date)

    def set_dates(self, sd, ed):
        self.open_calendar()
        base_path = '//*[@id="date-filter-menu-date-picker-container"]'
        sd_path = base_path + '/div[1]/div[1]/input'
        ed_path = base_path + '/div[1]/div[3]/input'
        start_end = [[sd.date().__str__(), sd_path],
                     [ed.date().__str__(), ed_path]]
        for date_info in start_end:
            self.change_dates(date_info[0], date_info[1])
        done_path = '//*[@id="date-filter-menu-date-picker-button"]/a/span'
        self.click_on_xpath(done_path)

    def create_report(self, sd, ed, title):
        self.search_title(title)
        self.set_dates(sd, ed)

    def set_metrics(self):
        for metric in self.metrics:
            metric_path = self.base_metric + self.metrics[metric]
            self.click_on_xpath(metric_path)

    def export_to_csv(self):
        export_xpath = '//*[@id=\"export-button\"]'
        self.click_on_xpath(export_xpath)
        xlsx_path = '//*[@id="export-menu-options"]/div[1]'
        self.click_on_xpath(xlsx_path)
        self.set_metrics()
        download_xpath = '//*[@id="pick-export-options"]'
        self.click_on_xpath(download_xpath)

    @staticmethod
    def get_file_as_df(temp_path=None):
        pd.DataFrame()
        file = os.listdir(temp_path)
        file_path = os.path.join(temp_path, file[0])
        sheet_names = ['Daily Spend', 'Daily Impressions', 'Top Sites']
        df = pd.concat(pd.read_excel(file_path, sheet_name=sheet_names,
                                     parse_dates=True), ignore_index=True)
        df.to_csv('tmp/output.csv', encoding='utf-8')
        temp_file = os.path.join(temp_path, 'output.csv')
        time.sleep(5)
        df = utl.import_read_csv(temp_file)
        shutil.rmtree(temp_path)
        return df

    def get_data(self, sd=None, ed=None, fields=None):
        self.browser = self.init_browser()
        sd, ed = self.get_data_default_check(sd, ed, fields)
        self.go_to_url(self.base_url)
        self.sign_in()
        self.create_report(sd, ed, self.title)
        self.export_to_csv()
        df = self.get_file_as_df(self.temp_path)
        self.quit()
        return df

    def quit(self):
        self.browser.quit()
