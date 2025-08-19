import os
import time
import json
import logging
import pandas as pd
import datetime as dt
import reporting.utils as utl
import selenium.common.exceptions as ex


class IasApi(object):
    config_path = utl.config_path
    username_str = 'username'
    password_str = 'password'
    advertiser_str = 'advertiser'
    campaign_str = 'campaign'
    default_config_file_name = 'iasapi.json'
    temp_path = 'tmp'
    base_url = 'https://reporting.integralplatform.com/spa'
    login_url = '{}/login'.format(base_url)
    report_url = '{}/shell/ias-signal-custom-reports/reports/'.format(base_url)

    dimensions = ['Teams', 'Campaigns', 'Media partners', 'Placements', 'Date']


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

    def input_config(self, config):
        logging.info('Loading IAS config file: {}.'.format(config))
        self.config_file = os.path.join(self.config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)
        self.username = self.config.get(self.username_str)
        self.password = self.config.get(self.password_str)
        self.advertiser = self.config.get(self.advertiser_str)
        self.campaign = self.config.get(self.campaign_str)
        self.config_list = [ self.username, self.password]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in config file.'.format(item))

    def get_data(self, sd=None, ed=None, fields=None):
        df = pd.DataFrame()
        self.sw = utl.SeleniumWrapper(headless=self.headless)
        self.browser = self.sw.browser
        self.sw.go_to_url(self.login_url)
        self.sign_in()
        self.change_advertiser()
        report_name = self.create_report(sd, ed)
        report_downloaded = self.export_to_csv(report_name)
        if report_downloaded:
            df = self.sw.get_file_as_df(self.temp_path)
        self.browser.quit()
        return df

    def sign_in(self, attempt=0):
        logging.info('Signing in.: Attempt {}'.format(attempt))
        form_xpath = '//*[@id="root"]/div/div/div/div[1]/form'
        user_xpath = '{}/div[1]/div[1]/input'.format(form_xpath)
        next_btn = '{}/div[2]/button'.format(form_xpath)
        pass_xpath = '{}/div[2]/div/div/input'.format(form_xpath)
        login_btn = '//*[@id="root"]/div/div/div/div[1]/form/div[3]/button[2]'
        user_pass = [(self.username, user_xpath, next_btn),
                     (self.password, pass_xpath, login_btn)]
        for input_val, input_elem, btn_elem in user_pass:
            elem = self.browser.find_element_by_xpath(input_elem)
            elem.send_keys(input_val)
            self.sw.random_delay(0.3, 1)
            self.sw.click_on_xpath(btn_elem)
        return True

    def change_advertiser(self):
        logging.info('Changing to advertiser {}.'.format(self.advertiser))
        sel_xpath = '//*[@id="root"]/div/div[2]/div/div[1]/div[2]/div/div/div'
        self.sw.wait_for_elem_load(sel_xpath, selector=self.sw.select_xpath)
        self.sw.click_on_xpath(sel_xpath)
        search_xpath = '//*[@id="TeamSwitcher"]'
        elem = self.browser.find_element_by_xpath(search_xpath)
        elem.send_keys(self.advertiser)
        ad_xpath = '/html/body/div[2]/div[2]/div/div/div[2]/div/div'
        self.sw.click_on_xpath(ad_xpath)

    def give_report_name(self, sd):
        xpath = ('//*[@id="root"]/div/div[2]/div/div[2]/div/div[1]'
                 '/div[1]/div[1]/div/input')
        self.sw.wait_for_elem_load(xpath, selector=self.sw.select_xpath)
        report = self.browser.find_element_by_xpath(xpath)
        today = dt.datetime.today().strftime('%Y%m%d')
        sd = sd.strftime('%Y%m%d')
        report_name = '{}_{}_{}'.format(today, sd, self.campaign)
        report.send_keys(report_name)
        return report_name

    def add_dimensions(self, rows=4, dimensions_in_row=10):
        base_xpath = '//*[@id="tabpanel-0"]/div/div[2]/'
        for row_num in range(rows):
            row_xpath = '{}div[{}]/div/div'.format(base_xpath, row_num)
            for dim in range(dimensions_in_row):
                dim_xpath = '{}[{}]/div/div/span'.format(row_xpath, dim + 1)
                try:
                    elem = self.browser.find_element_by_xpath(dim_xpath)
                except ex.NoSuchElementException:
                    continue
                elem_val = elem.get_attribute('innerHTML')
                if elem_val in self.dimensions:
                    elem.click()

    def add_metrics(self, metric_groups=2):
        base_xpath = '//*[@id="tabpanel-0"]/div/div[5]/div[3]/div/div'
        for metric_group_num in range(metric_groups):
            check_box_xpath = '{}[{}]/div[1]/div/div/div/div'.format(
                base_xpath, metric_group_num + 1)
            elem = self.browser.find_element_by_xpath(check_box_xpath)
            elem.click()

    def click_csv(self):
        csv_xpath = ('/html/body/div/div/div[2]/div/div[2]/div/div[1]'
                     '/div[6]/div[4]/span/div/input')
        elem = self.browser.find_element_by_xpath(csv_xpath)
        elem.click()

    def click_run_now(self):
        elem_xpath = ('//*[@id="root"]/div/div[2]/div/div[2]'
                      '/div/div[2]/div/div/button/span[1]')
        elem = self.browser.find_element_by_xpath(elem_xpath)
        elem.click()

    def create_report(self, sd, ed):
        logging.info('Creating report.')
        base_xpath = '//*[@id="root"]/div/div[2]/div/div[2]/div/div[1]'
        report_btn = '{}/div/div[1]/div[1]/button'.format(base_xpath)
        self.sw.click_on_xpath(report_btn)
        report_name = self.give_report_name(sd)
        self.add_dimensions()
        self.add_metrics()
        self.click_csv()
        self.click_run_now()
        return report_name

    def find_report_row(self, report_name, base_path=''):
        post_xpath = '/div[2]/div/span/div/span[1]/span/a'
        report_row = 0
        for report_row in range(1, 20):
            a_xpath = '{}div[2]/div/div/div[{}]{}'.format(
                base_path, report_row, post_xpath)
            if report_row == 1:
                self.sw.wait_for_elem_load(
                    a_xpath, selector=self.sw.select_xpath)
            elem = self.browser.find_element_by_xpath(a_xpath)
            row_report_name = elem.get_attribute('innerHTML')
            if row_report_name == report_name:
                break
        return report_row

    def click_report_download(self, report_row, base_path='', attempt=1):
        report_clicked = False
        xpath = '{}div[2]/div/div/div[{}]'.format(base_path, report_row)
        xpath = '{}/div[9]/div/span/div/span/a'.format(xpath)
        try:
            self.sw.click_on_xpath(xpath)
            report_clicked = True
            logging.warning('Report not clicked, attempt {}'.format(attempt))
        except ex.NoSuchElementException:
            time.sleep(5)
        return report_clicked

    def export_to_csv(self, report_name):
        logging.info('Downloading created report.')
        utl.dir_check(self.temp_path)
        base_path = '//*[@id="root"]/div/div[2]/div/div[2]/div/div[2]/div/div/'
        report_row = self.find_report_row(report_name, base_path)
        report_downloaded = False
        for x in range(40):
            report_clicked = self.click_report_download(report_row, base_path)
            if report_clicked:
                report_downloaded = True
                break
        return report_downloaded
