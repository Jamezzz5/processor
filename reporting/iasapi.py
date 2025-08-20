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
        self.advertiser = None
        self.campaign = None
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
        self.config_list = [self.username, self.password]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in config file.'.format(item))

    def get_data(self, sd=None, ed=None, fields=None):
        """
        Main function to call and pull data from platform using selenium

        :param sd: The start date to pull data
        :param ed: The end date to pull data
        :param fields: Currently does not pass in any information
        :return: The pandas dataframe with pulled data
        """
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
        """
        Attempts to sign in to platform using username and password from config

        :param attempt: For logging attempt number
        :return: Boolean if sign in was successful
        """
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
        """
        Uses advertiser from config to change advertiser through dropdown

        :return: None
        """
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
        """
        Sets the report name input with todays date, start date and campaign

        :param sd: Start date to add to the report name
        :return: The report name as a string
        """
        xpath = ('//*[@id="root"]/div/div[2]/div/div[2]/div/div[1]'
                 '/div[1]/div[1]/div/input')
        self.sw.wait_for_elem_load(xpath, selector=self.sw.select_xpath)
        report = self.browser.find_element_by_xpath(xpath)
        today = dt.datetime.today().strftime('%Y%m%d')
        sd = sd.strftime('%Y%m%d')
        report_name = '{}_{}_{}'.format(today, sd, self.campaign)
        report.send_keys(report_name)
        return report_name

    def click_on_custom_date_range(self):
        """
        Changes the date range to custom from the dropdown

        :return:
        """
        range_xpath = ('//*[@id="root"]/div/div[2]/div/div[2]/div/div[1]/'
                       'div[1]/div[2]/div/div/div/div/div/div')
        self.sw.click_on_xpath(range_xpath)
        container_xp = (
            "//div[@role='listbox']//div[contains(@style,'overflow: auto')]")
        container = self.browser.find_element_by_xpath(container_xp)
        scroll = "arguments[0].scrollTop = arguments[0].scrollTop + 100;"
        self.browser.execute_script(scroll, container)
        cust_xpath = "//div[starts-with(normalize-space(.), 'Custom range')]"
        self.sw.wait_for_elem_load(cust_xpath, selector=self.sw.select_xpath)
        self.sw.click_on_xpath(cust_xpath)

    def set_date(self, date_val, is_start=True):
        """
        Clicks through date selector to the date value specified

        :param date_val: The date (as a date) to change to
        :param is_start: Boolean to click on the correct selector
        :return:
        """
        xpath_num = 1 if is_start else 3
        xpath = '//*[@id="root"]/div/div[2]/div/div[2]/div/div[1]/div[1]'
        xpath = '{}/div[2]/div/div/div/div[{}]/input'.format(xpath, xpath_num)
        self.sw.click_on_xpath(xpath)
        year_drop_xpath = ('/html/body/div[2]/div[2]/div/'
                           'div/div/div[1]/div[1]/button')
        self.sw.click_on_xpath(year_drop_xpath)
        year_str = str(date_val.year)
        year_xpath = (f'//div[contains(@class,"MuiYearPicker-root")]//'
                      f'button[normalize-space(text())="{year_str}"]')
        self.sw.click_on_xpath(year_xpath)
        month_short = date_val.strftime('%b')
        month_xpath = f'//button[contains(normalize-space(),"{month_short}")]'
        self.sw.click_on_xpath(month_xpath)
        day_no_pad = str(date_val.day)
        day_xpath = (
            f'//button[@role="gridcell" and '
            f'        not(contains(@class,"Outside")) and '
            f'        normalize-space(text())="{day_no_pad}" and '
            f'        contains(@name,"{month_short} {day_no_pad} {year_str}")]'
        )
        self.sw.click_on_xpath(day_xpath)

    def change_date_range(self, sd, ed):
        """
        Clicks through and sets dates in selectors to start and end date

        :param sd: The start date as a date to change to
        :param ed: The end date as a date to change to
        :return:
        """
        self.click_on_custom_date_range()
        self.set_date(sd, is_start=True)
        yesterday = dt.date.today() - dt.timedelta(days=1)
        if ed.date() > yesterday:
            ed = dt.datetime.combine(yesterday, dt.time.min)
        self.set_date(ed, is_start=False)

    def add_dimensions(self, rows=4, dimensions_in_row=10):
        """
        Loops through the dimensions panel and clicks matching self.dimensions

        :param rows: The number of rows to loop over
        :param dimensions_in_row: Max number of dimensions in a row
        :return:
        """
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

    def change_date_to_by_day(self):
        """
        Adjusts the report so that it pulls by day

        :return:
        """
        xpath = ("//label[@data-testid='radio-label' and "
                          "normalize-space(.)='By day']")
        self.sw.click_on_xpath(xpath)

    def filter_report_by_campaign(self):
        """
        Filters campaigns in report based on string in self.campaign

        :return:
        """
        xp = (
            "//label[@data-testid='radio-label' "
            "        and normalize-space()='Select specific'"
            "        and ancestor::div[.//div[@role='button' "
            "        and .//span[normalize-space()='Campaigns']]]]")
        self.sw.click_on_xpath(xp)
        input_xpath = (".//input[@data-testid='search-input' and "
                        "        @placeholder='Filter by name or ID']")
        elem = self.browser.find_element_by_xpath(input_xpath)
        elem.send_keys(self.campaign)
        self.sw.xpath_from_id_and_click('checkbox-input-check-all')
        filter_xpath = ("//button[.//span[starts-with(normalize-space(.), "
                        "         'Apply filter')]]")
        self.sw.click_on_xpath(filter_xpath)

    def add_metrics(self, metric_groups=2):
        """
        Loops through the metrics panel and clicks ones matching self.metrics

        :param metric_groups: The number of metric groups to loop over
        :return:
        """
        base_xpath = '//*[@id="tabpanel-0"]/div/div[5]/div[3]/div/div'
        for metric_group_num in range(metric_groups):
            check_box_xpath = '{}[{}]/div[1]/div/div/div/div'.format(
                base_xpath, metric_group_num + 1)
            elem = self.browser.find_element_by_xpath(check_box_xpath)
            elem.click()

    def click_csv(self):
        """
        Clicks button to change the report export type to csv

        :return:
        """
        csv_xpath = ('/html/body/div/div/div[2]/div/div[2]/div/div[1]'
                     '/div[6]/div[4]/span/div/input')
        elem = self.browser.find_element_by_xpath(csv_xpath)
        elem.click()

    def click_run_now(self):
        """
        Clicks the run now button

        :return:
        """
        elem_xpath = ('//*[@id="root"]/div/div[2]/div/div[2]'
                      '/div/div[2]/div/div/button/span[1]')
        elem = self.browser.find_element_by_xpath(elem_xpath)
        elem.click()

    def create_report(self, sd, ed):
        """
        Builds report based on sd, ed, self.advertiser and self.campaign

        :param sd: The start date for the data
        :param ed: The end date for the data
        :return: The name of the report
        """
        logging.info('Creating report.')
        base_xpath = '//*[@id="root"]/div/div[2]/div/div[2]/div/div[1]'
        report_btn = '{}/div/div[1]/div[1]/button'.format(base_xpath)
        self.sw.click_on_xpath(report_btn)
        report_name = self.give_report_name(sd)
        self.change_date_range(sd, ed)
        self.add_dimensions()
        self.change_date_to_by_day()
        if self.campaign:
            self.filter_report_by_campaign()
        self.add_metrics()
        self.click_csv()
        self.click_run_now()
        return report_name

    def find_report_row(self, report_name, base_path=''):
        """
        Goes through the report table and finds the correct one based on name

        :param report_name: The name of the report to look for
        :param base_path: The base xpath for the table
        :return: The row index of the found report
        """
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
        """
        Clicks the download report button based on the specified row

        :param report_row: The row number of the report to download
        :param base_path: Base xpath for the table
        :param attempt: Attempt number for logging
        :return: Boolean to indicate if the download was clicked
        """
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
        """
        Waits for the report to be able to download and clicks when ready

        :param report_name: The name of the report to export
        :return: Boolean if the report was successfully downloaded
        """
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
