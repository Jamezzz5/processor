import os
import sys
import json
import time
import shutil
import logging
import pandas as pd
import datetime as dt
import reporting.utils as utl
import selenium.common.exceptions as ex
from selenium.webdriver.common.keys import Keys


class PmApi(object):
    config_path = utl.config_path
    base_url = 'https://explorer.pathmatics.com'
    temp_path = 'tmp'

    def __init__(self):
        self.sw = None
        self.browser = None
        self.config_file = None
        self.username = None
        self.password = None
        self.config_list = None
        self.config = None
        self.pm_title = None
        self.publisher = None
        self.ispot_title = None

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
        self.pm_title = self.config['account_filter']
        if not self.config['campaign_filter'] == "":
            self.publisher = self.config['campaign_filter']
        else:
            self.publisher = 1

    def check_config(self):
        if self.config['account_filter'] == '':
            logging.warning('{} not in config file. '
                            ' Aborting.'.format(self.config['account_filter']))
            sys.exit(0)

    def get_data_default_check(self, sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields:
            if str(fields) != 'nan':
                self.ispot_title = fields[0].lower()
        return sd, ed

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

    def search_title(self):
        self.browser.implicitly_wait(10)
        title_bar = \
            self.browser.find_element_by_xpath('//*[@id=\"omnibox-text\"]')
        title_bar.send_keys(self.pm_title)
        time.sleep(10)
        title_result = '//*[@id="omnibox-text-menu"]/div/div/div[{}]'\
            .format(self.publisher)
        self.click_on_xpath(title_result)
        title_result = self.browser.find_element_by_xpath(
            "//*[@class='entity-name']")
        logging.info('Getting data for {}.'.format(title_result.text))

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
        logging.info('Getting data from {} to {}.'.format(sd, ed))
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

    def create_report(self, sd, ed):
        self.search_title()
        self.set_dates(sd, ed)

    def export_to_csv(self):
        export_xpath = '//*[@id=\"export-button\"]'
        try:
            self.click_on_xpath(export_xpath)
        except ex.NoSuchElementException:
            logging.error('No data for title. Aborting.')
            sys.exit(0)
        xlsx_path = '//*[@id="export-menu-options"]/div[1]'
        self.click_on_xpath(xlsx_path)
        download_xpath = '//*[@id="pick-export-options"]'
        self.click_on_xpath(download_xpath)

    @staticmethod
    def get_url(html):
        url_loc = [html.find("video src=\""), html.find("img src=\"")]
        if url_loc == [-1, -1]:
            return None, None
        url_loc = min(i for i in url_loc if i > -1)
        html = html[url_loc:]
        html = html.split('\"', 2)
        # Underscores UTF-8 Encoded to Avoid Splitting
        url = html[1].replace('_', '%5F')
        html = html[2]
        return url, html

    def get_size_spend(self, element):
        element.click()
        self.browser.implicitly_wait(10)
        dialogue_box = self.browser.find_element_by_xpath(
            "//*[@class=\"creative-dialog-metrics\"]")
        html = dialogue_box.get_attribute('innerHTML')
        size_loc = html.find('Dimensions &amp; Type</h2>')
        if size_loc == -1:
            size = ""
        else:
            size = html[(size_loc + len('Dimensions &amp; Type<h/2>')):]
            size = size.split(',')[0]
            size = size.split('<span>')[1]
        spend_loc = html.find('Spend: ')
        spend = html[(spend_loc + len('Spend: ')):]
        spend = spend.split('</div>')[0]
        close = self.browser.find_element_by_xpath(
            '//*[@class=\"icon-32 large-x-grey dialog-close-x\"]')
        close.click()
        return size, spend

    def get_pm_creatives(self, urls, sizes, spends):
        element = self.browser.find_element_by_xpath(
            '//*[@id="top-creatives-grid"]')
        html = element.get_attribute('innerHTML')
        try:
            creative_elements = self.browser.find_elements_by_xpath(
                "//*[@class=\"creative-snapshot-cover\"]")
        except ex.NoSuchElementException:
            logging.warning('No creatives found.')
            return urls, sizes, spends
        for creative_element in creative_elements:
            url, html = self.get_url(html)
            if url:
                urls.append(url)
            else:
                break
            size, spend = self.get_size_spend(creative_element)
            sizes.append(size)
            spends.append(spend)

    def search_and_view_ispot(self):
        elem = self.browser.find_element_by_xpath('//*[@id="search-term"]')
        elem.send_keys(self.ispot_title)
        elem.send_keys(Keys.RETURN)

        try:
            elem = self.browser.find_element_by_xpath('//*[@class="view-all"]')
            elem.click()
        except (ex.NoSuchElementException, ex.
                ElementClickInterceptedException):
            pass

    def get_ispot_creatives(self, urls, sizes, spends):
        ispot_url = 'https://www.ispot.tv/search/'
        self.browser.get(ispot_url)
        try:
            elem = self.browser.find_element_by_xpath(
                '//*[@id="cookie-consent-close-btn"]')
            elem.click()
        except ex.NoSuchElementException:
            pass
        self.search_and_view_ispot()
        while True:
            ads = self.browser.find_elements_by_xpath(
                '//li[@class="ad-thumbnails__item"]/a')
            hrefs = []
            for ad in ads:
                if self.ispot_title in ad.text.lower():
                    hrefs.append(ad.get_attribute("href"))

            for href in hrefs:
                self.browser.get(href)
                elem = self.browser.find_element_by_xpath(
                    '//video[@id="video-main"]/source')
                url = elem.get_attribute("src")
                self.browser.execute_script("window.history.go(-1)")
                urls.append(url)
                sizes.append("")
                spends.append("0.000001")
            try:
                elem = self.browser.find_element_by_xpath('//*[@class="next"]')
                elem.click()
            except ex.NoSuchElementException:
                break

    def get_creatives(self):
        urls = []
        sizes = []
        spends = []
        self.get_pm_creatives(urls, sizes, spends)
        if self.ispot_title:
            self.get_ispot_creatives(urls, sizes, spends)
        return urls, sizes, spends

    def create_creatives_df(self):
        logging.info('Getting creative data')
        creatives = pd.DataFrame()
        creatives['Creative'] = ""
        creatives['Size'] = ""
        creatives['Creative Spend'] = ""
        urls, sizes, spends = self.get_creatives()
        for url, size, spend in zip(urls, sizes, spends):
            new_row = [url, size, spend]
            creatives.loc[0 if pd.isnull(creatives.index.max()) else
                          creatives.index.max() + 1] = new_row
        return creatives

    @staticmethod
    def clean_date_df(df):
        metric_names = ['Desktop Display ($)',
                        'Mobile Display ($)', 'Mobile Video ($)',
                        'Desktop Video ($)', 'Facebook ($)']
        df = df.melt(
            id_vars=[x for x in df.columns if x not in metric_names],
            value_vars=[x for x in metric_names if x in df.columns],
            var_name='Environment-variable',
            value_name='Environment-value')
        return df

    def get_file_as_df(self, temp_path=None, creative_df=None, ed=None):
        pd.DataFrame()
        file_path = None
        for x in range(1, 101):
            try:
                file = os.listdir(temp_path)
                file_path = os.path.join(temp_path, file[0])
            except (IndexError, FileNotFoundError):
                logging.warning('Data not downloaded. Waiting. Attempt {}.'
                                .format(x))
                time.sleep(10)
            else:
                logging.info('Data downloaded.')
                break
        sheet_names = {'Date': 'Daily Spend', 'Target': 'Top Publishers'}
        date_df = pd.read_excel(file_path, sheet_name=sheet_names['Date'],
                                parse_dates=True)
        target_df = pd.read_excel(file_path, sheet_name=sheet_names['Target'],
                                  parse_dates=True)
        date_df = self.clean_date_df(date_df)
        df = pd.concat([date_df, target_df, creative_df], ignore_index=True)
        df['Date'].fillna(value=ed, inplace=True)
        df.to_csv('tmp/output.csv', encoding='utf-8')
        temp_file = os.path.join(temp_path, 'output.csv')
        time.sleep(5)
        df = utl.import_read_csv(temp_file)
        shutil.rmtree(temp_path)
        return df

    def get_data(self, sd=None, ed=None, fields=None):
        self.sw = utl.SeleniumBrowser()
        self.browser = self.sw.browser
        sd, ed = self.get_data_default_check(sd, ed, fields)
        self.sw.go_to_url(self.base_url)
        self.sign_in()
        self.create_report(sd, ed)
        self.export_to_csv()
        creative_df = self.create_creatives_df()
        df = self.get_file_as_df(self.temp_path, creative_df, ed)
        self.quit()
        return df

    def quit(self):
        self.browser.quit()
