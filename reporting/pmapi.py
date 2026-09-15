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


OTP_REASON = ('Sensor Tower login requires an OTP; use an app password '
              'or the export lane')

SITE_DEFAULTS = {
    'base_url': 'https://pathmatics.sensortower.com',
    'login_url': 'https://app.sensortower.com/users/sign_in',
    'username_id': 'email',
    'password_id': 'password',
    'otp_name': 'user[otp_attempt]',
    'submit_xpath': '//*[@id="new_user"]//*[@type="submit"]',
    'signed_in_id': 'omnibox-text',
    'omnibox_id': 'omnibox-text',
    'omnibox_menu_xpath': '//*[@id="omnibox-text-menu"]/div/div/div[{}]',
    'entity_xpath': "//*[@class='entity-name']",
    'popup_close_xpath': '//*[@id="pendo-close-guide-8643dd7a"]',
    'calendar_popup_xpath': '//*[@id="pendo-close-guide-768f0d44"]',
    'dates_button_xpath': '//*[@id="dates-filter-button"]/a',
    'custom_date_xpath': '//*[@id="date-filter-menu"]/div/div[1]/div[10]',
    'date_picker_xpath': '//*[@id="date-filter-menu-date-picker-container"]',
    'date_done_xpath': '//*[@id="date-filter-menu-date-picker-button"]'
                       '/a/span',
    'export_button_xpath': '//*[@id="export-button"]',
    'export_xlsx_xpath': '//*[@id="export-menu-options"]/div[1]',
    'export_download_xpath': '//*[@id="pick-export-options"]',
    'sheet_daily': 'Daily Spend',
    'sheet_publishers': 'Top Publishers',
}


class OtpRequired(ex.WebDriverException):
    """Sign-in stopped at a one-time-passcode prompt."""


class PmApi(object):
    api_field_options = (
        ('Brand Tracker',
         'Brand tracker pull; any other value is an iSpot title'),)
    config_path = utl.config_path
    temp_path = 'tmp'
    sign_in_attempts = 300
    otp_poll_seconds = 2

    def __init__(self, headless=True):
        self.sw = None
        self.browser = None
        self.base_window = None
        self.headless = headless
        self.config_file = None
        self.username = None
        self.password = None
        self.config_list = None
        self.config = None
        self.pm_title = None
        self.publisher = None
        self.ispot_title = None
        self.brand_tracker = False
        self.site = dict(SITE_DEFAULTS)

    @property
    def base_url(self):
        return self.site['base_url']

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
        if isinstance(self.config.get('site'), dict):
            self.site.update(self.config['site'])
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
        if fields and fields != ['nan']:
            for field in fields:
                if field == 'Brand Tracker':
                    self.brand_tracker = True
                else:
                    self.ispot_title = fields.lower()
        return sd, ed

    def otp_prompted(self):
        """Whether the page is asking for a one-time passcode."""
        try:
            fields = self.browser.find_elements_by_name(
                self.site['otp_name'])
        except ex.WebDriverException:
            return False
        return any(f.is_displayed() for f in fields)

    def sign_in(self, otp_wait=0):
        """Fill the sign in form and submit it once.

        :param otp_wait: Seconds to wait for a human to type a
            one-time passcode into the (visible) browser when the site
            asks for one; 0 raises :class:`OtpRequired` instead
        :return: True once the signed in app is up
        """
        signed_in_id = self.site['signed_in_id']
        form_up = self.sw.wait_for_elem_load(
            self.site['username_id'], attempts=self.sign_in_attempts,
            sleep_time=.1, visible=True, raise_exception=False)
        if not form_up:
            if self.sw.wait_for_elem_load(
                    signed_in_id, attempts=20, sleep_time=.1,
                    raise_exception=False):
                logging.info('Already signed in, continuing.')
                return True
            raise ex.NoSuchElementException(
                'Neither the sign in form ({}) nor the signed in app '
                '({}) loaded.'.format(self.site['username_id'],
                                      signed_in_id))
        user_pass = [(self.username, self.site['username_id']),
                     (self.password, self.site['password_id'])]
        for value, elem_id in user_pass:
            elem = self.find_elem(self.sw.get_xpath_from_id(elem_id))
            elem.clear()
            elem.send_keys(value)
        self.sw.click_on_xpath(self.site['submit_xpath'], sleep=5)
        if self.sw.wait_for_elem_load(
                signed_in_id, attempts=self.sign_in_attempts,
                sleep_time=.1, raise_exception=False):
            return True
        if self.otp_prompted():
            deadline = time.time() + otp_wait
            while time.time() < deadline:
                if self.sw.wait_for_elem_load(
                        signed_in_id, attempts=1, sleep_time=.1,
                        raise_exception=False):
                    return True
                time.sleep(self.otp_poll_seconds)
            raise OtpRequired(OTP_REASON)
        raise ex.NoSuchElementException(
            'Sign in did not complete - check the credentials in '
            '{}.'.format(self.config_file))

    def find_elem(self, xpath):
        elem = self.browser.find_element_by_xpath(xpath)
        return elem

    def close_pop_up(self):
        pop_up_close_xpath = self.site['popup_close_xpath']
        try:
            self.sw.click_on_xpath(pop_up_close_xpath)
        except Exception as e:
            logging.info('No pop-ups found. Continuing')


    def search_title(self, title):
        self.browser.implicitly_wait(10)
        title_bar = self.browser.find_element_by_xpath(
            self.sw.get_xpath_from_id(self.site['omnibox_id']))
        title_bar.send_keys(title)
        time.sleep(10)
        title_result = self.site['omnibox_menu_xpath'].format(
            self.publisher)
        self.sw.click_on_xpath(title_result)
        title_result = self.browser.find_element_by_xpath(
            self.site['entity_xpath'])
        logging.info('Getting data for {}.'.format(title_result.text))
        return title_result.text

    def open_calendar(self):
        cal_button_xpath = self.site['dates_button_xpath']
        pop_up_xpath = self.site['calendar_popup_xpath']
        try:
            self.sw.click_on_xpath(pop_up_xpath)
        except ex.NoSuchElementException:
            pass
        self.sw.click_on_xpath(cal_button_xpath)
        custom_date_xpath = self.site['custom_date_xpath']
        self.sw.click_on_xpath(custom_date_xpath)

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
        base_path = self.site['date_picker_xpath']
        sd_path = base_path + '/div[1]/div[1]/input'
        ed_path = base_path + '/div[1]/div[3]/input'
        start_end = [[sd.date().__str__(), sd_path],
                     [ed.date().__str__(), ed_path]]
        for date_info in start_end:
            self.change_dates(date_info[0], date_info[1])
        done_path = self.site['date_done_xpath']
        self.sw.click_on_xpath(done_path)

    def create_report(self, sd, ed, title):
        resp_title = self.search_title(title)
        self.set_dates(sd, ed)
        return resp_title

    def export_to_csv(self):
        export_xpath = self.site['export_button_xpath']
        try:
            self.sw.click_on_xpath(export_xpath)
        except ex.NoSuchElementException:
            logging.error('No data for title. Aborting.')
            return False
        xlsx_path = self.site['export_xlsx_xpath']
        self.sw.click_on_xpath(xlsx_path)
        download_xpath = self.site['export_download_xpath']
        self.sw.click_on_xpath(download_xpath)
        return True

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
        sheet_names = {'Date': self.site['sheet_daily'],
                       'Target': self.site['sheet_publishers']}
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
        sd, ed = self.get_data_default_check(sd, ed, fields)
        self.sw = utl.SeleniumWrapper(headless=self.headless)
        try:
            self.browser = self.sw.browser
            self.base_window = self.browser.window_handles[0]
            self.sw.go_to_url(self.site['login_url'])
            self.sign_in()
            self.close_pop_up()
            df = pd.DataFrame()
            title_list = self.pm_title.split(',')
            for title in title_list:
                resp_title = self.create_report(sd, ed, title)
                export_success = self.export_to_csv()
                if not export_success:
                    continue
                if self.brand_tracker:
                    creative_df = pd.DataFrame()
                else:
                    creative_df = self.create_creatives_df()
                tdf = self.get_file_as_df(self.temp_path, creative_df, ed)
                tdf['Title'] = resp_title
                df = pd.concat([df, tdf], ignore_index=True)
        finally:
            self.sw.quit()
        return df
