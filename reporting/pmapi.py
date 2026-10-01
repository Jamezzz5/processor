import os
import re
import sys
import json
import time
import logging
import pandas as pd
import datetime as dt
from urllib.parse import urlencode
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
    'signed_in_id': 'NavSearch',
    'omnibox_id': 'NavSearch',
    'popup_close_xpath': '//*[@id="pendo-close-guide-8643dd7a"]',
    'csrf_selector': 'meta[name=csrf-token]',
    'search_path': '/api/ads/search',
    'series_path': '/api/ads/brand/{}/timeseries/channels',
    'country': 'US',
}

CHANNELS = {
    'desktop_display': 'Desktop Display',
    'desktop_video': 'Desktop Video',
    'facebook': 'Facebook',
    'mobile_display': 'Mobile Display',
    'mobile_video': 'Mobile Video',
    'ott': 'OTT',
    'reddit': 'Reddit',
    'tiktok': 'TikTok',
    'youtube': 'YouTube',
}
ENTITY_TYPES = ('brand', 'advertiser')
CURRENCY_UNITS = {'cent': 100.0, 'dollar': 1.0}
DATE_COL = 'Date'
CHANNEL_COL = 'Environment-variable'
SPEND_COL = 'Environment-value'
IMPRESSIONS_COL = 'Impressions'
SPEND_SUFFIX = ' ($)'
FRAME_COLS = [DATE_COL, CHANNEL_COL, SPEND_COL, IMPRESSIONS_COL]

REQUEST_SCRIPT = """
const done = arguments[arguments.length - 1];
const [verb, url, body, selector] = arguments;
const meta = document.querySelector(selector);
fetch(url, {method: verb, credentials: 'same-origin',
            headers: {'Content-type': 'application/json',
                      'X-CSRF-Token': meta ? meta.content : ''},
            body: body ? JSON.stringify(body) : undefined})
  .then(r => r.text().then(t => done({status: r.status, text: t})))
  .catch(e => done({status: 0, text: String(e)}));
"""


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
    request_timeout = 60

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
        self.brand = None
        self.window = None
        self.frame = None
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

    def type_into(self, elem_id, value):
        """Replace one field's contents with ``value``."""
        elem = self.find_elem(self.sw.get_xpath_from_id(elem_id))
        elem.clear()
        elem.send_keys(value)

    def reveal_password(self):
        """Press Next past the email when the password is still hidden;
        a form showing both fields is left alone."""
        password_id = self.site['password_id']
        if self.sw.wait_for_elem_load(
                password_id, attempts=1, sleep_time=.1, visible=True,
                raise_exception=False):
            return
        self.sw.click_on_xpath(self.site['submit_xpath'], sleep=1)
        if not self.sw.wait_for_elem_load(
                password_id, attempts=self.sign_in_attempts,
                sleep_time=.1, visible=True, raise_exception=False):
            raise ex.NoSuchElementException(
                'The password field ({}) never appeared after the '
                'email step.'.format(password_id))

    def sign_in(self, otp_wait=0):
        """Fill the sign in form, one step or two, and submit it.

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
        self.type_into(self.site['username_id'], self.username)
        self.reveal_password()
        self.type_into(self.site['password_id'], self.password)
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


    def request_json(self, verb, path, body=None):
        """The parsed JSON of one app request, sent from inside the
        signed in page so it rides that session and CSRF token; raises
        WebDriverException on anything but HTTP 200 JSON."""
        self.browser.set_script_timeout(self.request_timeout)
        answer = self.browser.execute_async_script(
            REQUEST_SCRIPT, verb, path, body, self.site['csrf_selector'])
        endpoint = path.split('?')[0]
        if (status := answer.get('status')) != 200:
            raise ex.WebDriverException(
                f'{endpoint} answered HTTP {status}.')
        try:
            return json.loads(answer.get('text') or '')
        except ValueError:
            raise ex.WebDriverException(
                f'{endpoint} did not answer with JSON.')

    @staticmethod
    def name_key(name):
        """A brand name reduced to what has to match: letters and
        digits, case folded."""
        return re.sub(r'[^0-9a-z]', '', str(name or '').casefold())

    def pick_brand(self, title, brands):
        """The listed brand named ``title`` (search is fuzzy), a brand
        before an advertiser, then the bigger spender; None if none."""
        key = self.name_key(title)
        named = [b for b in brands
                 if key and self.name_key(b.get('name')) == key]
        if not named:
            return None
        return min(named, key=lambda b: (
            ENTITY_TYPES.index(b.get('type'))
            if b.get('type') in ENTITY_TYPES else len(ENTITY_TYPES),
            -((b.get('spend') or {}).get('value') or 0)))

    def search_title(self, title):
        """Remember and return the brand named ``title``.

        :raises LookupError: when no listed brand has that name
        """
        query = [('entity_types[]', kind) for kind in ENTITY_TYPES]
        query.append(('search_terms', title))
        found = self.request_json(
            'GET', '{}?{}'.format(self.site['search_path'],
                                  urlencode(query)))
        brand = self.pick_brand(title, found.get('brands') or [])
        if brand is None:
            raise LookupError(
                'Sensor Tower lists no brand named {}.'.format(title))
        self.brand = brand
        logging.info('Getting data for {}.'.format(brand['name']))
        return brand['name']

    def create_report(self, sd, ed, title):
        """Resolve ``title`` to its brand and remember the window the
        next :func:`export_to_csv` pulls."""
        self.frame = None
        resp_title = self.search_title(title)
        self.window = (sd, ed)
        return resp_title

    @staticmethod
    def series_rows(answer):
        """The by-channel answer as one row per channel per day with
        spend, in dollars. Days nothing was spent on are left out."""
        unit = (answer.get('currency') or {}).get('unit')
        if unit not in CURRENCY_UNITS:
            raise ValueError(
                'Spend came back in an unknown unit ({}).'.format(unit))
        rows = []
        for channel in answer.get('channels') or []:
            label = CHANNELS.get(channel.get('channel'),
                                 channel.get('channel'))
            for point in channel.get('timeseries') or []:
                spend = point.get('spend') or 0
                if not label or spend <= 0:
                    continue
                rows.append({
                    DATE_COL: str(point.get('date'))[:10],
                    CHANNEL_COL: '{}{}'.format(label, SPEND_SUFFIX),
                    SPEND_COL: round(spend / CURRENCY_UNITS[unit], 2),
                    IMPRESSIONS_COL: point.get('impressions')})
        return rows

    def export_to_csv(self):
        """Pull the remembered brand's daily spend by channel; False when
        it spent nothing in the window."""
        sd, ed = self.window
        query = urlencode([('category_id', 0), ('date_granularity', 'daily'),
                           ('start_date', sd.date().isoformat()),
                           ('end_date', ed.date().isoformat()),
                           ('sort_by', 'spend')])
        path = self.site['series_path'].format(self.brand['id'])
        body = [{'country_code': self.site['country'],
                 'channels': list(CHANNELS)}]
        rows = self.series_rows(
            self.request_json('POST', f'{path}?{query}', body))
        self.frame = pd.DataFrame(rows, columns=FRAME_COLS)
        if not rows:
            logging.warning('No spend for {} in the window.'.format(
                self.brand['name']))
        return bool(rows)

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

    def get_file_as_df(self, temp_path=None, creative_df=None, ed=None):
        """The last pull as a frame, creatives appended; ``temp_path``
        is unused and kept for callers."""
        frames = [df for df in (self.frame, creative_df)
                  if df is not None and not df.empty]
        if not frames:
            return pd.DataFrame(columns=FRAME_COLS)
        df = pd.concat(frames, ignore_index=True)
        df[DATE_COL] = df[DATE_COL].fillna(value=ed)
        return df

    def read_creatives(self):
        """The creatives frame for a pull that asked for one; a failed
        read costs the creatives, never the spend."""
        if self.brand_tracker:
            return pd.DataFrame()
        try:
            return self.create_creatives_df()
        except ex.WebDriverException as e:
            logging.warning('Creatives not read: {}'.format(e))
            return pd.DataFrame()

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
                try:
                    resp_title = self.create_report(sd, ed, title.strip())
                except LookupError as e:
                    logging.warning('{} Skipping it.'.format(e))
                    continue
                export_success = self.export_to_csv()
                if not export_success:
                    continue
                creative_df = self.read_creatives()
                tdf = self.get_file_as_df(self.temp_path, creative_df, ed)
                tdf['Title'] = resp_title
                df = pd.concat([df, tdf], ignore_index=True)
        finally:
            self.sw.quit()
        return df
