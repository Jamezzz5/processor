import os
import io
import json
import logging
import pandas as pd
import datetime as dt
from urllib.parse import urlparse
import reporting.utils as utl
import reporting.awss3 as awss3


class SsApi(object):
    url = 'url'
    site = 'site'
    file_name = 'file_name'
    img_url = 'img_url'
    ads = 'ads'
    ad_count = 'ad_count'
    ad_domains = 'ad_domains'
    shot_key = 'shot_key'
    capture_status = 'capture_status'
    capture_detail = 'capture_detail'
    manifest_only = (capture_status, capture_detail)
    date = 'date'
    hour = 'hour'
    device = 'device'
    device_mobile = 'Mobile'
    device_desktop = 'Desktop'
    ss_file_path = 'screenshots'
    output_file = 'sites.xlsx'
    output_csv = 'sites.csv'
    manifest_name = 'manifest.json'
    run_format = '%y%m%d_%H'
    host_fallbacks = {'reddit.com': 'old.reddit.com'}
    fallback_kinds = ('blocked', 'bot_check')
    scan_kinds = ('ok', 'consent_wall')

    def __init__(self, file_name='site_config.csv', ss_file_path_date=None,
                 sites=None, s3=None, prefix=None):
        """Site list from the config csv, or from ``sites`` when a
        caller drives the sweep in-process.

        :param file_name: config csv naming the sites to shoot
        :param ss_file_path_date: run folder leaf (``%y%m%d_%H``)
        :param sites: row dicts to shoot instead of reading the csv
        :param s3: a configured awss3.S3, when the caller owns the config
        :param prefix: bucket folder for this run in place of
            ``screenshots`` -- a plan check keeps its shots apart from
            the nightly sweep's
        """
        logging.info('Getting config from {}.'.format(file_name))
        self.file_name = os.path.join(utl.config_path, file_name)
        self.ss_file_path = prefix or self.ss_file_path
        self.sites = {}
        self.slots = {}
        self.run_id = None
        self.config = (self.rows_to_config(sites) if sites is not None
                       else self.import_config())
        self.ss_file_path_date = self.add_file_path(ss_file_path_date)
        self.add_device_to_config()
        self.set_all_sites()
        self.s3 = s3

    def input_config(self, api_file):
        self.s3 = awss3.S3()
        self.s3.input_config(api_file)

    def import_config(self):
        """Site list from the config csv, or empty when it is absent.

        The class is constructed for every processor that carries an
        ss source, before any vendor-key filtering, so raising here
        would take down the whole import loop on a box that has not
        been given a site list yet.
        """
        if not os.path.isfile(self.file_name):
            logging.warning(
                'No site list at {}, skipping screenshots.'.format(
                    self.file_name))
            return {}
        try:
            df = pd.read_csv(self.file_name)
        except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
            logging.warning('Could not read site list {}: {}'.format(
                self.file_name, e))
            return {}
        return df.to_dict(orient='index')

    @staticmethod
    def rows_to_config(rows):
        """Site rows handed in by a caller, shaped like the csv read."""
        return {i: dict(row) for i, row in enumerate(rows)}

    def set_site(self, index):
        site_dict = self.config[index]
        site = Site(site_dict, ss_file_path_date=self.ss_file_path_date)
        return site

    def get_site(self, index):
        return self.sites[index]

    def set_all_sites(self):
        """One Site per row, kept beside the row rather than in it: the
        row's ``site`` column is the name the view groups captures on,
        so it has to stay a string."""
        for index in self.config:
            site = self.set_site(index)
            self.sites[index] = site
            self.config[index][self.site] = site.name

    def add_device_to_config(self):
        if not self.config:
            return
        total_indices = max(self.config) + 1
        for index in range(total_indices):
            new_index = index + total_indices
            self.config[index][self.device] = self.device_desktop
            self.config[new_index] = self.config[index].copy()
            self.config[new_index][self.device] = self.device_mobile

    @classmethod
    def fallback_url(cls, url):
        """``url`` on the stand-in ``host_fallbacks`` names for its host
        or a parent of it, else ''."""
        host = utl.SeleniumWrapper.url_host(url)
        for domain, alt in cls.host_fallbacks.items():
            if utl.SeleniumWrapper.host_under(host, domain):
                return urlparse(url)._replace(netloc=alt).geturl()
        return ''

    @classmethod
    def fallback_shot(cls, browser, url, verdict, file_name=None,
                      **shot_kwargs):
        """Re-shoot a blocked page at its stand-in host, returning the
        new verdict with that host named: reddit's new front end refuses
        the browser where old.reddit.com still serves the page.
        """
        alt = cls.fallback_url(url)
        if verdict[0] not in cls.fallback_kinds or not alt or alt == url:
            return verdict
        kind, detail = browser.take_screenshot(alt, file_name,
                                               scroll_first=True,
                                               **shot_kwargs)
        via = f'via {utl.SeleniumWrapper.url_host(alt)}'
        return kind, f'{detail} {via}' if detail else via

    @classmethod
    def screenshot_site(cls, browser, site, attempts=2, scan_ads=False):
        """Screenshot one site, rebuilding the browser between tries.

        A command that timed out leaves the driver session unusable, so
        retrying on the same browser only times out again; ad slots
        are read only off a page that was actually shown.

        :param browser: SeleniumWrapper to shoot with
        :param site: Site providing the url and output file name
        :param attempts: tries before the site is given up on
        :param scan_ads: also read the page's ad slots, without clicks
        :return: (ad slots found, (kind, detail) of the shot); the slots
            are [] when not asked, not shown or the site failed
        """
        for attempt in range(attempts):
            try:
                verdict = browser.take_screenshot(site.url, site.file_name,
                                                  scroll_first=True)
                verdict = cls.fallback_shot(browser, site.url, verdict,
                                            site.file_name)
                if scan_ads and verdict[0] in cls.scan_kinds:
                    return browser.scan_ad_slots(
                        shot_prefix=site.file_name[:-4]), verdict
                return [], verdict
            except browser.browser_errors as e:
                logging.warning(
                    'Failed to screenshot {} on attempt {}. {}'.format(
                        site.url, attempt + 1, e))
                if attempt < attempts - 1:
                    browser.restart_browser()
        logging.error('Could not screenshot {}.'.format(site.url))
        return [], ('error_page', 'page unreachable')

    def get_data(self, sd, ed, fields):
        if not self.config:
            logging.warning('No sites to screenshot.')
            return pd.DataFrame()
        browser = utl.SeleniumWrapper(page_load_strategy='eager')
        try:
            for index in self.config:
                site = self.get_site(index)
                if site.device == self.device_mobile and not browser.mobile:
                    browser.quit()
                    browser = utl.SeleniumWrapper(
                        mobile=True, page_load_strategy='eager')
                slots, verdict = self.screenshot_site(browser, site,
                                                      scan_ads=True)
                self.slots[index] = slots
                self.config[index][self.file_name] = site.file_name
                self.config[index][self.ad_count] = len(slots)
                self.config[index][self.ad_domains] = ';'.join(sorted(
                    {x['landing_domain'] for x in slots
                     if x['landing_domain']}))
                self.config[index][self.capture_status] = verdict[0]
                self.config[index][self.capture_detail] = verdict[1]
        finally:
            browser.quit()
        self.write_config_to_df()
        df = self.upload_screenshots()
        return df

    def upload_screenshots(self):
        """Push the page shots, their ad-slot shots and the run's
        manifest to the bucket, then write the run's frame."""
        for index in self.config:
            site = self.get_site(index)
            image_data = utl.image_to_binary(site.file_name, True)
            if image_data:
                key = site.file_name.replace('\\', '/')
                url = self.s3.s3_upload_file_obj(image_data, key)
                self.config[index][self.img_url] = url
                self.config[index][self.shot_key] = key
            self.upload_slot_shots(index)
        self.upload_manifest()
        df = self.write_config_to_df()
        return df

    def upload_slot_shots(self, index):
        """Upload one row's ad-slot shots beside its page shot."""
        for slot in self.slots.get(index, []):
            path = slot.pop('shot_path', '')
            data = utl.image_to_binary(path, True) if path else None
            slot['shot_key'] = path.replace('\\', '/') if data else ''
            slot['shot_url'] = (
                self.s3.s3_upload_file_obj(data, slot['shot_key'])
                if data else '')

    @staticmethod
    def clean_value(value):
        """A csv cell as json will carry it: NaN reads as ''."""
        if isinstance(value, float) and pd.isna(value):
            return ''
        return value

    def manifest_rows(self):
        """Every capture of this run as a plain dict -- the row's
        columns (a ``partner`` column rides along), the shot and its ad
        slots -- for the manifest and for callers driving the class
        in-process."""
        stamp = dt.datetime.strptime(self.run_id, self.run_format)
        rows = []
        for index, row in self.config.items():
            out = {k: self.clean_value(v) for k, v in row.items()}
            out.update({self.url: self.get_site(index).url,
                        'run': self.run_id, 'captured_at': stamp.isoformat(),
                        self.ads: self.slots.get(index, [])})
            rows.append(out)
        return rows

    def upload_manifest(self):
        """Write this run's manifest beside its shots, locally and in
        the bucket, so a reader that cannot see this box still gets
        every capture with its ad slots."""
        body = json.dumps(self.manifest_rows(), default=str)
        local = os.path.join(self.ss_file_path_date, self.manifest_name)
        with open(local, 'w', encoding='utf-8') as f:
            f.write(body)
        key = '/'.join([self.ss_file_path.replace('\\', '/'),
                        self.run_id, self.manifest_name])
        self.s3.s3_upload_file_obj(io.BytesIO(body.encode('utf-8')), key)

    def add_file_path(self, ss_file_path_date=None):
        if not ss_file_path_date:
            ss_file_path_date = dt.datetime.today().strftime(self.run_format)
        self.run_id = ss_file_path_date
        file_path = os.path.join(self.ss_file_path, ss_file_path_date)
        utl.dir_check(file_path)
        return file_path

    def write_config_to_df(self):
        """The run's frame for the sheet and the db load, without the
        verdict columns that ride the manifest only."""
        df = pd.DataFrame.from_dict(self.config, orient='index')
        df = df.drop(columns=[c for c in self.manifest_only
                              if c in df.columns])
        date = dt.datetime.strptime(self.run_id, self.run_format)
        df[self.date] = date
        df[self.hour] = date.hour
        output_file = os.path.join(self.ss_file_path_date, self.output_file)
        df.to_excel(output_file, index=False)
        df.to_excel(self.output_file, index=False)
        df.to_csv(self.output_csv, index=False)
        return df


class Site(object):
    prot = 'http'
    prots = 'https://'
    www = 'www'
    tlds = ['.com', '.net', '.de', '.org', '.gov', '.edu', '.tv']
    base_file_path = 'screenshots'

    def __init__(self, site_dict=None, url=None, file_name=None,
                 ss_file_path_date=None, device=None):
        self.url = url
        self.file_name = file_name
        self.device = device
        self.ss_file_path_date = ss_file_path_date
        self.site_dict = site_dict
        if site_dict:
            for k in site_dict:
                setattr(self, k, site_dict[k])
        self.url, self.file_name = self.check_site_params(
            self.url, self.file_name, self.device)
        self.name = self.site_name()

    def site_name(self):
        """The csv's own ``site`` when it names one, else the host --
        the string the view groups captures on."""
        given = getattr(self, 'site', None)
        if isinstance(given, str) and given.strip():
            return given.strip()
        return utl.SeleniumWrapper.url_host(self.url)

    def check_url(self, url=None):
        if (url[:4] != self.prot and
                (self.www not in url and url.count('.') == 1)):
            url = '{}{}.{}'.format(self.prots, self.www, url)
        elif url[:4] != self.prot:
            url = '{}{}'.format(self.prots, url)
        return url

    def check_file_name(self, url=None, file_name=None, device=None):
        if url and not file_name:
            file_name = url
            for x in [self.prots, self.www] + self.tlds:
                file_name = file_name.replace(x, '')
            file_name = file_name.replace('.', '').replace('/', ' ')
            if device:
                file_name = '{}_{}'.format(file_name, device)
            file_name += '.png'
        file_name = os.path.join(self.ss_file_path_date, file_name)
        return file_name

    def check_site_params(self, url=None, file_name=None, device=None):
        file_name = self.check_file_name(url, file_name, device)
        url = self.check_url(url)
        return url, file_name
