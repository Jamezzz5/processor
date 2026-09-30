import os
import io
import json
import time
import logging
import dataclasses
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
    ad_viewport_density = 'ad_viewport_density'
    page_kind = 'page_kind'
    parent_url = 'parent_url'
    pages_walked = 'pages_walked'
    full_shot_key = 'full_shot_key'
    full_shot_url = 'full_shot_url'
    page_height = 'page_height'
    section = 'section'
    headlines = 'headlines'
    article = 'article'
    feeds = 'feeds'
    feed = 'feed'
    kind_home = 'home'
    kind_article = 'article'
    vitals_fields = ('lab_lcp_ms', 'lab_cls', 'lab_ttfb_ms',
                     'page_kilobytes', 'third_party_hosts', 'ad_frames',
                     ad_viewport_density)
    walk_fields = (page_kind, parent_url, pages_walked, full_shot_key,
                   full_shot_url, page_height, section)
    editorial_fields = (headlines, article, feeds)
    manifest_only = ((capture_status, capture_detail) + vitals_fields
                     + walk_fields + editorial_fields + (feed,))
    articles_per_site = 1
    headlines_per_site = 40
    site_budget_s = 150
    walk_budget_s = 2.5 * 3600
    in_process_args = ('--disable-site-isolation-trials',
                       '--disable-features=IsolateOrigins,site-per-process')
    capture = utl.CaptureOptions(
        name='sweep', page_load_timeout=25, shoot_partial=True,
        paint_wait=15, challenge_wait=30, headed=True, native_identity=True,
        fresh_cookies=True,
        extra_args=in_process_args + ('--disable-dev-shm-usage',
                                      '--screen-info={1920x1080}'),
        drop_args=('--window-position',))
    retry_waits = {'page_load_timeout': 45, 'challenge_wait': 45,
                   'paint_wait': 20}
    retry_kinds = ('error_page', 'blank', 'bot_check')
    retry_budget_s = 30 * 60
    second_pass_tag = 'second pass'
    toggle_kinds = ('ssl_error',)
    full_shot_suffix = '_full.jpg'
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
                 sites=None, s3=None, prefix=None, articles_per_site=None,
                 capture=None, second_pass=None, headlines_per_site=None):
        """Site list from the config csv, or from ``sites`` when a
        caller drives the sweep in-process.

        :param file_name: config csv naming the sites to shoot
        :param ss_file_path_date: run folder leaf (``%y%m%d_%H``)
        :param sites: row dicts to shoot instead of reading the csv
        :param s3: a configured awss3.S3, when the caller owns the config
        :param prefix: bucket folder for this run in place of
            ``screenshots`` -- a plan check keeps its shots apart from
            the nightly sweep's
        :param articles_per_site: article pages walked off each home
            page; None means the class default for the csv sweep and
            none at all for handed-in rows, which name exact pages
        :param capture: utl.CaptureOptions every browser is built with
        :param second_pass: re-shoot the home pages that did not show;
            None means only for the csv sweep
        :param headlines_per_site: headlines kept off each home page,
            defaulted like ``articles_per_site``
        """
        logging.info('Getting config from {}.'.format(file_name))
        self.file_name = os.path.join(utl.config_path, file_name)
        self.ss_file_path = prefix or self.ss_file_path
        self.sites = {}
        self.slots = {}
        self.full_shots = {}
        self.run_id = None
        self.from_csv = sites is None
        self.capture = capture or self.capture
        self.second_pass = (self.from_csv if second_pass is None
                            else second_pass)
        if articles_per_site is None:
            articles_per_site = 0 if sites is not None else \
                self.articles_per_site
        self.articles_per_site = articles_per_site
        if headlines_per_site is None:
            headlines_per_site = (self.headlines_per_site if self.from_csv
                                  else 0)
        self.headlines_per_site = headlines_per_site
        self.config = (self.rows_to_config(sites) if sites is not None
                       else self.import_config())
        self.ss_file_path_date = self.add_file_path(ss_file_path_date)
        self.add_device_to_config()
        self.add_walk_defaults()
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

    def add_walk_defaults(self):
        """Give every row the walk fields as a home page, so a manifest
        reader never has to know which sweep wrote it."""
        for row in self.config.values():
            row[self.page_kind] = self.kind_home
            row[self.parent_url] = ''
            row[self.pages_walked] = 0
            row[self.full_shot_key] = ''
            row[self.full_shot_url] = ''
            row[self.page_height] = ''
            row[self.section] = self.clean_value(row.get(self.section, ''))
            row[self.feed] = self.clean_value(row.get(self.feed, ''))
            row.update(self.empty_editorial())

    @classmethod
    def empty_editorial(cls):
        """Fresh editorial fields for a page nothing was read off."""
        return {cls.headlines: [], cls.article: {}, cls.feeds: []}

    @classmethod
    def full_shot_name(cls, file_name):
        """The jpeg path of a page shot's full-page companion."""
        return file_name[:-4] + cls.full_shot_suffix

    @staticmethod
    def toggle_www(url):
        """``url`` with ``www.`` taken off or put on its host."""
        parts = urlparse(url)
        host = parts.netloc
        if not host:
            return ''
        twin = host[4:] if host.lower().startswith('www.') else f'www.{host}'
        return parts._replace(netloc=twin).geturl()

    @classmethod
    def fallback_url(cls, url, kind=''):
        """``url`` on the stand-in ``host_fallbacks`` names for its host
        or a parent of it, or on its ``www.`` twin when ``kind`` is one
        of ``toggle_kinds``; else ''."""
        if kind in cls.toggle_kinds:
            return cls.toggle_www(url)
        host = utl.SeleniumWrapper.url_host(url)
        for domain, alt in cls.host_fallbacks.items():
            if utl.SeleniumWrapper.host_under(host, domain):
                return urlparse(url)._replace(netloc=alt).geturl()
        return ''

    @classmethod
    def fallback_shot(cls, browser, url, verdict, file_name=None,
                      **shot_kwargs):
        """Re-shoot a refused page at its stand-in host or ``www.``
        twin, returning the new verdict with that host named."""
        alt = cls.fallback_url(url, verdict[0])
        kinds = cls.fallback_kinds + cls.toggle_kinds
        if verdict[0] not in kinds or not alt or alt == url:
            return verdict
        kind, detail = browser.take_screenshot(alt, file_name,
                                               scroll_first=True,
                                               **shot_kwargs)
        via = f'via {urlparse(alt).netloc.lower()}'
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
        full = cls.full_shot_name(site.file_name)
        error = None
        for attempt in range(attempts):
            try:
                verdict = browser.take_screenshot(site.url, site.file_name,
                                                  scroll_first=True,
                                                  full_file_name=full)
                verdict = cls.fallback_shot(browser, site.url, verdict,
                                            site.file_name,
                                            full_file_name=full)
                if scan_ads and verdict[0] in cls.scan_kinds:
                    return browser.scan_ad_slots(
                        shot_prefix=site.file_name[:-4]), verdict
                return [], verdict
            except browser.browser_errors as e:
                error = e
                logging.warning(
                    'Failed to screenshot {} on attempt {}. {}'.format(
                        site.url, attempt + 1, e))
                if attempt < attempts - 1:
                    browser.restart_browser()
        logging.error('Could not screenshot {}.'.format(site.url))
        return [], ('error_page',
                    f'browser stopped answering: {type(error).__name__}')

    @staticmethod
    def viewport_density(slots, viewport):
        """Return the first viewport's ad share, counting overlaps once.
        Return None when the viewport is unavailable."""
        width, height = viewport.get('w') or 0, viewport.get('h') or 0
        if not width or not height:
            return None
        rectangles = []
        edges = set()
        for slot in slots:
            top, left = slot.get('top'), slot.get('left')
            if top is None or left is None:
                continue
            x1, x2 = max(left, 0), min(left + slot['width'], width)
            y1, y2 = max(top, 0), min(top + slot['height'], height)
            if x2 > x1 and y2 > y1:
                rectangles.append((x1, x2, y1, y2))
                edges.update((x1, x2))
        edges = sorted(edges)
        area = 0
        for left, right in zip(edges, edges[1:]):
            spans = sorted((y1, y2) for x1, x2, y1, y2 in rectangles
                           if x1 < right and x2 > left)
            covered = end = 0
            for start, stop in spans:
                covered += max(0, stop - max(start, end))
                end = max(end, stop)
            area += (right - left) * covered
        return round(min(area / float(width * height), 1.0), 4)

    @classmethod
    def page_vitals(cls, browser, slots, verdict):
        """The lab reading and ad viewport density of a page that was
        shown, over ``vitals_fields``; {} for one that was not."""
        if verdict[0] not in cls.scan_kinds:
            return {}
        read = getattr(browser, 'page_vitals', None)
        vitals = dict(read() or {}) if read else {}
        viewport = vitals.pop('viewport', None) or {}
        out = {k: vitals[k] for k in cls.vitals_fields if k in vitals}
        density = cls.viewport_density(slots, viewport)
        if density is not None:
            out[cls.ad_viewport_density] = density
        return out

    def page_editorial(self, browser, site, verdict, kind):
        """A shown home page's headlines and feeds, or an article's own
        meta; {} for a page that was not shown."""
        if verdict[0] not in self.scan_kinds:
            return {}
        read = getattr(browser, 'read_page_meta', None)
        meta = dict(read() or {}) if read else {}
        feeds = meta.pop(self.feeds, [])
        if kind == self.kind_article:
            return {self.article: meta}
        read = getattr(browser, 'read_headlines', None)
        found = (read(site.url, self.headlines_per_site)
                 if read and self.headlines_per_site else [])
        return {self.headlines: found, self.feeds: feeds}

    def manifest_path(self):
        """Where this run's manifest sits on this box."""
        return os.path.join(self.ss_file_path_date, self.manifest_name)

    @classmethod
    def page_capture(cls):
        """The sweep's capture options without its profile folder, for
        a caller shooting pages of its own."""
        return dataclasses.replace(cls.capture, profile_dir='')

    def retry_capture(self):
        """The run's capture options with each wait it has on raised to
        ``retry_waits``, for the second pass."""
        waits = {k: max(v, getattr(self.capture, k))
                 for k, v in self.retry_waits.items()
                 if getattr(self.capture, k)}
        return dataclasses.replace(self.capture, **waits)

    def device_capture(self, mobile, retry=False):
        """One device's capture options: the csv sweep's profile folder
        splits per device, and handed-in rows use none."""
        options = self.retry_capture() if retry else self.capture
        if not options.profile_dir:
            return options
        leaf = 'mobile' if mobile else 'desktop'
        folder = (os.path.join(options.profile_dir, leaf)
                  if self.from_csv else '')
        return dataclasses.replace(options, profile_dir=folder)

    def new_browser(self, mobile, retry=False):
        """A browser for one device under ``device_capture``."""
        return utl.SeleniumWrapper(
            mobile=mobile, page_load_strategy='eager',
            capture=self.device_capture(mobile, retry))

    def browser_for(self, browser, site, retry=False):
        """``browser``, or a mobile one in its place when ``site`` is
        the first mobile row."""
        if site.device != self.device_mobile or browser.mobile:
            return browser
        browser.quit()
        return self.new_browser(True, retry)

    def get_data(self, sd, ed, fields):
        """Shoot every listed page, walking articles off each shown
        home page while the budgets hold; a run whose manifest already
        exists is skipped."""
        if not self.config:
            logging.warning('No sites to screenshot.')
            return pd.DataFrame()
        if os.path.isfile(self.manifest_path()):
            logging.warning(f'Run {self.run_id} already has a manifest '
                            f'at {self.manifest_path()}, skipping.')
            return pd.DataFrame()
        walk_deadline = time.time() + self.walk_budget_s
        browser = self.new_browser(False)
        try:
            browser = self.shoot_rows(browser, list(self.config),
                                      walk_deadline)
        finally:
            browser.quit()
        self.run_second_pass(walk_deadline)
        self.write_config_to_df()
        df = self.upload_screenshots()
        return df

    def shoot_rows(self, browser, indices, walk_deadline, retry=False):
        """Shoot the rows at ``indices`` in order and return the browser
        left for the caller to quit; ``walk_deadline`` stops walks."""
        first = browser
        try:
            for index in indices:
                if retry and time.time() > walk_deadline:
                    logging.warning('Second pass budget spent.')
                    break
                site = self.get_site(index)
                browser = self.browser_for(browser, site, retry)
                if retry:
                    self.retry_row(browser, index, site)
                else:
                    self.shoot_row(browser, index, site, walk_deadline)
        except BaseException:
            if browser is not first:
                browser.quit()
            raise
        return browser

    def shoot_row(self, browser, index, site, walk_deadline):
        """Shoot one page onto its row, then walk its articles while
        the site's and the run's budgets hold."""
        site_deadline = time.time() + self.site_budget_s
        slots, verdict = self.screenshot_site(browser, site, scan_ads=True)
        self.record_capture(browser, index, site, slots, verdict)
        if (self.articles_per_site and verdict[0] in self.scan_kinds
                and time.time() < min(site_deadline, walk_deadline)):
            self.walk_articles(browser, index, site, site_deadline)

    def retry_indices(self):
        """The home rows in ``retry_kinds``, desktop first."""
        rows = [i for i, row in self.config.items()
                if row.get(self.page_kind) == self.kind_home
                and row.get(self.capture_status) in self.retry_kinds]
        return sorted(rows, key=lambda i: self.config[i].get(
            self.device) == self.device_mobile)

    def run_second_pass(self, walk_deadline):
        """Re-shoot the home pages that did not show on fresh, more
        patient browsers until ``retry_budget_s`` or ``walk_deadline``."""
        indices = self.retry_indices() if self.second_pass else []
        deadline = min(time.time() + self.retry_budget_s, walk_deadline)
        if not indices or time.time() > deadline:
            return
        mobile = self.get_site(indices[0]).device == self.device_mobile
        browser = self.new_browser(mobile, retry=True)
        try:
            browser = self.shoot_rows(browser, indices, deadline,
                                      retry=True)
        finally:
            browser.quit()

    @staticmethod
    def read_shot(path):
        """A shot's bytes, or None when there is no such file."""
        try:
            with open(path, 'rb') as f:
                return f.read()
        except OSError:
            return None

    @staticmethod
    def restore_shot(path, data):
        """Put back the first shot a retry wrote over, or remove it."""
        if data is not None:
            with open(path, 'wb') as f:
                f.write(data)
        elif os.path.isfile(path):
            os.remove(path)

    def retry_row(self, browser, index, site):
        """Shoot a page again, once: a retry that shows replaces the
        row, tagged; one that does not leaves the first verdict and shot.
        """
        first = self.read_shot(site.file_name)
        slots, (kind, detail) = self.screenshot_site(
            browser, site, attempts=1, scan_ads=True)
        if kind not in self.scan_kinds:
            self.restore_shot(site.file_name, first)
            return
        tag = self.second_pass_tag
        self.record_capture(browser, index, site, slots,
                            (kind, f'{detail} {tag}' if detail else tag))

    def record_capture(self, browser, index, site, slots, verdict):
        """Land one page's shot, slots, verdict, lab reading, editorial
        fields and full-page companion on its row."""
        row = self.config[index]
        self.slots[index] = slots
        row[self.file_name] = site.file_name
        row[self.ad_count] = len(slots)
        row[self.ad_domains] = ';'.join(sorted(
            {x['landing_domain'] for x in slots if x['landing_domain']}))
        row[self.capture_status] = verdict[0]
        row[self.capture_detail] = verdict[1]
        row.update(self.page_vitals(browser, slots, verdict))
        row.update(self.page_editorial(browser, site, verdict,
                                       row.get(self.page_kind)))
        path, height = getattr(browser, 'last_full_shot', None) or ('', None)
        self.full_shots[index] = path
        row[self.page_height] = height if height is not None else ''

    def walk_articles(self, browser, index, site, deadline):
        """Shoot the articles linked off one shown home page, the home
        row's own headlines first, each as its own row, until
        ``deadline``."""
        found = self.config[index].get(self.headlines) or []
        links = [x['href'] for x in found[:self.articles_per_site]]
        read = getattr(browser, 'find_article_links', None)
        if not links and read:
            links = read(site.url, self.articles_per_site)
        for n, url in enumerate(links, 1):
            if time.time() > deadline:
                logging.warning(f'Site budget spent on {site.url}; '
                                'article walk stopped.')
                break
            new_index = max(self.config) + 1
            row, article = self.article_row(index, url, n)
            self.config[new_index] = row
            self.sites[new_index] = article
            slots, verdict = self.screenshot_site(browser, article,
                                                  attempts=1, scan_ads=True)
            self.record_capture(browser, new_index, article, slots, verdict)
            self.config[index][self.pages_walked] += 1

    def article_row(self, index, url, n):
        """``(row, Site)`` for the ``n``th article off the home row at
        ``index``, keeping the home row's own columns and site name."""
        home = self.config[index]
        skip = {self.file_name, self.ad_count, self.ad_domains,
                self.img_url, self.shot_key, self.pages_walked,
                *self.vitals_fields, self.capture_status,
                self.capture_detail, self.full_shot_key,
                self.full_shot_url, self.page_height,
                *self.editorial_fields}
        row = {k: v for k, v in home.items() if k not in skip}
        row.update({self.url: url, self.page_kind: self.kind_article,
                    self.parent_url: self.get_site(index).url,
                    self.pages_walked: 0, self.full_shot_key: '',
                    self.full_shot_url: '', self.page_height: ''})
        row.update(self.empty_editorial())
        stem = os.path.basename(self.get_site(index).file_name)[:-4]
        device = str(home.get(self.device) or '')
        stem = stem.removesuffix(f'_{device}') if device else stem
        name = f'{stem}_a{n}_{device}.png'
        article = Site(row, file_name=name,
                       ss_file_path_date=self.ss_file_path_date)
        row[self.site] = article.name
        return row, article

    def upload_screenshots(self):
        """Push the page, full-page and ad-slot shots and the run's
        manifest to the bucket, then write the run's frame."""
        for index in self.config:
            site = self.get_site(index)
            image_data = utl.image_to_binary(site.file_name, True)
            if image_data:
                key = site.file_name.replace('\\', '/')
                url = self.s3.s3_upload_file_obj(image_data, key)
                self.config[index][self.img_url] = url
                self.config[index][self.shot_key] = key
            self.upload_full_shot(index)
            self.upload_slot_shots(index)
        self.upload_manifest()
        df = self.write_config_to_df()
        return df

    def upload_full_shot(self, index):
        """Upload one row's full-page jpeg beside its page shot."""
        path = self.full_shots.get(index, '')
        data = utl.image_to_binary(path, True) if path else None
        if not data:
            return
        key = path.replace('\\', '/')
        self.config[index][self.full_shot_url] = self.s3.s3_upload_file_obj(
            data, key)
        self.config[index][self.full_shot_key] = key

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
        with open(self.manifest_path(), 'w', encoding='utf-8') as f:
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
