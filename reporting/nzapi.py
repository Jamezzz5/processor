import os
import re
import sys
import json
import time
import logging
import tempfile
import requests
import numpy as np
import pandas as pd
import datetime as dt
import reporting.utils as utl


class NzApi(object):
    config_path = utl.config_path
    base_url = 'https://api.newzoo.com/v1.0/bulk_exports/'
    mau_dataset = 'engagement/pc_ps_xbox/mau'
    rollup = 'Total'
    worldwide_code = 'ZZ'
    download_timeout = 600
    required_cols = ('title', 'date', 'country_code', 'device',
                     'platform')
    title_aliases = ('game', 'game_title')
    name_month = re.compile(r'(20\d{2})[-_]?(0[1-9]|1[0-2])')
    country_names = {
        'AE': 'United Arab Emirates', 'AR': 'Argentina',
        'AT': 'Austria', 'AU': 'Australia', 'BE': 'Belgium',
        'BR': 'Brazil', 'CA': 'Canada', 'CH': 'Switzerland',
        'CL': 'Chile', 'CO': 'Colombia', 'CZ': 'Czech Republic',
        'DE': 'Germany', 'DK': 'Denmark', 'ES': 'Spain',
        'FI': 'Finland', 'FR': 'France', 'GB': 'United Kingdom',
        'HU': 'Hungary', 'ID': 'Indonesia', 'IE': 'Ireland',
        'IL': 'Israel', 'IT': 'Italy', 'JP': 'Japan',
        'KR': 'South Korea', 'MX': 'Mexico', 'NL': 'Netherlands',
        'NO': 'Norway', 'NZ': 'New Zealand', 'PL': 'Poland',
        'PT': 'Portugal', 'RU': 'Russia', 'SA': 'Saudi Arabia',
        'SE': 'Sweden', 'TH': 'Thailand', 'TR': 'Turkey',
        'US': 'United States', 'ZA': 'South Africa',
        'ZZ': 'Worldwide',
    }

    def __init__(self):
        self.config = None
        self.config_file = None
        self.game_title = None
        self.api_key = None
        self.country_filter = None
        self.config_list = None
        self.df = pd.DataFrame()
        self.r = None

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Nz config file: {}'.format(config))
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
        self.game_title = self.config['game_title']
        self.api_key = self.config['api_key']
        if 'country_filter' in self.config:
            self.country_filter = self.config['country_filter']
        self.config_list = [self.config, self.api_key, self.game_title]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Nz config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    @staticmethod
    def get_data_default_check(sd, ed):
        if ed is None:
            ed = dt.datetime.today()
        if sd is None:
            sd = ed - dt.timedelta(days=31)
        return sd, ed

    @staticmethod
    def slugify(title):
        """Newzoo's slug scheme: lowercase, every non-alphanumeric run
        -> '-', no edge-stripping (byte parity with their pipeline)."""
        return re.sub(r'[^a-z0-9]+', '-', str(title).lower())

    @staticmethod
    def month_span(sd, ed):
        """Every 'YYYY-MM' between sd and ed inclusive."""
        months = set()
        cursor = dt.date(sd.year, sd.month, 1)
        end = dt.date(ed.year, ed.month, 1)
        while cursor <= end:
            months.add(cursor.strftime('%Y-%m'))
            cursor = (cursor + dt.timedelta(days=32)).replace(day=1)
        return months

    def market_codes(self):
        """The config's country_filter as ISO codes ([] = worldwide
        rollup only). Accepts ISO codes or the REST-era display names,
        so configs written against the old endpoint keep working."""
        if not self.country_filter:
            return []
        by_name = {v.lower(): k for k, v in self.country_names.items()}
        codes = []
        for entry in str(self.country_filter).split(','):
            entry = entry.strip()
            if not entry:
                continue
            code = by_name.get(entry.lower())
            if code is None and len(entry) == 2:
                code = entry.upper()
            if code is None:
                logging.warning(
                    'Unknown Newzoo market {} - skipped.'.format(entry))
            else:
                codes.append(code)
        return codes

    def create_header(self):
        header = {
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.api_key),
        }
        return header

    def make_request(self, method, url, params=None, body=None, header=None):
        try:
            response = self.raw_request(method, url, params=params,
                                        body=body, header=header)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            response = self.make_request(method, url, params=params,
                                         body=body, header=header)
        return response

    @staticmethod
    def raw_request(method, url, params=None, body=None, header=None):
        if method == 'post':
            response = requests.post(url, json=body, headers=header)
        elif method == 'get':
            response = requests.get(url, params=params, headers=header)
        else:
            response = None
        time.sleep(1)
        return response

    def fetch_manifest(self, header):
        """The MAU dataset's manifest -> export file names, or None
        with the failure logged (auth, transport, unexpected shape)."""
        url = '{}{}/manifest'.format(self.base_url, self.mau_dataset)
        self.r = self.make_request('get', url, header=header)
        if self.r is None:
            return None
        if self.r.status_code in (401, 403):
            logging.error('Newzoo auth failed ({}) - check api_key in '
                          'the Nz config.'.format(self.r.status_code))
            return None
        try:
            data = self.r.json()
        except ValueError:
            logging.error('Newzoo manifest returned a non-JSON body.')
            return None
        if self.r.status_code != 200 or not isinstance(data, dict) \
                or 'files' not in data:
            logging.error('Newzoo manifest error {}: {}'.format(
                self.r.status_code, str(data)[:300]))
            return None
        names = []
        for entry in data['files']:
            if isinstance(entry, str):
                names.append(entry)
            elif isinstance(entry, dict):
                name = (entry.get('name') or entry.get('file_name')
                        or entry.get('filename'))
                if name:
                    names.append(str(name))
        if not names:
            logging.warning('Newzoo manifest lists no export files.')
        return names

    def select_files(self, names, months):
        """Manifest names worth downloading for ``months`` — names
        carrying a parseable month are kept only in-window; a naming
        scheme with no parseable months keeps every file and the
        row-level date filter narrows instead."""
        kept, matched = [], False
        for name in names:
            found = self.name_month.search(name)
            if found is None:
                continue
            matched = True
            if '{}-{}'.format(*found.groups()) in months:
                kept.append(name)
        return kept if matched else list(names)

    def download_file(self, name, dest, header):
        """Stream one export to ``dest``; False (logged) on failure —
        a missed file skips, kept rows still land."""
        url = '{}{}/download'.format(self.base_url, self.mau_dataset)
        try:
            with requests.get(url, headers=header,
                              params={'file_name': name}, stream=True,
                              timeout=self.download_timeout) as r:
                if r.status_code != 200:
                    logging.warning('Newzoo download {} returned {}'
                                    ''.format(name, r.status_code))
                    return False
                with open(dest, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
        except requests.exceptions.RequestException as e:
            logging.warning('Newzoo download {} failed: {}'
                            ''.format(name, e))
            return False
        return True

    def read_export(self, path, months, codes):
        """One downloaded export -> its in-window rollup rows as a
        DataFrame, or None (logged). All columns come back — the
        vendormatrix maps the subset it knows."""
        try:
            import pyarrow.parquet as pq
        except ImportError:
            logging.warning('pyarrow is not installed - Newzoo bulk '
                            'exports cannot be read.')
            return None
        have = set(pq.read_schema(path).names)
        missing = set(self.required_cols) - have
        if missing:
            logging.warning('Newzoo export {} lacks {} - skipped.'
                            ''.format(os.path.basename(path),
                                      sorted(missing)))
            return None
        filters = [('device', '=', self.rollup),
                   ('platform', '=', self.rollup)]
        if codes:
            filters.append(('country_code', 'in', codes))
        else:
            filters.append(('country_code', '=', self.worldwide_code))
        df = pq.read_table(path, filters=filters).to_pandas()
        month = df['date'].astype(str).str[:7]
        return df[month.isin(months)]

    def shape_df(self, df):
        """Concatenated rollup rows -> the pull's frame: the config's
        titles only (slug-matched), list cells flattened to sorted
        tuples, REST-era title aliases and market names attached."""
        wanted = {self.slugify(t) for t in self.game_title.split(',')
                  if t.strip()}
        df = df[df['title'].astype(str).map(self.slugify)
                .isin(wanted)].copy()
        for col in df.columns:
            if df[col].map(lambda v:
                           isinstance(v, (list, np.ndarray))).any():
                df[col] = df[col].map(
                    lambda v: tuple(sorted(v, key=str))
                    if isinstance(v, (list, np.ndarray)) else v)
        for alias in self.title_aliases:
            df[alias] = df['title']
        df['market'] = df['country_code'].map(self.country_names)
        df['market'] = df['market'].fillna(df['country_code'])
        df['date'] = pd.to_datetime(df['date'])
        return df.reset_index(drop=True)

    def get_data(self, sd=None, ed=None, fields=None):
        sd, ed = self.get_data_default_check(sd, ed)
        if fields and 'Viewership' in fields:
            logging.warning('Newzoo viewership was retired with the '
                            'REST API and has no bulk dataset - '
                            'skipping that field.')
        months = self.month_span(sd, ed)
        header = self.create_header()
        codes = self.market_codes()
        self.df = pd.DataFrame()
        names = self.fetch_manifest(header)
        if not names:
            return self.df
        frames = []
        with tempfile.TemporaryDirectory() as tmp:
            for name in self.select_files(names, months):
                dest = os.path.join(tmp, os.path.basename(name)
                                    or 'export.parquet')
                if not self.download_file(name, dest, header):
                    continue
                df = self.read_export(dest, months, codes)
                if df is not None and not df.empty:
                    frames.append(df)
                try:
                    os.remove(dest)
                except OSError:
                    pass
        if frames:
            self.df = self.shape_df(pd.concat(frames,
                                              ignore_index=True))
        logging.info('Newzoo bulk pull complete: {} row(s).'.format(
            len(self.df)))
        return self.df
