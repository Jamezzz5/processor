import os
import io
import re
import gzip
import json
import time
import signal
import shutil
import random
import base64
import zipfile
import logging
import requests
import pandas as pd
import numpy as np
import datetime as dt
import selenium.webdriver as wd
import reporting.vmcolumns as vmc
import reporting.dictcolumns as dctc
import reporting.expcolumns as exc
from subprocess import check_output
import http.client as http_client
import selenium.common.exceptions as ex
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



config_path = 'config/'
raw_path = 'raw_data/'
error_path = 'ERROR_REPORTS/'
dict_path = 'dictionaries/'
backup_path = 'backup/'
preview_path = './ad_previews/'
preview_config = 'preview_config.csv'
db_df_trans_config = 'db_df_translation.csv'

RULE_PREF = 'RULE'
RULE_METRIC = 'METRIC'
RULE_QUERY = 'QUERY'
RULE_FACTOR = 'FACTOR'
RULE_CONST = [RULE_METRIC, RULE_QUERY, RULE_FACTOR]
PRE = 'PRE'
POST = 'POST'

na_values = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN',
             'null', '-nan', '1.#IND', '1.#QNAN', 'N/A', 'NULL', 'NaN', 'n/a',
             'nan']
sheet_name_splitter = ':::'
tmp_file_suffix = 'TMP'


def dir_check(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def import_read_csv(filename, path=None, file_check=True, error_bad='error',
                    empty_df=False, nrows=None, file_type=None):
    sheet_names = []
    if file_check and sheet_name_splitter in filename:
        filename = filename.split(sheet_name_splitter)
        sheet_names = filename[1:]
        filename = filename[0]
    if path:
        filename = os.path.join(path, filename)
    if file_check:
        if not os.path.isfile(filename):
            logging.warning('{} not found.  Continuing.'.format(filename))
            return pd.DataFrame()
    if not file_type:
        file_type = os.path.splitext(filename)[1].lower()
    kwargs = {'parse_dates': True, 'keep_default_na': False,
              'na_values': na_values, 'nrows': nrows}
    if sheet_names:
        kwargs['sheet_name'] = sheet_names
    if file_type == '.xlsx':
        read_func = pd.read_excel
    else:
        read_func = pd.read_csv
        kwargs['encoding'] = 'utf-8'
        kwargs['on_bad_lines'] = error_bad
        kwargs['low_memory'] = False
    try:
        df = read_func(filename, **kwargs)
    except UnicodeDecodeError:
        if 'encoding' in kwargs:
            kwargs['encoding'] = 'iso-8859-1'
        df = read_func(filename, **kwargs)
    except pd.errors.EmptyDataError as e:
        msg = 'Data {} empty.  Continuing. {}'.format(filename, e)
        logging.warning(msg)
        if empty_df:
            df = pd.DataFrame()
        else:
            df = None
            return df
    except ValueError as e:
        logging.warning(e)
        read_func = pd.read_csv
        df = read_func(filename, **kwargs)
    if sheet_names:
        df = pd.concat(df, ignore_index=True, sort=True)
    df = df.rename(columns=lambda x: x.strip())
    return df


def write_file(df, file_name):
    """Writes a df to disk as csv or xlsx parsing file type from name

    Keyword arguments:
    df -- the dataframe to be written
    file_name -- the name of the file to write to on disk
    """
    logging.debug('Writing {}'.format(file_name))
    file_type = os.path.splitext(file_name)[1].lower()
    kwargs = {}
    if file_type == '.xlsx':
        write_func = df.to_excel
    else:
        write_func = df.to_csv
        kwargs['encoding'] = 'utf-8'
    try:
        write_func(file_name, index=False, **kwargs)
        return True
    except IOError:
        logging.warning('{} could not be opened.  This file was not saved.'
                        ''.format(file_name))
        return False


def exceldate_to_datetime(excel_date):
    epoch = dt.datetime(1899, 12, 30)
    delta = dt.timedelta(hours=round(excel_date * 24))
    return epoch + delta


def string_to_date(my_string):
    month_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec']
    if ('/' in my_string and my_string[-4:][:2] != '20' and
            ':' not in my_string and len(my_string) in [6, 7, 8]):
        try:
            return dt.datetime.strptime(my_string, '%m/%d/%y')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif ('/' in my_string and my_string[-4:][:2] == '20' and
          ':' not in my_string):
        if my_string[0] == '/':
            new_month = '{:02d}'.format(dt.datetime.today().month)
            my_string = '{}{}'.format(new_month, my_string)
        if '//' in my_string:
            my_string = my_string.replace('//', '/01/')
        try:
            return dt.datetime.strptime(my_string, '%m/%d/%Y')
        except ValueError:
            logging.info(f"Retrying date as day/month/year for {my_string}")
            try:
                return dt.datetime.strptime(my_string, '%d/%m/%Y')
            except ValueError:
                logging.warning('Could not parse date: {}'.format(my_string))
                return pd.NaT
    elif (((len(my_string) == 5) and (my_string[0] == '4')) or
          ((len(my_string) == 7) and (my_string.count('.') == 1))):
        return exceldate_to_datetime(float(my_string))
    elif len(my_string) == 8 and my_string.isdigit() and my_string[0] == '2':
        try:
            return dt.datetime.strptime(my_string, '%Y%m%d')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif len(my_string) in [7, 8] and '.' in my_string:
        return dt.datetime.strptime(my_string, '%m.%d.%y')
    elif my_string == '0' or my_string == '0.0':
        return pd.NaT
    elif ((len(my_string) == 22) and (':' in my_string) and
          ('+' in my_string)):
        my_string = my_string[:-6]
        return dt.datetime.strptime(my_string, '%Y-%m-%d %M:%S')
    elif ((':' in my_string) and ('/' in my_string) and my_string[1] == '/' and
          my_string[4] == '/'):
        my_string = my_string[:9]
        return dt.datetime.strptime(my_string, '%m/%d/%Y')
    elif (('PST' in my_string) and (len(my_string) == 28) and
          (':' in my_string)):
        my_string = my_string.replace('PST ', '')
        return dt.datetime.strptime(my_string, '%a %b %d %M:%S:%H %Y')
    elif (('-' in my_string) and (my_string[:2] == '20') and
          len(my_string) == 10):
        try:
            return dt.datetime.strptime(my_string, '%Y-%m-%d')
        except ValueError:
            try:
                return dt.datetime.strptime(my_string, '%Y-%d-%m')
            except ValueError:
                logging.warning('Could not parse date: {}'.format(my_string))
                return pd.NaT
    elif ((len(my_string) == 19) and (my_string[:2] == '20') and
          ('-' in my_string) and (':' in my_string)):
        try:
            return dt.datetime.strptime(my_string, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif ((len(my_string) == 7 or len(my_string) == 8) and
          my_string[-4:-2] == '20'):
        return dt.datetime.strptime(my_string, '%m%d%Y')
    elif ((len(my_string) == 6 or len(my_string) == 5) and
          my_string[-3:] in month_list):
        my_string = my_string + '-' + dt.datetime.today().strftime('%Y')
        return dt.datetime.strptime(my_string, '%d-%b-%Y')
    elif len(my_string) == 24 and my_string[-3:] == 'GMT':
        my_string = my_string[4:-11]
        return dt.datetime.strptime(my_string, '%d%b%Y')
    elif len(my_string) == 23 and ' - ' in my_string:
        my_string = my_string.split(' - ')[0]
        return dt.datetime.strptime(my_string, '%Y-%m-%d')
    elif len(my_string) == 7 and my_string[:2] == '20':
        return dt.datetime.strptime(my_string, '%Y-%m').date()
    else:
        return my_string


def data_to_type(df, float_col=None, date_col=None, str_col=None, int_col=None,
                 fill_empty=True):
    df = df.loc[:, ~df.columns.duplicated()]
    if float_col is None:
        float_col = []
    if date_col is None:
        date_col = []
    if str_col is None:
        str_col = []
    if int_col is None:
        int_col = []
    for col in float_col:
        if col not in df:
            continue
        df[col] = df[col].fillna(0)
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: x.replace('$', ''))
        df[col] = df[col].apply(lambda x: x.replace(',', ''))
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].astype(float)
    for col in date_col:
        if col not in df:
            continue
        df[col] = df[col].replace(['1/0/1900', '1/1/1970'], '0')
        if fill_empty:
            df[col] = (
                df[col]
                .astype("object")
                .where(df[col].notna(), dt.date.today())
            )
        else:
            df[col] = df[col].fillna(pd.Timestamp('nat'))
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: string_to_date(x))
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()
    for col in str_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
        df[col] = df[col].str.replace(r"\s+", " ", regex=True).str.strip()
    for col in int_col:
        if col not in df:
            continue
        df[col] = df[col].astype(int)
    return df


def first_last_adj(df, first_row, last_row):
    """
    Modifies dataframe based on the first and last rows provided. If first_row
    is greater than zero, sets the dataframe's columns to the row above
    first_row and removes any rows above first_row in the df. Logs a warning
    if the df columns are null. If last_row is greater than zero, removes
    last_row number of rows from the end of the df.

    :param df:Dataframe to adjust
    :param first_row:Index of first row of data in df
    :param last_row:Index of last row of data in df
    :returns:Adjusted dataframe
    """
    logging.debug('Removing First & Last Rows')
    if df.empty:
        logging.warning('Empty df did not adjust first and last rows')
        return df
    first_row = int(first_row)
    last_row = int(last_row)
    if first_row > 0:
        df.columns = df.loc[first_row - 1]
        df = df.iloc[first_row:]
    if 0 < abs(last_row) < len(df):
        df = df.iloc[:-abs(last_row)]
    if pd.isnull(df.columns.values).any():
        logging.warning('At least one column name is undefined.  Your first'
                        'row is likely incorrect. For reference the first few'
                        'rows are:\n' + str(df.head()))
    return df


def date_removal(df, date_col_name, start_date, end_date):
    if (end_date.date() is not pd.NaT and
            end_date.date() != dt.date.today()):
        df = df[df[date_col_name] <= end_date]
    if (start_date.date() is not pd.NaT and
            start_date.date() != dt.date.today()):
        df = df[df[date_col_name] >= start_date]
    return df


def col_removal(df, key, removal_cols, warn=True):
    """
    Drops columns from a df

    :param df: The df to remove columns from
    :param key: Key string for logging purposes
    :param removal_cols: List of column names to remove
    :param warn: Boolean to log columns that were missing
    :return: The df with columns removed
    """
    logging.debug('Dropping unnecessary columns')
    if 'ALL' in removal_cols:
        plan_cols = [x + vmc.planned_suffix for x in vmc.datafloatcol]
        removal_cols = [x for x in df.columns
                        if x not in dctc.COLS + vmc.datacol + vmc.ad_rep_cols +
                        removal_cols + plan_cols]
    removal_cols = set(removal_cols)
    missing_cols = removal_cols.difference(df.columns)
    removal_cols = removal_cols.intersection(df.columns)
    if warn:
        for col in missing_cols:
            if col == 'nan':
                continue
            msg = '{} is not in {}.  It was not removed.'.format(col, key)
            logging.warning(msg)
    if removal_cols:
        df = df.drop(columns=list(removal_cols))
    return df


def apply_rules(df, vm_rules, pre_or_post, **kwargs):
    grouped_q_idx = {}
    for rule in vm_rules:
        for item in RULE_CONST:
            if item not in vm_rules[rule].keys():
                logging.warning('{} not in vendormatrix for rule {}.  '
                                'The rule did not run.'.format(item, rule))
                return df
        metrics = kwargs[vm_rules[rule][RULE_METRIC]]
        queries = kwargs[vm_rules[rule][RULE_QUERY]]
        factor = kwargs[vm_rules[rule][RULE_FACTOR]]
        if (str(metrics) == 'nan' or str(queries) == 'nan' or
                str(factor) == 'nan'):
            continue
        metrics = metrics.split('::')
        if metrics[0] != pre_or_post:
            continue
        set_column_to_value = False
        if len(metrics) == 3:
            set_column_to_value = True
            if metrics[1] not in df.columns:
                logging.warning(
                    '{} not in columns setting to 0.'.format(metrics[1]))
                df[metrics[1]] = 0
            if metrics[2] in grouped_q_idx:
                df.loc[~df.index.isin(grouped_q_idx[metrics[2]]),
                       metrics[2]] = (
                    df.loc[~df.index.isin(grouped_q_idx[metrics[2]]),
                           metrics[1]])
            else:
                df[metrics[2]] = df[metrics[1]]
            metrics[1] = metrics[2]
        tdf = df
        metrics = metrics[1].split('|')
        queries = queries.split('|')
        for query in queries:
            query = query.split('::')
            if len(query) == 1:
                logging.warning('Malformed query: {} \n In rule: {} \n'
                                'It may only have one :.  It was not used to'
                                'filter data'.format(query, rule))
                continue
            values = query[1].split(',')
            if query[0] not in df:
                logging.warning('{} not in data for rule {}.  '
                                'The rule did not run.'.format(query[0], rule))
                return df
            if query[0] == vmc.date:
                sd = string_to_date(values[0])
                ed = string_to_date(values[1])
                tdf = tdf.loc[(df[query[0]] >= sd) & (df[query[0]] <= ed)]
            else:
                if len(query) == 3 and query[2] == 'EXCLUDE':
                    tdf = tdf.loc[~tdf[query[0]].isin(values)]
                else:
                    tdf = tdf.loc[tdf[query[0]].isin(values)]
        q_idx = list(tdf.index.values)
        for metric in metrics:
            if metric not in df:
                logging.warning('{} not in data for rule {}.  '
                                'The rule did not run.'.format(metric, rule))
                continue
            df = data_to_type(df, float_col=[metric])
            df.loc[q_idx, metric] = (df.loc[q_idx, metric].astype(float) *
                                     float(factor))
            if set_column_to_value:
                if metric not in grouped_q_idx:
                    grouped_q_idx[metric] = q_idx
                else:
                    grouped_q_idx[metric].extend(q_idx)
    for metric in grouped_q_idx:
        if metric not in df:
            continue
        df.loc[~df.index.isin(grouped_q_idx[metric]), metric] = (
                df.loc[~df.index.isin(grouped_q_idx[metric]), metric]
                .astype(float) * 0)
    return df


def add_header(df, header, first_row):
    if str(header) == 'nan' or first_row == 0:
        return df
    df[header] = df.columns[0]
    df.set_value(first_row - 1, header, header)
    return df


def add_dummy_header(df, header_len, location='head'):
    cols = df.columns
    dummy_df = pd.DataFrame(data=[cols] * header_len, columns=cols)
    if location == 'head':
        df = pd.concat([dummy_df, df]).reset_index(drop=True)
    elif location == 'foot':
        df = pd.concat([df, dummy_df]).reset_index(drop=True)
    return df


def get_default_format(col):
    if 'Cost' in col or col[:2] == 'CP':
        format_map = '${:,.2f}'.format
    elif 'VCR' in col or col[-2:] == 'TR':
        format_map = '{:,.2%}'.format
    else:
        format_map = '{:,.0f}'.format
    return format_map


def give_df_default_format(df, columns=None):
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(0)
    if not columns:
        columns = df.columns
    for col in columns:
        format_map = get_default_format(col)
        try:
            df[col] = df[col].map(format_map)
        except ValueError as e:
            logging.warning('ValueError: {}'.format(e))
    return df


def db_df_translation(columns=None, proc_dir='', reverse=False):
    df = import_read_csv(
        os.path.join(proc_dir, config_path, db_df_trans_config))
    if not columns or df.empty:
        return {}
    if reverse:
        translation = dict(zip(df[exc.translation_db], df[exc.translation_df]))
    else:
        translation = dict(zip(df[exc.translation_df], df[exc.translation_db]))
    return {x: translation[x] if x in translation else x for x in columns}


def rename_duplicates(old):
    seen = []
    root_dict = {}
    for x in old:
        if x in seen:
            if re.search(r' (\d+-)*\d+$', x):
                split_x = x.split()
                root = ' '.join(split_x[:-1])
            else:
                root = x
            if root not in root_dict:
                root_dict[root] = 1
            new_val = x
            while new_val in old:
                new_val = '{} {}'.format(root, root_dict[root])
                root_dict[root] += 1
            yield new_val
        else:
            seen.append(x)
            yield x


def date_check(sd, ed):
    sd = sd.date()
    ed = ed.date()
    if sd > ed:
        logging.warning('Start date greater than end date.  Start date '
                        'was set to end date.')
        sd = ed
    return sd, ed


def filter_df_on_col(df, col_name, col_val, exclude=False):
    if col_name not in df.columns:
        logging.warning('Unable to filter df. Column "{}" '
                        'not in datasource.'.format(col_name))
        return df
    df = df.dropna(subset=[col_name])
    df = df.reset_index(drop=True)
    if exclude:
        df = df[~df[col_name].astype('U').str.contains(col_val)]
    else:
        df = df[df[col_name].astype('U').str.contains(col_val)]
    return df


def image_to_binary(file_name, as_bytes_io=False):
    if os.path.isfile(file_name):
        with open(file_name, 'rb') as image_file:
            image_data = image_file.read()
            if as_bytes_io:
                image_data = io.BytesIO(image_data)
    else:
        logging.warning('{} does not exist returning None'.format(file_name))
        image_data = None
    return image_data


def base64_to_binary(data):
    data = data.split(',')[1]
    decoded_bytes = base64.b64decode(data)
    return io.BytesIO(decoded_bytes)


def write_df_to_buffer(df, file_name='raw', default_format=True,
                       base_folder=''):
    csv_file = '{}{}'.format(file_name, '.csv')
    if default_format:
        gzip_extension = '.gzip'
    else:
        gzip_extension = '.gz'
    zip_file = '{}{}'.format(file_name, gzip_extension)
    today_yr = dt.datetime.strftime(dt.datetime.today(), '%Y')
    today_str = dt.datetime.strftime(dt.datetime.today(), '%m%d')
    today_folder_name = '{}/{}/'.format(today_yr, today_str)
    product_name = '{}_{}'.format(df['uploadid'].unique()[0],
                                  '_'.join(df['productname'].unique()))
    product_name = re.sub(r'\W+', '', product_name)
    zip_file = '{}/{}{}/{}'.format(
        base_folder, today_folder_name, product_name, zip_file)
    buffer = io.BytesIO()
    with gzip.GzipFile(filename=csv_file, fileobj=buffer, mode="wb") as f:
        f.write(df.to_csv().encode())
    buffer.seek(0)
    return buffer, zip_file


class SignalTimeoutException(Exception):
    pass


def signal_handler(signum, frame):
    raise SignalTimeoutException("Function timed out")


def poll_until_true(func, func_kwargs=None, attempts=20, sleep=.1,
                    raise_on_fail=True,
                    exception_msg='Polling timed out before true.',
                    click_elem_id='', load_elem_id=''):
    """
    Polls the specified function with the provided kwargs (if any) until it
    returns true or the max number of attempts is reached. If function fails to
    return true and raise_on_fail is set to true, raises exception.

    :param func: Name of function to be polled
    :param func_kwargs: Dictionary of keyword arguments to be passed into func
    :param attempts: The amount of times to check before returning/ raising
    exception; 20 by default
    :param sleep: Time in seconds to sleep between polling; .1 s by default
    :param raise_on_fail: Whether to raise an exception if function fails to
    return true before the max number of attempts is reached; True by default
    :param exception_msg: Text to give exception if raised;
    'Polling timed out before true.' if not specified
    :param click_elem_id: The original element id that was clicked
    :param load_elem_id: The element that loads after click
    :return: Whether polling succeeded in getting a true value
    """
    return_val = False
    for x in range(attempts):
        if not func_kwargs:
            func_kwargs = {}
        return_val = func(**func_kwargs)
        if return_val:
            break
        if click_elem_id and x > (attempts / 2):
            SeleniumWrapper.xpath_from_id_and_click(
                SeleniumWrapper, click_elem_id, load_elem_id=load_elem_id)
        time.sleep(sleep)
    if not return_val and raise_on_fail:
        raise Exception(exception_msg)
    return return_val


class SeleniumWrapper(object):
    driver_path = 'drivers'
    selectize_xpath = 'selectized'
    liquid_xpath = 'liquid'

    def __init__(self, mobile=False, headless=True):
        self.mobile = mobile
        self.headless = headless
        self.browser, self.co = self.init_browser(self.headless)
        self.base_window = self.browser.window_handles[0]
        self.select_id = By.ID
        self.select_class = By.CLASS_NAME
        self.select_xpath = By.XPATH
        self.select_css = By.CSS_SELECTOR
        self.use_js_click = False

    @staticmethod
    def get_chrome_version():
        """
        Get the installed version of Chrome.
        :return: version
        """
        win_path = 'HKEY_CURRENT_USER\\Software\\Google\\Chrome\\BLBeacon'
        out_list = ['reg', 'query', win_path, '/v', 'version']
        reg_search = r'(\d+\.\d+\.\d+\.\d+)'
        output = check_output(out_list).decode()
        version = re.search(reg_search, output).group(1)
        return version

    @staticmethod
    def get_chromedriver_version(chrome_version):
        """
        Fetch the corresponding ChromeDriver version for the given Chrome version.
        """
        driver_version = chrome_version
        major_version = chrome_version.split('.')[0]
        url = (f'https://googlechromelabs.github.io/chrome-for-testing/'
               f'known-good-versions-with-downloads.json')
        r = requests.get(url)
        for version in r.json()['versions']:
            if version['version'].startswith(major_version):
                driver_version =  version['version']
                break
        return driver_version

    def download_chromedriver(self, version):
        """
        Download the correct version of ChromeDriver.
        """
        url = 'https://storage.googleapis.com/chrome-for-testing-public/'
        file_name = 'chromedriver-win64.zip'
        url = '{}{}/win64/{}'.format(url, version, file_name)
        response = requests.get(url)
        with open(file_name, "wb") as file:
            file.write(response.content)
        dir_check(self.driver_path)
        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall(self.driver_path)
        os.remove(file_name)
        logging.info(f"Downloaded ChromeDriver version {version}")
        """
        destination = 'C:/Windows/chromedriver.exe'
        file_name = os.path.join(
            self.driver_path, file_name.replace('.zip', ''),
            file_name.replace('-win64.zip', '.exe'))
        shutil.move(file_name, destination)
        """

    @staticmethod
    def get_random_user_agent():
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 "
            "Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 "
            "Firefox/89.0"
        ]
        random_user_agent = random.choice(user_agents)
        return random_user_agent

    def init_browser(self, headless):
        download_path = os.path.join(os.getcwd(), 'tmp')
        co = wd.chrome.options.Options()
        if headless:
            co.add_argument('--headless=new')
        random_user_agent = self.get_random_user_agent()
        co.add_argument('user-agent={}'.format(random_user_agent))
        co.add_argument('--disable-features=VizDisplayCompositor')
        co.add_argument('--window-size=1920,1080')
        co.add_argument('--start-maximized')
        co.add_argument('--no-sandbox')
        co.add_argument('--disable-gpu')
        prefs = {'download.default_directory': download_path,
                 "credentials_enable_service": False,
                 "profile.password_manager_enabled": False
                 }
        co.add_experimental_option('prefs', prefs)
        co.add_experimental_option('excludeSwitches', ['enable-automation'])
        co.add_experimental_option('useAutomationExtension', False)
        co.add_argument('--disable-blink-features=AutomationControlled')
        if self.mobile:
            mobile_emulation = {"deviceName": "iPhone X"}
            co.add_experimental_option("mobileEmulation", mobile_emulation)
        try:
            browser = wd.Chrome(options=co)
        except (ex.SessionNotCreatedException, FileNotFoundError) as e:
            logging.warning(e)
            chrome_version = self.get_chrome_version()
            driver_version = self.get_chromedriver_version(chrome_version)
            self.download_chromedriver(driver_version)
            browser = wd.Chrome(options=co)
        browser.execute_script("""
            Object.defineProperty(navigator, 'webdriver', 
            { get: () => undefined });
            window.navigator.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'plugins', 
            { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', 
            { get: () => ['en-US', 'en'] });
        """)
        co.page_load_strategy = 'none'
        browser.maximize_window()
        browser.set_script_timeout(10)
        browser.set_page_load_timeout(10)
        self.enable_download_in_headless_chrome(browser, download_path)
        return browser, co

    @staticmethod
    def enable_download_in_headless_chrome(driver, download_dir):
        # add missing support for chrome "send_command"  to selenium webdriver
        driver.command_executor._commands["send_command"] = \
            ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow', 'downloadPath': download_dir}}
        driver.execute("send_command", params)

    @staticmethod
    def random_delay(min_time=0.5, max_time=2.5):
        time.sleep(random.uniform(min_time, max_time))

    def go_to_url(self, url, sleep=5, elem_id=''):
        logging.info('Going to url {}.'.format(url))
        max_attempts = 10
        for x in range(max_attempts):
            try:
                self.browser.get(url)
                break
            except (ex.TimeoutException, ex.WebDriverException) as e:
                msg = 'Exception attempt: {}, retrying: \n {}'.format(x + 1, e)
                logging.warning(msg)
                if x > (max_attempts - 2):
                    logging.warning(
                        'Reached max attempts. Restarting browser...')
                    try:
                        self.quit()
                    except Exception as e:
                        logging.warning(f'Error during browser quit: {e}')
                    try:
                        self.browser, self.co = self.init_browser(
                            self.headless)
                        self.base_window = self.browser.window_handles[0]
                        self.browser.get(url)
                    except Exception as e:
                        logging.error(f'Restart and reload failed: {e}')
                        return False
        if elem_id:
            self.wait_for_elem_load(elem_id)
        else:
            time.sleep(sleep)
        return True

    @staticmethod
    def click_on_elem(elem, sleep=2):
        elem.click()
        time.sleep(sleep)

    def scroll_to_elem(self, elem,
                       scroll_script="arguments[0].scrollIntoView();"):
        self.browser.execute_script(scroll_script, elem)

    def click_error(self, elem, e, attempts=0):
        if self.use_js_click:
            try:
                self.browser.execute_script("arguments[0].click();", elem)
                return True
            except (ex.ElementNotInteractableException,
                    ex.ElementClickInterceptedException,
                    ex.StaleElementReferenceException) as e:
                logging.warning('Could not click JS: {}'.format(e))
        elem_id = ''
        try:
            elem_id = elem.get_attribute('id')
        except ex.StaleElementReferenceException as stale_error:
            logging.warning(stale_error)
        if elem_id:
            log_val = elem_id
        else:
            log_val = elem
        logging.info('Element: {}\nError: {}'.format(log_val, e))
        scroll_script = "arguments[0].scrollIntoView({block:'center'});"
        if attempts > 5:
            scroll_script = "window.scrollTo(0, 0)"
        try:
            self.scroll_to_elem(elem, scroll_script)
        except ex.StaleElementReferenceException as e:
            logging.warning(e)
        time.sleep(.1)
        return False

    def click_on_xpath(self, xpath='', sleep=2, elem=None):
        elem_click = True
        attempts = 10
        for x in range(attempts):
            cur_elem = elem
            if xpath:
                cur_elem = self.browser.find_element_by_xpath(xpath)
            try:
                self.click_on_elem(cur_elem, sleep)
            except (ex.ElementNotInteractableException,
                    ex.ElementClickInterceptedException,
                    ex.StaleElementReferenceException) as e:
                elem_click = self.click_error(cur_elem, e, x)
            if elem_click:
                break
            else:
                elem_click = True
        if not elem_click:
            tt = attempts * sleep
            msg = 'Xpath: {} not clicked in {}s'.format(xpath, tt)
            raise Exception(msg)
        return elem_click

    def quit(self):
        try:
            self.browser.close()
        except ex.WebDriverException as e:
            logging.warning('Error closing: {}'.format(e))
        self.browser.quit()

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
                df = import_read_csv(temp_file, empty_df=True)
                os.remove(temp_file)
                break
            time.sleep(5)
        shutil.rmtree(temp_path)
        return df

    def take_screenshot_get_ads(self, url=None, file_name=None):
        self.take_screenshot(url=url, file_name=file_name)
        ads = self.get_all_iframe_ads()
        return ads

    def click_accept_buttons(self, btn_xpath, timeout=3, poll_frequency=0.2):
        use_alarm = hasattr(signal, "SIGALRM")
        if use_alarm:
            signal.signal(signal.SIGALRM, signal_handler)
            signal.alarm(timeout + 1)
        wait = WebDriverWait(self.browser, timeout,
                             poll_frequency=poll_frequency)
        try:
            accept_buttons = wait.until(
                EC.visibility_of_all_elements_located((By.XPATH, btn_xpath)))
        except (ex.TimeoutException, SignalTimeoutException) as e:
            accept_buttons = None
        finally:
            if use_alarm:
                signal.alarm(0)
        if accept_buttons:
            self.click_on_xpath(sleep=3, elem=accept_buttons[0])

    def accept_cookies(self):
        btn = ['AKZEPTIEREN UND WEITER', 'Accept Cookies', 'OK',
               'Accept All Cookies', 'Zustimmen', 'Accetto', "J'ACCEPTE",
               'Accetta', 'I agree', 'Continue', 'Proceed']
        btn_xpath = [
            """//*[contains(normalize-space(text()), "{}")]""".format(x)
            for x in btn]
        btn_xpath = ' | '.join(btn_xpath)
        self.click_accept_buttons(btn_xpath)
        try:
            iframes = self.browser.find_elements(By.TAG_NAME, "iframe")
        except http_client.CannotSendRequest as e:
            logging.warning(e)
            iframes = []
        for iframe in iframes:
            try:
                is_displayed = iframe.is_displayed()
            except ex.StaleElementReferenceException as e:
                logging.warning(e)
                is_displayed = False
            if is_displayed:
                try:
                    self.browser.switch_to.frame(iframe)
                except ex.WebDriverException as e:
                    logging.warning(e)
                    continue
                self.click_accept_buttons(btn_xpath)
                try:
                    self.browser.switch_to.default_content()
                except http_client.CannotSendRequest as e:
                    logging.warning(e)
                    continue

    def take_screenshot(self, url=None, file_name=None):
        logging.info('Getting screenshot from {} and '
                     'saving to {}.'.format(url, file_name))
        went_to_url = True
        if url:
            went_to_url = self.go_to_url(url)
        if went_to_url:
            self.accept_cookies()
            self.browser.execute_script("window.scrollTo(0, 0)")
            self.browser.save_screenshot(file_name)

    def take_elem_screenshot(self, url=None, xpath=None, file_name=None):
        logging.info('Getting screenshot from {} and '
                     'saving to {}.'.format(url, file_name))
        self.go_to_url(url, sleep=10)
        elem = self.browser.find_element_by_xpath(xpath)
        elem.screenshot(file_name)

    def get_all_iframes(self, url=None):
        if url:
            self.go_to_url(url)
        all_iframes = self.browser.find_elements_by_tag_name('iframe')
        all_iframes = [x for x in all_iframes if x.is_displayed()]
        return all_iframes

    def get_all_iframe_ads(self, url=None):
        ads = []
        all_iframes = self.get_all_iframes(url)
        for iframe in all_iframes:
            iframe_properties = {}
            for x in ['width', 'height']:
                try:
                    iframe_properties[x] = iframe.get_attribute(x)
                except ex.StaleElementReferenceException:
                    logging.warning('{} element not gathered.'.format(x))
                    iframe_properties[x] = 'None'
            iframe.click()
            if len(self.browser.window_handles) > 1:
                new_window = [x for x in self.browser.window_handles
                              if x != self.base_window][0]
                self.browser.switch_to.window(new_window)
                time.sleep(5)
                iframe_properties['lp_url'] = self.browser.current_url
                logging.info('Got iframe with properties:'
                             ' {}'.format(iframe_properties))
                ads.append(iframe_properties)
                self.browser.close()
                self.browser.switch_to.window(self.base_window)
            time.sleep(5)
        return ads

    def clear_elem(self, elem_id, attempts=10, sleep_time=.1):
        """Clear an input, retrying while it is not yet interactable.

        Mirrors ``send_keys_wrapper``: re-fetches the element between
        attempts so stale references don't propagate, and gives the
        browser a 100ms polling window for cells that transition into
        editable inputs asynchronously after a click.
        """
        for attempt in range(attempts):
            try:
                elem = self.browser.find_element(By.ID, elem_id)
                self.browser.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", elem)
                elem.clear()
                return
            except (ex.ElementNotInteractableException,
                    ex.StaleElementReferenceException):
                if attempt == attempts - 1:
                    raise
                time.sleep(sleep_time)

    def send_keys_wrapper(self, elem, value, elem_xpath=''):
        elem_sent = True
        for x in range(10):
            try:
                elem.send_keys(value)
            except (ex.ElementNotInteractableException,
                    ex.StaleElementReferenceException) as e:
                if elem_xpath:
                    elem = self.browser.find_element_by_xpath(elem_xpath)
                elem_sent = self.click_error(elem, e)
            if elem_sent:
                break
            else:
                elem_sent = True
        return elem_sent

    def send_multiple_keys_wrapper(self, elem, items):
        for item in items:
            self.send_keys_wrapper(elem, item)
            wd.ActionChains(self.browser).send_keys(Keys.TAB).perform()

    def get_elem_type(self, elem_xpath, elem):
        elem_type = ''
        for x in range(10):
            try:
                elem_type = elem.get_attribute('type')
                break
            except ex.StaleElementReferenceException as e:
                logging.warning(e)
                elem = self.browser.find_element_by_xpath(elem_xpath)
                time.sleep(.1)
        return elem_type

    def send_key_from_list(self, item, get_xpath_from_id=True,
                           clear_existing=True, send_escape=True):
        elem_xpath = item[1]
        if get_xpath_from_id:
            if '-selectized' in elem_xpath or '-liquid' in elem_xpath:
                elem_xpath = self.resolve_select_id(elem_xpath)
            elem_xpath = self.get_xpath_from_id(elem_xpath)
        elem = self.browser.find_element_by_xpath(elem_xpath)
        is_liquid = self.liquid_xpath in elem_xpath
        clear_specified = len(item) > 2 and item[2] == 'clear'
        if clear_existing and (is_liquid or clear_specified):
            self._click_liquid_clear_button(elem)
        elem_type = self.get_elem_type(elem_xpath, elem)
        if elem_type == 'checkbox':
            self.click_on_xpath(elem=elem, sleep=.1)
        elif isinstance(item[0], list):
            self.send_multiple_keys_wrapper(elem, item[0])
        else:
            self.send_keys_wrapper(elem, item[0], elem_xpath)
        return elem

    def _click_liquid_clear_button(self, elem):
        """Click the clear-all X on a LiquidSelect input, if present."""
        clear_x = (
            'following-sibling::div[contains(@class, "lq-actions")]'
            '/span[contains(@class, "lq-btn-clear")]')
        for _ in range(5):
            try:
                matches = elem.find_elements_by_xpath(clear_x)
                break
            except ex.StaleElementReferenceException as e:
                logging.warning(e)
                matches = []
                time.sleep(.1)
        if matches:
            self.click_on_xpath(elem=matches[0], sleep=.1)

    def send_key_new_value_check(self, item, get_xpath_from_id=True,
                                 clear_existing=True, send_escape=True,
                                 elem=None):
        attempts = 2
        for x in range(attempts):
            try:
                self.wait_for_elem_load(item[1], new_value=item[0],
                                        attempts=25)
                break
            except:
                logging.warning('{} could not load.'.format(item))
                elem = self.send_key_from_list(item, get_xpath_from_id,
                                               clear_existing, send_escape)
        return elem

    def send_keys_from_list(self, elem_input_list, get_xpath_from_id=True,
                            clear_existing=True, send_escape=True,
                            new_value='', choose_existing=False):
        for item in elem_input_list:
            elem = self.send_key_from_list(
                item, get_xpath_from_id, clear_existing, send_escape)
            if not self._is_liquid_xpath(item[1], get_xpath_from_id):
                continue
            if new_value:
                elem = self.send_key_new_value_check(
                    item, get_xpath_from_id, clear_existing, send_escape,
                    elem=elem)
            for _ in range(3):
                try:
                    elem.send_keys(Keys.ENTER)
                    break
                except (ex.ElementNotInteractableException,
                        ex.StaleElementReferenceException) as e:
                    logging.warning(e)
            if send_escape:
                wd.ActionChains(self.browser).send_keys(
                    Keys.ESCAPE).perform()

    def _is_liquid_xpath(self, elem_xpath, get_xpath_from_id):
        """True when ``elem_xpath`` drives a LiquidSelect widget —
        either because it already carries the ``-liquid`` suffix, or
        because its ``-selectized`` id resolves to one."""
        if self.liquid_xpath in elem_xpath:
            return True
        if not get_xpath_from_id:
            return False
        if self.selectize_xpath not in elem_xpath:
            return False
        return self.liquid_xpath in self.resolve_select_id(elem_xpath)

    def xpath_from_id_and_click(self, elem_id, sleep=2, load_elem_id=''):
        if load_elem_id:
            sleep = .01
        elem_xpath = self.get_xpath_from_id(elem_id)
        self.click_on_xpath(elem_xpath, sleep)
        if load_elem_id:
            try:
                self.wait_for_elem_load(load_elem_id, attempts=200)
            except Exception as e:
                logging.warning('Attempt to re-click: {}'.format(e))
                self.click_on_xpath(elem_xpath, sleep)
                self.wait_for_elem_load(load_elem_id, attempts=800)

    @staticmethod
    def get_xpath_from_id(elem_id):
        return '//*[@id="{}"]'.format(elem_id)

    def wait_for_elem_load(self, elem_id, selector=None, attempts=1000,
                           sleep_time=.01, visible=False, new_value='',
                           attribute='value', raise_exception=True):
        """
        Incrementally checks if an element exists. If raise_exception is True,
        raises exception if an element fitting the provided parameter conditions
        does not exist by the specified number of attempts.

        :param elem_id: Identifier of the element to monitor (e.x. ID)
        :param selector: Method to find the element by based on the elem_id
        provided; set to 'By.ID' if None
        :param attempts: The amount of times to check before raising exception
        or exiting; 1000 by default
        :param sleep_time: Time in seconds to sleep between polling; .01 s by
        default
        :param visible: Whether the element needs to be visible before being
        considered loaded; False by default
        :param new_value: Value the element's attribute, specified by the
        attribute parameter, should match before being considered loaded, if
        any; empty string by default
        :param attribute: Attribute of the element that must match new_value, if
        specified, before the element is considered loaded; 'value' by default
        :param raise_exception: Whether to raise an exception if element does
        not load; True by default
        :return: Whether an element fitting the provided parameter conditions
        exists by the maximum number of attempts
        """
        selector = selector if selector else self.select_id
        elem_found = False
        widget_suffixed = (
            selector == self.select_id
            and isinstance(elem_id, str)
            and ('-selectized' in elem_id or '-liquid' in elem_id))
        for x in range(attempts):
            lookup_id = (
                self.resolve_select_id(elem_id)
                if widget_suffixed else elem_id)
            e = self.browser.find_elements(selector, lookup_id)
            if e:
                elem_visible = True
                if visible:
                    try:
                        elem_visible = e[0].is_displayed()
                    except ex.StaleElementReferenceException:
                        e = self.browser.find_elements(selector, elem_id)
                        elem_visible = e[0].is_displayed()
                if new_value:
                    try:
                        cur_value = e[0].get_attribute(attribute)
                    except ex.StaleElementReferenceException:
                        cur_value = ''
                    if new_value not in cur_value:
                        elem_visible = False
                if elem_visible:
                    elem_found = True
                    break
            time.sleep(sleep_time)
        if not elem_found:
            tt = attempts * sleep_time
            msg = 'Element {} not found in {}s.'.format(elem_id, tt)
            if raise_exception:
                file_name = 'NOT_FOUND_ERROR.png'
                self.take_screenshot(file_name=file_name)
                raise Exception(msg)
        return elem_found

    def wait_for_elem_disappear(self, elem_id, selector=None, attempts=5,
                                sleep_time=.1, raise_exception=True):
        """
        Incrementally checks if an element exists. If raise_exception is True,
        raises exception if the element has not disappeared/ ceased to exist by
        the specified number of attempts.

        :param elem_id: Identifier of the element to monitor (e.x. ID)
        :param selector: Method to find the element by based on the elem_id
        provided; set to 'By.ID' if None
        :param attempts: The amount of times to check before raising exception
        or exiting; 5 by default
        :param sleep_time: Time in seconds to sleep between polling; .1 s by
        default
        :param raise_exception: Whether to raise an exception if element does
        not disappear; True by default
        :return: Whether the element disappeared by the maximum number of
        attempts
        """
        selector = selector if selector else self.select_id
        func_kwargs = {'sw': self, 'elem_id': elem_id, 'selector': selector}
        func = (lambda sw, elem_id, selector:
                not sw.browser.find_elements(selector, elem_id))
        exception_msg = 'Element {} did not disappear.'.format(elem_id)
        task_complete = poll_until_true(func, func_kwargs, attempts, sleep_time,
                                        raise_exception, exception_msg)
        return task_complete

    def drag_and_drop(self, elem, target):
        action_chains = wd.ActionChains(self.browser)
        action_chains.drag_and_drop(elem, target).perform()

    def get_element_order(self, elem1_id, elem2_id):
        elem1 = self.browser.find_element_by_id(elem1_id)
        elem2 = self.browser.find_element_by_id(elem2_id)
        first_element_position = self.browser.execute_script(
            "return arguments[0].getBoundingClientRect().top", elem1)
        second_element_position = self.browser.execute_script(
            "return arguments[0].getBoundingClientRect().top", elem2)
        return first_element_position < second_element_position

    def count_rows_in_table(self, elem_id=''):
        """Count visible data rows in a table.

        LiquidTable interleaves each visible ``trN`` with a hidden
        ``trHiddenN`` form row, so the Hidden ones must be filtered.
        Returns 0 when no tbody exists yet (empty tables haven't had
        ``addRowToTable`` lazily create one).
        """
        elem = self.browser.find_element
        if elem_id:
            elem = elem(By.ID, elem_id)
        tbodies = elem.find_elements(By.CSS_SELECTOR, "table > tbody")
        if not tbodies:
            return 0
        rows = tbodies[0].find_elements(By.TAG_NAME, "tr")
        return sum(1 for r in rows
                   if 'Hidden' not in (r.get_attribute('id') or ''))

    def resolve_select_id(self, base_id):
        """Return the LiquidSelect wrapper input id for a select element.

        Accepts a bare base id (``partnerSelect``) or either legacy
        suffix (``partnerSelect-selectized`` / ``-liquid``); the suffix
        is stripped before lookup so tests written against the old
        Selectize id keep working unchanged."""
        for suffix in (f'-{self.liquid_xpath}', f'-{self.selectize_xpath}'):
            if base_id.endswith(suffix):
                base_id = base_id[: -len(suffix)]
                break
        return f'{base_id}-{self.liquid_xpath}'

    def submit_form(self, form_names=None, select_form_names=None,
                    submit_id='loadContinue', test_name='test',
                    clear_existing=True, send_escape=True, new_value='',
                    choose_existing=False):
        """
        Fill a form and optionally submit it.

        :param form_names: List of element ids for form items
        :param select_form_names: List of element ids for select items
        :param submit_id: Element id clicked to submit the form, or falsy
            to skip submission
        :param test_name: Value entered into every non-date form item
        :param clear_existing: Remove the existing value before entry
        :param send_escape: Press escape after filling each select item
        :param new_value: Value to wait for in the first field before submit
        :param choose_existing: Unused — retained for call-site compat
        """
        form_names = form_names or []
        select_form_names = select_form_names or []
        elem_form = []
        for raw_id in form_names + select_form_names:
            if self._looks_like_select_id(raw_id, select_form_names):
                form_name = self.resolve_select_id(raw_id)
            else:
                form_name = raw_id
            if 'date' in form_name:
                form_val = dt.datetime.now().strftime('%m-%d-%Y')
            else:
                form_val = test_name
            elem_form.append((form_val, form_name))
        if test_name:
            self.send_keys_from_list(
                elem_form, clear_existing=clear_existing,
                send_escape=send_escape, new_value=new_value,
                choose_existing=choose_existing)
        if submit_id:
            if new_value:
                first_id = elem_form[0][1]
                base_id = (first_id
                    .replace(f'-{self.selectize_xpath}', '')
                    .replace(f'-{self.liquid_xpath}', ''))
                self.wait_for_elem_load(base_id, new_value=new_value)
            self.xpath_from_id_and_click(submit_id, .01)

    def _looks_like_select_id(self, raw_id, select_form_names):
        """True when ``raw_id`` names a LiquidSelect-backed field."""
        return (
            raw_id in select_form_names
            or 'cur' in raw_id or 'Select' in raw_id
            or '-selectized' in raw_id or '-liquid' in raw_id)

    def select_widget_rendered(self, elem_id):
        """Return True when the LiquidSelect wrapper input for this
        select id exists in the DOM. Accepts a bare id or one with
        the legacy ``-selectized`` / ``-liquid`` suffix."""
        return bool(
            self.browser.find_elements(
                By.ID, self.resolve_select_id(elem_id)))

    def get_select_tag_elements(self, base_id):
        """Return the selected-tag chips inside a LiquidSelect wrapper.

        ``base_id`` may be a plain select id or a suffixed wrapper
        input id (``{base}-selectized`` / ``{base}-liquid``).
        """
        for suffix in (f'-{self.liquid_xpath}', f'-{self.selectize_xpath}'):
            if base_id.endswith(suffix):
                base_id = base_id[: -len(suffix)]
                break
        wraps = self.browser.find_elements(By.ID, f'lq-wrap-{base_id}')
        if not wraps:
            return []
        return wraps[0].find_elements(By.CSS_SELECTOR, '.lq-tag')

    def run_worker(self, worker, attempts=20, raise_on_fail=True):
        """
        Drain the given RQ worker until a task completes.

        :param worker: Worker exposing ``.work(burst=True)``
        :param attempts: Max polling attempts
        :param raise_on_fail: Raise if no task completes within attempts
        :return: Whether a task completed
        """
        return poll_until_true(
            lambda worker: worker.work(burst=True),
            {'worker': worker}, attempts, .1, raise_on_fail,
            'Worker did not complete task.')

    def click_and_run_worker(self, elem_id, worker, load_elem_id='',
                             worker_attempts=20):
        """Click an element by id then drain ``worker``."""
        self.xpath_from_id_and_click(elem_id, load_elem_id=load_elem_id)
        return self.run_worker(worker, attempts=worker_attempts)

    def navigate_and_submit(self, url, form_names=None,
                            select_form_names=None,
                            submit_id='loadContinue', worker=None,
                            **submit_kwargs):
        """Go to ``url``, fill & submit the form, drain ``worker`` if given."""
        self.go_to_url(url, elem_id=submit_id)
        self.submit_form(form_names=form_names,
                         select_form_names=select_form_names,
                         submit_id=submit_id, **submit_kwargs)
        if worker:
            self.run_worker(worker)

    def wait_for_condition(self, func, func_kwargs=None, attempts=20,
                           sleep=.1, raise_on_fail=True,
                           exception_msg='Condition not met.'):
        """Thin wrapper around ``poll_until_true`` for test ergonomics."""
        return poll_until_true(func, func_kwargs, attempts, sleep,
                               raise_on_fail, exception_msg)

    def check_app_alert(self, key_terms=None):
        """
        Return True if a toast stack entry matches any of ``key_terms``,
        or any toast exists when ``key_terms`` is falsy.
        """
        stack = self.browser.find_elements(
            By.CSS_SELECTOR, '#lqToastStack .lq-toast')
        if not stack:
            return False
        if not key_terms:
            return True
        try:
            combined_text = ' '.join(
                t.get_attribute('innerHTML') for t in stack)
        except ex.StaleElementReferenceException:
            return False
        found_terms = [x for x in key_terms if x in combined_text]
        return len(found_terms) > 0

    def wait_for_app_alert(self, key_terms=None, attempts=10, sleep=.1):
        """Poll until a toast matching ``key_terms`` appears."""
        exception_msg = 'App alert not found.'
        if key_terms:
            terms = ', '.join(key_terms)
            exception_msg = (
                f'App alert containing one or more of the terms '
                f'({terms}) not found.')
        return poll_until_true(
            self.check_app_alert, {'key_terms': key_terms}, attempts,
            sleep, exception_msg=exception_msg)


def copy_file(old_file, new_file, attempt=1, max_attempts=100, sleep=60):
    try:
        shutil.copy(old_file, new_file)
    except PermissionError as e:
        logging.warning('Could not copy {}: {}'.format(old_file, e))
    except OSError as e:
        attempt += 1
        if attempt > max_attempts:
            msg = 'Exceeded after {} attempts not copying {} {}'.format(
                max_attempts, old_file, e)
            logging.warning(msg)
        else:
            logging.warning('Attempt {}: could not copy {} due to OSError '
                            'retrying in 60s: {}'.format(attempt, old_file, e))
            time.sleep(sleep)
            copy_file(old_file, new_file, attempt=attempt,
                      max_attempts=max_attempts)


def copy_tree_no_overwrite(old_path, new_path, log=True, overwrite=False):
    old_files = os.listdir(old_path)
    for idx, file_name in enumerate(old_files):
        if log:
            logging.info(int((int(idx) / int(len(old_files))) * 100))
        old_file = os.path.join(old_path, file_name)
        new_file = os.path.join(new_path, file_name)
        if os.path.isfile(old_file):
            if os.path.exists(new_file) and not overwrite:
                continue
            else:
                copy_file(old_file, new_file)
        elif os.path.isdir(old_file):
            if not os.path.exists(new_file):
                os.mkdir(new_file)
            copy_tree_no_overwrite(old_file, new_file, log=False,
                                   overwrite=overwrite)


def lower_words_from_str(word_str, split_underscore=False):
    if split_underscore:
        words = re.split(r"[^a-z0-9']+", word_str.lower())
        words = [w for w in words if w]
    else:
        pattern = r"[\w']+|[.,!?;/]"
        words = re.findall(pattern, word_str.lower())
    return words


def index_words_from_list(word_list, word_idx, obj_to_append):
    if not word_idx:
        word_idx = {}
    for word in word_list:
        if word in word_idx:
            word_idx[word].append(obj_to_append)
        else:
            word_idx[word] = [obj_to_append]
    return word_idx


def is_list_in_list(first_list, second_list, contains=False, return_vals=False):
    in_list = False
    if contains:
        name_in_list = [x for x in first_list if
                        x in second_list or [y for y in second_list if x in y]]
    else:
        name_in_list = [x for x in first_list if x in second_list]
    if name_in_list:
        in_list = True
        if return_vals:
            in_list = name_in_list
    return in_list


def get_next_value_from_list(first_list, second_list):
    next_values = [first_list[idx + 1] for idx, x in enumerate(first_list) if
                   x in second_list]
    return next_values


def get_dict_values_from_list(list_search, dict_check, check_dupes=False):
    values_in_dict = []
    keys_added = []
    dict_key = next(iter(dict_check[0]))
    for x in dict_check:
        lower_val = str(x[dict_key]).lower()
        if lower_val in list_search:
            if (check_dupes and lower_val not in keys_added) or not check_dupes:
                keys_added.append(lower_val)
                values_in_dict.append(x)
    return values_in_dict


def check_dict_for_key(dict_to_check, key, missing_return_value=''):
    if key in dict_to_check:
        return_value = dict_to_check[key]
        if not return_value:
            return_value = missing_return_value
    else:
        return_value = missing_return_value
    return return_value


def get_next_number_from_list(words, lower_name, cur_model_name,
                              last_instance=False, break_words_list=None):
    if lower_name not in words:
        for x in lower_name.split('_'):
            if x in words:
                lower_name = x
                break
    post_words = words[words.index(lower_name):]
    if break_words_list:
        for idx, x in enumerate(post_words):
            if idx != 0 and x in break_words_list:
                post_words = post_words[:idx]
                break
    if last_instance:
        idx = next(i for i in reversed(range(len(post_words)))
                   if post_words[i] == lower_name)
        post_words = post_words[idx:]
    cost = [x for x in post_words if
            any(y.isdigit() for y in x) and
            x not in [cur_model_name, lower_name]]
    if cost:
        if len(cost) > 1:
            cost_append = ''
            post_words = post_words[post_words.index(cost[0]):]
            for x in range(1, len(post_words), 2):
                two_comb = post_words[x:x + 2]
                if len(two_comb) > 1 and two_comb[0] == ',':
                    cost_append += two_comb[1]
                else:
                    break
            cost = [cost[0] + cost_append]
        cost = cost[0].replace('$', '')
        cost = cost.replace('k', '000')
        cost = cost.replace('m', '000000')
    else:
        cost = 0
    if any(c.isalpha() for c in str(cost)):
        cost = 0
    return cost


def get_next_values_from_list(first_list, match_list=None, break_list=None,
                              date_search=False):
    name_list = ['named', 'called', 'name', 'title', 'categorized']
    if not match_list:
        match_list = name_list.copy()
    match_list = is_list_in_list(match_list, first_list, False, True)
    if not match_list:
        return []
    first_list = first_list[first_list.index(match_list[0]) + 1:]
    if break_list:
        for value in first_list:
            if value in break_list and value not in match_list:
                first_list = first_list[:first_list.index(value)]
                break
    first_list = [x for x in first_list if x not in name_list]
    delimit = ''
    if not date_search:
        first_list = [x.capitalize() for x in first_list
                      if not (x.isdigit() and int(x) > 10)]
        delimit = ' '
    first_list = delimit.join(first_list).split('.')[0].split(',')
    first_list = [x.strip(' ') for x in first_list]
    return first_list


def clean_monetary_input(monetary_input):
    """
    Remove commas, spaces, dollar signs, and k/m from monetary input values.

    :params monetary_input: Monetary input value to be cleaned
    :return: Inputted value as string formatted as float
    """
    if monetary_input is None:
        return '0'

    cleaned_input = str(monetary_input).lower()
    replace = [(',', ''), ('$', ''), (' ', ''), ('k', '000'),
               ('m', '000000')]
    for old, new in replace:
        cleaned_input = cleaned_input.replace(old, new)
    return cleaned_input


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
