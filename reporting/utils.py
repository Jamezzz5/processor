import os
import io
import re
import time
import shutil
import logging
import pandas as pd
import datetime as dt
import selenium.webdriver as wd
import reporting.vmcolumns as vmc
import reporting.dictcolumns as dctc
import selenium.common.exceptions as ex

config_path = 'config/'
raw_path = 'raw_data/'
error_path = 'ERROR_REPORTS/'
dict_path = 'dictionaries/'
backup_path = 'backup/'

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


def import_read_csv(filename, path=None, file_check=True, error_bad=True,
                    empty_df=False, nrows=None):
    sheet_names = []
    if sheet_name_splitter in filename:
        filename = filename.split(sheet_name_splitter)
        sheet_names = filename[1:]
        filename = filename[0]
    if path:
        filename = os.path.join(path, filename)
    if file_check:
        if not os.path.isfile(filename):
            logging.warning('{} not found.  Continuing.'.format(filename))
            return pd.DataFrame()
    file_type = os.path.splitext(filename)[1].lower()
    kwargs = {'parse_dates': True, 'keep_default_na': False,
              'na_values': na_values, 'nrows': nrows}
    if sheet_names:
        kwargs['sheet_name'] = sheet_names
    if file_type == '.xlsx':
        read_func = pd.read_excel
    else:
        read_func = pd.read_csv
    try:
        df = read_func(filename, encoding='utf-8',
                       error_bad_lines=error_bad, **kwargs)
    except pd.io.common.CParserError:
        df = read_func(filename, sep=None, engine='python', **kwargs)
    except UnicodeDecodeError:
        df = read_func(filename, encoding='iso-8859-1', **kwargs)
    except pd.io.common.EmptyDataError:
        logging.warning('Raw Data {} empty.  Continuing.'.format(filename))
        if empty_df:
            df = pd.DataFrame()
        else:
            df = None
            return df
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
    if file_type == '.xlsx':
        write_func = df.to_excel
    else:
        write_func = df.to_csv
    try:
        write_func(file_name, index=False, encoding='utf-8')
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
    if ('/' in my_string and my_string[-4:][:2] != '20' and
            ':' not in my_string and len(my_string) in [6, 7, 8]):
        try:
            return dt.datetime.strptime(my_string, '%m/%d/%y')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif ('/' in my_string and my_string[-4:][:2] == '20' and
            ':' not in my_string):
        return dt.datetime.strptime(my_string, '%m/%d/%Y')
    elif (((len(my_string) == 5) and (my_string[0] == '4')) or
            ((len(my_string) == 7) and ('.' in my_string))):
        return exceldate_to_datetime(float(my_string))
    elif len(my_string) == 8 and my_string.isdigit() and my_string[0] == '2':
        return dt.datetime.strptime(my_string, '%Y%m%d')
    elif len(my_string) == 8 and '.' in my_string:
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
        return dt.datetime.strptime(my_string, '%Y-%m-%d')
    else:
        return my_string


def data_to_type(df, float_col=None, date_col=None, str_col=None, int_col=None,
                 fill_empty=True):
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
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: x.replace('$', ''))
        df[col] = df[col].apply(lambda x: x.replace(',', ''))
        df[col] = df[col].replace(['nan', 'NA'], 0)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].astype(float)
    for col in date_col:
        if col not in df:
            continue
        df[col] = df[col].replace(['1/0/1900', '1/1/1970'], '0')
        if fill_empty:
            df[col] = df[col].fillna(dt.datetime.today())
        else:
            df[col] = df[col].fillna(pd.Timestamp('nat'))
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: string_to_date(x))
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()
    for col in str_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
        df[col] = df[col].str.strip()
        df[col] = df[col].apply(lambda x: ' '.join(x.split()))
    for col in int_col:
        if col not in df:
            continue
        df[col] = df[col].astype(int)
    return df


def first_last_adj(df, first_row, last_row):
    logging.debug('Removing First & Last Rows')
    first_row = int(first_row)
    last_row = int(last_row)
    if first_row > 0:
        df.columns = df.loc[first_row - 1]
        df = df.iloc[first_row:]
    if last_row > 0:
        df = df[:-last_row]
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
    logging.debug('Dropping unnecessary columns')
    if 'ALL' in removal_cols:
        plan_cols = [x + vmc.planned_suffix for x in vmc.datafloatcol]
        removal_cols = [x for x in df.columns
                        if x not in dctc.COLS + vmc.datacol + vmc.ad_rep_cols +
                        removal_cols + plan_cols]
    for col in removal_cols:
        if col not in df:
            if col == 'nan':
                continue
            if warn:
                logging.warning('{} is not or is no longer in {}.  '
                                'It was not removed.'.format(col, key))
            continue
        df = df.drop(col, axis=1)
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
        df = dummy_df.append(df).reset_index(drop=True)
    elif location == 'foot':
        df = df.append(dummy_df).reset_index(drop=True)
    return df


def give_df_default_format(df, columns=None):
    if not columns:
        columns = df.columns
    for col in columns:
        if 'Cost' in col or col[:2] == 'CP':
            format_map = '${:,.2f}'.format
        else:
            format_map = '{:,.0f}'.format
        df[col] = df[col].map(format_map)
    return df


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


class SeleniumWrapper(object):
    def __init__(self, mobile=False, headless=True):
        self.mobile = mobile
        self.headless = headless
        self.browser, self.co = self.init_browser(self.headless)
        self.base_window = self.browser.window_handles[0]

    def init_browser(self, headless):
        download_path = os.path.join(os.getcwd(), 'tmp')
        co = wd.chrome.options.Options()
        if headless:
            co.headless = True
        co.add_argument('--disable-features=VizDisplayCompositor')
        co.add_argument('--window-size=1920,1080')
        co.add_argument('--start-maximized')
        co.add_argument('--no-sandbox')
        co.add_argument('--disable-gpu')
        prefs = {'download.default_directory': download_path}
        co.add_experimental_option('prefs', prefs)
        if self.mobile:
            mobile_emulation = {"deviceName": "iPhone X"}
            co.add_experimental_option("mobileEmulation", mobile_emulation)
        browser = wd.Chrome(options=co)
        browser.maximize_window()
        browser.set_script_timeout(10)
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

    def go_to_url(self, url, sleep=5):
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
                    logging.warning('More than ten attempts returning.')
                    return False
        time.sleep(sleep)
        return True

    @staticmethod
    def click_on_elem(elem, sleep=2):
        elem.click()
        time.sleep(sleep)

    def click_on_xpath(self, xpath, sleep=2):
        self.click_on_elem(self.browser.find_element_by_xpath(xpath), sleep)

    def quit(self):
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

    def take_screenshot(self, url=None, file_name=None):
        logging.info('Getting screenshot from {} and '
                     'saving to {}.'.format(url, file_name))
        self.go_to_url(url)
        self.browser.save_screenshot(file_name)

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

    def send_keys_from_list(self, elem_input_list, get_xpath_from_id=True):
        for item in elem_input_list:
            elem_xpath = item[1]
            if get_xpath_from_id:
                elem_xpath = self.get_xpath_from_id(elem_xpath)
            elem = self.browser.find_element_by_xpath(elem_xpath)
            elem.send_keys(item[0])
            if 'selectized' in elem_xpath:
                elem.send_keys(u'\ue007')

    def xpath_from_id_and_click(self, elem_id, sleep=2):
        self.click_on_xpath(self.get_xpath_from_id(elem_id), sleep)

    @staticmethod
    def get_xpath_from_id(elem_id):
        return '//*[@id="{}"]'.format(elem_id)
