import os
import io
import re
import csv
import gzip
import json
import time
import codecs
import shutil
import signal
import random
import base64
import select
import zipfile
import logging
import subprocess
import requests
import pandas as pd
import numpy as np
import datetime as dt
import urllib3.exceptions as url_ex
from dataclasses import dataclass
from urllib.parse import unquote, urlparse
from PIL import Image
import selenium.webdriver as wd
import reporting.vmcolumns as vmc
import reporting.dictcolumns as dctc
import reporting.expcolumns as exc
from subprocess import check_output
import http.client as http_client
import selenium.common.exceptions as ex
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.remote_connection import RemoteConnection



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


def rewind(filename):
    """Seeks an in-memory upload back to its start so it can be read again.

    :param filename: a path on disk, left alone, or a file object
    :returns: the same filename
    """
    if hasattr(filename, 'seek'):
        filename.seek(0)
    return filename


def read_head_bytes(filename, size):
    """Reads the first bytes of a path on disk or of a file object.

    :param filename: a path on disk or a file object
    :param size: how many bytes to read
    :returns: the leading bytes of the file
    """
    if hasattr(filename, 'read'):
        head = rewind(filename).read(size)
        rewind(filename)
        return head if isinstance(head, bytes) else head.encode('utf-8')
    with open(filename, 'rb') as f:
        return f.read(size)


def get_file_encoding(filename):
    """Determines a file's encoding from its byte order mark, if any.

    :param filename: a path on disk or a file object to check
    :returns: the name of the encoding the file was written with
    """
    boms = [(codecs.BOM_UTF8, 'utf-8-sig'), (codecs.BOM_UTF32_LE, 'utf-32'),
            (codecs.BOM_UTF32_BE, 'utf-32'), (codecs.BOM_UTF16_LE, 'utf-16'),
            (codecs.BOM_UTF16_BE, 'utf-16')]
    bom = read_head_bytes(filename, 4)
    encoding = [x[1] for x in boms if bom.startswith(x[0])]
    return encoding[0] if encoding else 'iso-8859-1'


def read_ragged_csv(filename, kwargs):
    """Reads a csv with rows holding more fields than its first row.

    Sniffs the delimiter and row widths from the head of the file.  When the
    first row is narrower than the rest, as with a title above the header, a
    column is named for each field so no rows are lost.  Rows wider than any
    in the sample are skipped.

    :param filename: a path on disk or a file object
    :param kwargs: the keyword arguments of the read attempt that failed
    :returns: a dataframe of the file
    """
    encoding = kwargs.get('encoding', 'utf-8')
    sample = read_head_bytes(filename, 1024 * 1024)
    sample = sample.decode(encoding, errors='replace')
    try:
        delimiter = csv.Sniffer().sniff(sample, delimiters=',\t;|').delimiter
    except csv.Error:
        delimiter = ','
    rows = csv.reader(sample.splitlines(), delimiter=delimiter)
    widths = [len(x) for x in rows] or [1]
    common_width = max(set(widths), key=lambda x: (widths.count(x), x))
    kwargs = dict(kwargs, sep=delimiter, on_bad_lines='skip')
    if widths[0] < common_width:
        names = [str(x) for x in range(max(widths))]
        kwargs = dict(kwargs, names=names, header=None, skiprows=1)
    return pd.read_csv(rewind(filename), **kwargs)


def read_csv_fallback(read_func, filename, kwargs):
    """Retries a read so an unparsable file does not stop the run.

    :param read_func: the pandas function used by the failed read attempt
    :param filename: a path on disk or a file object
    :param kwargs: the keyword arguments of the read attempt that failed
    :returns: a dataframe of the file, empty when it could not be read
    """
    try:
        try:
            return read_func(rewind(filename), **kwargs)
        except pd.errors.ParserError as e:
            msg = 'Ragged rows in {}, widening columns. {}'
            logging.warning(msg.format(filename, e))
            return read_ragged_csv(filename, kwargs)
    except Exception as e:
        msg = 'Could not read {}.  Continuing. {}'
        logging.warning(msg.format(filename, e))
        return pd.DataFrame()


def reset_implicit_index(df):
    """Returns leading columns pandas moved into the index to the data.

    A csv whose first row is narrower than the rows below it, as with a
    report title above the header, reads with its leading columns as an
    unnamed index; those values belong in the rows for the header search.

    :param df: the dataframe returned by read_csv
    :returns: the dataframe with a fresh positional index
    """
    if isinstance(df, pd.DataFrame) and not isinstance(df.index,
                                                       pd.RangeIndex):
        df = df.reset_index()
    return df


def import_read_csv(filename, path=None, file_check=True, error_bad='error',
                    empty_df=False, nrows=None, file_type=None):
    """Reads a csv or xlsx from disk or from an uploaded file object.

    :param filename: a path, optionally with ``:::sheet`` suffixes, or a
        file object such as an upload
    :returns: a dataframe, None or empty when the file has no data
    """
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
        if file_type == '.xlsx':
            kwargs['sheet_name'] = sheet_names
        else:
            logging.info(f'Ignoring sheet_name for non-excel file {filename}')
    if file_type == '.xlsx':
        read_func = pd.read_excel
    else:
        read_func = pd.read_csv
        kwargs['encoding'] = 'utf-8'
        kwargs['on_bad_lines'] = error_bad
        kwargs['low_memory'] = True
    try:
        df = read_func(filename, **kwargs)
    except UnicodeDecodeError:
        if 'encoding' in kwargs:
            kwargs['encoding'] = get_file_encoding(filename)
        df = read_csv_fallback(read_func, filename, kwargs)
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
        df = read_csv_fallback(pd.read_csv, filename, kwargs)
    if sheet_names and isinstance(df, dict):
        df = pd.concat(df, ignore_index=True, sort=True)
    if read_func is pd.read_csv:
        df = reset_implicit_index(df)
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
        df[col] = df[col].astype('int64')
    return df


def first_last_adj(df, first_row, last_row):
    """
    Modifies dataframe based on the first and last rows provided. If first_row
    is greater than zero, sets the dataframe's columns to the row above
    first_row and removes any rows above first_row in the df. Logs a warning
    if the df columns are null. If last_row is greater than zero, removes
    last_row number of rows from the end of the df.  Rows are positions, so
    the frame's index labels do not matter; the caller's frame is left as is.

    :param df:Dataframe to adjust
    :param first_row:Position of first row of data in df
    :param last_row:Number of rows to drop from the end of df
    :returns:Adjusted dataframe
    """
    logging.debug('Removing First & Last Rows')
    if df.empty:
        logging.warning('Empty df did not adjust first and last rows')
        return df
    first_row = int(first_row)
    last_row = int(last_row)
    if first_row > 0:
        header = df.iloc[first_row - 1]
        df = df.iloc[first_row:].set_axis(header.values, axis=1)
    if 0 < abs(last_row) < len(df):
        df = df.iloc[:-abs(last_row)]
    if pd.isnull(df.columns.values).any():
        logging.warning('At least one column name is undefined.  Your first'
                        'row is likely incorrect. For reference the first few'
                        'rows are:\n' + str(df.head()))
    return df


def date_removal(df, date_col_name, start_date, end_date):
    df = data_to_type(df, date_col=date_col_name)
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


def remove_date_suffix(name):
    """
    Strips any trailing _YYYY-MM-DD stamps from a name.

    :param name: a string of the name to strip the date stamps from
    :returns: a string of the name without any trailing date stamps
    """
    return re.sub(r'(_\d{4}-\d{2}-\d{2})+$', '', name)


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


def date_blocks(sd, ed, max_days):
    """
    Splits a range into consecutive blocks of at most max_days days, for
    a report that answers one block at a time and whose rows are each a
    single day's figure, so the blocks stack without double counting.

    :param sd: Start of the range, a date or datetime
    :param ed: End of the range, the same type as sd
    :param max_days: Longest block, in days, the report answers at once
    :return: List of (start, end) tuples covering sd to ed in order
    """
    blocks = []
    for offset in range(0, (ed - sd).days + 1, max_days):
        block_sd = sd + dt.timedelta(days=offset)
        block_ed = min(block_sd + dt.timedelta(days=max_days - 1), ed)
        blocks.append((block_sd, block_ed))
    return blocks


def parse_campaign_filter(campaign_filter):
    """
    Splits a campaign filter into ids to send to an api and values to match
    against the report once it downloads.

    An api that filters campaigns server side only accepts ids, so an all
    numeric filter can be sent.  A mix is never sent, since an id filter and
    a name filter intersect to nothing.  Every value is returned for the
    post download match either way, because a numeric value is as likely to
    be an id the campaign is named for as it is to be the campaign's own id.

    :param campaign_filter: The raw comma separated filter
    :return: A tuple of the ids for the api and the values to match
    """
    values = [x.strip() for x in str(campaign_filter or '').split(',')]
    values = [x for x in values if x and x.lower() != 'nan']
    ids = values if values and all(x.isdigit() for x in values) else []
    return ids, values


def filter_df_on_campaign(df, values, name_col, id_col='',
                          keep_on_no_match=True, keep_cols=None):
    """
    Narrows a report to the campaigns a filter names.

    Matching is exact on the id column and a literal (non regex) substring
    on the name column, so a value only has to be right about the campaign
    and not about which of the two it is.  When the filter matches nothing
    the unfiltered df is kept, so a stale or mistyped value surfaces as a
    warning rather than as empty data.  A row filled in any of keep_cols
    is measured above the campaign, so no campaign names it: it is kept
    whatever the filter says and left out of the match stats.

    :param df: The downloaded report
    :param values: The filter values to match
    :param name_col: The name of the campaign name column
    :param id_col: The name of the campaign id column, when the report has one
    :param keep_on_no_match: Whether a filter that matches nothing keeps the
        unfiltered df.  False where the caller already knows the filter names
        a campaign in the report, so a non match means the campaign did not
        deliver rather than that the filter is wrong
    :param keep_cols: Columns of the rows the filter never drops, such as
        an account's daily reach
    :return: The filtered dataframe
    """
    if not values or df.empty:
        return df
    cols = [x for x in [name_col, id_col] if x and x in df.columns]
    if not cols:
        logging.warning(
            'Neither {} nor {} in report, not filtering on the campaign '
            'filter.  The report type may not support the campaign '
            'grouping.'.format(name_col, id_col))
        return df
    keep = pd.Series(False, index=df.index)
    for col in [x for x in (keep_cols or []) if x in df.columns]:
        keep |= df[col].notna()
    mask = pd.Series(False, index=df.index)
    if name_col in cols:
        names = df[name_col].astype('U')
        for value in values:
            mask |= names.str.contains(value, regex=False)
    if id_col in cols:
        ids = df[id_col].astype('U').str.strip().str.replace(
            r'\.0$', '', regex=True)
        mask |= ids.isin(values)
    mask &= ~keep
    tdf = df[mask]
    record_campaign_filter_stats(values, tdf, df[~keep], cols[0],
                                 keep_on_no_match)
    if tdf.empty:
        logging.warning(
            'Campaign filter {} did not match any of the {} campaigns '
            'pulled, returning {} data.  Campaigns: {}'.format(
                values, df[cols[0]].nunique(),
                'unfiltered' if keep_on_no_match else 'empty',
                sorted(df[cols[0]].dropna().unique().tolist())))
        return df if keep_on_no_match else df[keep].reset_index(drop=True)
    logging.info('Filtered to {} of {} rows on campaign filter.'.format(
        len(tdf), len(df)))
    return df[mask | keep].reset_index(drop=True)


campaign_filter_stats_file = 'campaign_filter_stats.json'
_STATS_SAMPLE_CAP = 25
_pending_filter_stat = {}


def record_campaign_filter_stats(values, tdf, df, campaign_col,
                                 keep_on_no_match):
    """Hold the match record for a just-filtered report."""
    try:
        campaigns = df[campaign_col].dropna().astype('U').unique().tolist()
        _pending_filter_stat.clear()
        _pending_filter_stat.update({
            'filter_values': list(values),
            'matched': int(tdf[campaign_col].nunique()),
            'total': len(campaigns),
            'kept_all': bool(tdf.empty and keep_on_no_match),
            'sample_campaigns': sorted(campaigns)[:_STATS_SAMPLE_CAP],
        })
    except Exception as e:
        logging.warning('Could not record campaign filter stats: '
                        '{}'.format(e))


def drain_campaign_filter_stats():
    """Return and clear the pending record -- the import handler calls
    this per vendor key so a record cannot bleed across cards."""
    stat = dict(_pending_filter_stat)
    _pending_filter_stat.clear()
    return stat


def write_campaign_filter_stats(stats_by_vk, config_path_dir=None):
    """Merge per-vendor-key match records into the config-dir JSON
    the app reads, so an earlier partial import's records survive."""
    if not stats_by_vk:
        return
    file_dir = config_path_dir or config_path
    file_name = os.path.join(file_dir, campaign_filter_stats_file)
    existing = {}
    if os.path.exists(file_name):
        try:
            with open(file_name, 'r') as f:
                existing = json.load(f)
        except (IOError, ValueError):
            existing = {}
    if not isinstance(existing, dict):
        existing = {}
    existing.update(stats_by_vk)
    dir_check(file_dir)
    try:
        with open(file_name, 'w') as f:
            json.dump(existing, f)
    except IOError as e:
        logging.warning('Could not write campaign filter stats: '
                        '{}'.format(e))


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


class ClickFailedException(Exception):
    """An element could not be clicked in the allotted attempts."""


def poll_until_true(func, func_kwargs=None, attempts=20, sleep=.1,
                    raise_on_fail=True,
                    exception_msg='Polling timed out before true.',
                    click_elem_id='', load_elem_id='', sw=None):
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
    :param click_elem_id: The original element id that was clicked; re-clicked
    past the halfway point in case the first click no-opped
    :param load_elem_id: The element that loads after click
    :param sw: SeleniumWrapper instance, required for the re-click
    :return: Whether polling succeeded in getting a true value
    """
    return_val = False
    for x in range(attempts):
        if not func_kwargs:
            func_kwargs = {}
        return_val = func(**func_kwargs)
        if return_val:
            break
        if sw and click_elem_id and x > (attempts / 2):
            sw.xpath_from_id_and_click(
                click_elem_id, load_elem_id=load_elem_id)
        time.sleep(sleep)
    if not return_val and raise_on_fail:
        raise Exception(exception_msg)
    return return_val


@dataclass(frozen=True)
class CaptureOptions:
    """How a ``SeleniumWrapper`` loads and shoots a page; the defaults
    are the browser every caller already gets."""
    name: str = 'baseline'
    page_load_timeout: int = 10
    shoot_partial: bool = False
    paint_wait: float = 0
    challenge_wait: float = 0
    extra_args: tuple = ()
    drop_args: tuple = ()
    profile_dir: str = ''
    headed: bool = False
    native_identity: bool = False
    fresh_cookies: bool = False


class VirtualDisplay(object):
    """An Xvfb screen for a windowed browser, handed to the driver
    through its own environment only."""
    command = ('Xvfb', '-displayfd', '1', '-screen', '0', '1920x1080x24',
               '-nolisten', 'tcp')
    start_timeout = 5
    stop_timeout = 5

    def __init__(self):
        self.process = None
        self.name = ''

    @classmethod
    def available(cls):
        """Whether this box can start one."""
        return os.name == 'posix' and bool(shutil.which(cls.command[0]))

    def start(self):
        """Start the server and return its display as ``:<n>``."""
        self.process = subprocess.Popen(list(self.command),
                                        stdout=subprocess.PIPE)
        ready = select.select([self.process.stdout], [], [],
                              self.start_timeout)[0]
        number = self.process.stdout.readline().strip() if ready else b''
        if not number.isdigit():
            self.stop()
            raise OSError('Xvfb did not name a display.')
        self.name = f':{number.decode()}'
        return self.name

    def stop(self):
        """Stop the server; one that already died is left alone."""
        process, self.process, self.name = self.process, None, ''
        if not process:
            return
        try:
            process.terminate()
            process.wait(timeout=self.stop_timeout)
            process.stdout.close()
        except (OSError, ValueError, subprocess.TimeoutExpired) as e:
            logging.warning(f'Error stopping the virtual display: {e}')


class SeleniumWrapper(object):
    driver_path = 'drivers'
    selectize_xpath = 'selectized'
    liquid_xpath = 'liquid'
    command_timeout = 60
    launch_errors = (url_ex.HTTPError, http_client.HTTPException)
    launch_attempts = 3
    launch_pause = 15
    browser_errors = (ex.WebDriverException, url_ex.HTTPError,
                      http_client.HTTPException)
    accept_exact = ['ok', 'continue', 'proceed', 'i agree', 'agree',
                    'accept', 'accept all', 'allow all', 'i accept',
                    'got it', 'consent', 'accetto', 'accetta',
                    'zustimmen', 'alle akzeptieren', "j'accepte",
                    'tout accepter']
    accept_contains = ['accept cookies', 'accept all cookies',
                       'akzeptieren und weiter', 'accept & continue']
    consent_selectors = (
        '#onetrust-accept-btn-handler', '#didomi-notice-agree-button',
        '.qc-cmp2-summary-buttons button[mode=primary]',
        '.fc-cta-consent', 'button[title="Accept all"]',
        '#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll',
        '.sp_choice_type_11', '.jad_cmp_paywall_button-cookies')
    consent_frame_marks = ('sp_message_iframe', 'consent', 'cmp', 'privacy')
    cookie_wait = 2
    cookie_timeout = 15
    verdict_markers = {
        'bot_check': ('performing security verification', 'just a moment',
                      'verify you are human', 'checking your browser',
                      'enable javascript and cookies to continue'),
        'blocked': ('sorry, you have been blocked',
                    'blocked by network security', 'access denied',
                    'request blocked'),
        'ssl_error': ('your connection is not private', 'net::err_cert'),
        'error_page': ('something went wrong', "this page isn't working",
                       "this site can't be reached", '502 bad gateway',
                       '503 service unavailable', 'net::err_')}
    login_markers = ('log in', 'sign in', 'login')
    login_paths = ('/login', '/signin', '/sign-in', '/account/login',
                   '/users/sign_in')
    login_body_max = 400
    mobile_emulation = {
        'deviceMetrics': {'width': 375, 'height': 812, 'pixelRatio': 3.0,
                          'mobile': True, 'touch': True},
        'userAgent': (
            'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) '
            'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 '
            'Mobile/15E148 Safari/604.1')}
    login_dialog_words = ('log in', 'sign in', 'sign up')
    error_body_max = 600
    consent_words = ('cookie', 'consent', 'privacy', 'agree', 'partners',
                     'datenschutz', 'choix', 'consentement',
                     'einwilligung', 'zustimmung', 'consenso',
                     'share or sell', 'do not sell')
    consent_cover_min = 0.2
    blank_span = 6
    blank_ratio = 0.995
    retry_settles = {'blank': (3, 8), 'bot_check': (8,),
                     'consent_wall': (1,), 'login_wall': (1,)}
    ready_wait = 8
    verdict_budget = 20
    capture = CaptureOptions()
    display = None
    last_nav_error = ''
    browser_profile = ''
    slot_settle = 0.2
    nav_started = 0
    partial_load = False
    partial_tag = 'partial load'
    partial_kinds = ('ok', 'consent_wall')
    refused_kinds = ('bot_check', 'blocked', 'ssl_error', 'error_page')
    challenge_poll = 2
    paint_poll = 0.5
    profile_arg = '--user-data-dir='
    profile_cache_arg = '--disk-cache-size=52428800'
    profile_locked_mark = 'user data directory is already in use'
    window_size = (1920, 1080)
    dialog_selector = (
        '[role=dialog],[aria-modal=true],#onetrust-banner-sdk,'
        '.qc-cmp2-container,#didomi-host,.fc-consent-root')
    close_selector = ('[aria-label*=close i], [data-e2e*=close],'
                      ' button[class*=close]')
    dismiss_exact = ('skip', 'not now', 'no thanks', 'maybe later',
                     'close')
    page_state_script = (
        "const area = e => { const b = e.getBoundingClientRect();"
        "  return b.width * b.height; };"
        "const shown = e => e.getBoundingClientRect().height > 0;"
        "const box = [...document.querySelectorAll("
        f"'{dialog_selector}')]"
        "  .filter(shown)"
        "  .sort((a, b) => area(b) - area(a))[0];"
        "const text = document.body ? document.body.innerText : '';"
        "const nav = performance.getEntriesByType('navigation')[0] || {};"
        "const r = box ? box.getBoundingClientRect() : null;"
        "const view = window.innerWidth * window.innerHeight;"
        "const seen = r ? Math.max(0, Math.min(r.right, window.innerWidth)"
        "    - Math.max(r.left, 0))"
        "  * Math.max(0, Math.min(r.bottom, window.innerHeight)"
        "    - Math.max(r.top, 0)) : 0;"
        "const buttons = box ? [...box.querySelectorAll("
        "  'button, a, [role=button], input[type=button],"
        " input[type=submit]')].filter(shown)"
        "  .map(e => (e.innerText || e.value || '').trim().slice(0, 40))"
        "  .filter(t => t).slice(0, 8) : [];"
        "return {title: document.title || '', url: location.href,"
        "  proto: location.protocol, state: document.readyState,"
        "  text: text.slice(0, 2000), len: text.length,"
        "  dialog: box ? box.innerText.slice(0, 300) : '',"
        "  modal: box ? box.getAttribute('aria-modal') === 'true' : false,"
        "  status: nav.responseStatus || 0,"
        "  origin: performance.timeOrigin || 0,"
        "  nodes: document.getElementsByTagName('*').length,"
        "  cover: view ? Math.round(Math.min(seen / view, 1) * 1000)"
        "    / 1000 : 0,"
        "  buttons: buttons};")
    elem_box_script = (
        "arguments[0].scrollIntoView({block: 'center',"
        "  behavior: 'instant'});"
        "const r = arguments[0].getBoundingClientRect();"
        "return {x: r.left + window.scrollX, y: r.top + window.scrollY,"
        "  width: r.width, height: r.height};")
    scroll_top_script = (
        "window.scrollTo({top: 0, left: 0, behavior: 'instant'});")
    scroll_part_script = (
        "window.scrollTo({top: document.body.scrollHeight * arguments[0],"
        " left: 0, behavior: 'instant'});")
    paint_probe_script = (
        "const v = window.__lqVitals || {};"
        "return {lcp: v.lcp || 0, state: document.readyState,"
        "  len: document.body ? document.body.innerText.length : 0};")
    stealth_script = (
        "Object.defineProperty(navigator, 'webdriver',"
        " {get: () => undefined});"
        "window.navigator.chrome = {runtime: {}};"
        "Object.defineProperty(navigator, 'plugins',"
        " {get: () => [1, 2, 3, 4, 5]});"
        "Object.defineProperty(navigator, 'languages',"
        " {get: () => ['en-US', 'en']});")
    webdriver_guard_script = (
        "if (navigator.webdriver) {"
        "  Object.defineProperty(Navigator.prototype, 'webdriver',"
        "    {get: () => false, configurable: true});"
        "}")
    client_hints_script = (
        "const done = arguments[arguments.length - 1];"
        "const data = navigator.userAgentData;"
        "if (!data) { done(null); return; }"
        "data.getHighEntropyValues(['architecture', 'bitness', 'model',"
        "  'platformVersion', 'fullVersionList', 'wow64'])"
        "  .then(done).catch(() => done(null));")
    client_hints_page = 'chrome://version/'
    client_hint_fields = ('brands', 'fullVersionList', 'platform',
                          'platformVersion', 'architecture', 'bitness',
                          'wow64', 'model', 'mobile')
    headless_mark = 'HeadlessChrome'
    ad_vendors = (
        ('doubleclick.net', 'Google'), ('googlesyndication.com', 'Google'),
        ('googleadservices.com', 'Google'), ('2mdn.net', 'Google'),
        ('googletagservices.com', 'Google'), ('adnxs.com', 'Xandr'),
        ('amazon-adsystem.com', 'Amazon DSP'),
        ('rubiconproject.com', 'Magnite'), ('criteo.com', 'Criteo'),
        ('criteo.net', 'Criteo'), ('openx.net', 'OpenX'),
        ('pubmatic.com', 'PubMatic'), ('taboola.com', 'Taboola'),
        ('outbrain.com', 'Outbrain'), ('teads.tv', 'Teads'),
        ('indexww.com', 'Index Exchange'),
        ('casalemedia.com', 'Index Exchange'),
        ('smartadserver.com', 'Equativ'), ('adsrvr.org', 'The Trade Desk'),
        ('yieldmo.com', 'Yieldmo'), ('sharethrough.com', 'Sharethrough'),
        ('adform.net', 'Adform'), ('3lift.com', 'TripleLift'),
        ('mgid.com', 'MGID'), ('revcontent.com', 'Revcontent'),
        ('raptive.com', 'Raptive'), ('adthrive.com', 'Raptive'),
        ('mediavine.com', 'Mediavine'), ('freestar.io', 'Freestar'),
        ('playwire.com', 'Playwire'), ('connatix.com', 'Connatix'),
        ('primis.tech', 'Primis'), ('flashtalking.com', 'Flashtalking'),
        ('innovid.com', 'Innovid'), ('serving-sys.com', 'Sizmek'),
        ('sizmek.com', 'Sizmek'), ('celtra.com', 'Celtra'),
        ('nativo.net', 'Nativo'), ('media.net', 'Media.net'),
        ('lijit.com', 'Sovrn'), ('sovrn.com', 'Sovrn'),
        ('zemanta.com', 'Zemanta'))
    ad_hosts = tuple(host for host, _ in ad_vendors)
    site_vendor = 'Site direct'
    ad_markers = ('google_ads_iframe', 'div-gpt-ad', 'adsbygoogle',
                  'google-query-id')
    native_markers = ('taboola', 'outbrain', 'mgid', 'revcontent',
                      'sharethrough', 'nativo', 'zergnet')
    container_markers = ('ad-slot', 'ad_slot', 'adunit', 'ad-unit',
                         'advert', 'data-ad', 'sponsored')
    container_id_pattern = re.compile(r'(^|[-_])ad([-_]|$)')
    container_max = (1200, 1400)
    iab_sizes = ((300, 250), (728, 90), (300, 600), (160, 600), (320, 50),
                 (970, 250), (970, 90), (336, 280), (300, 50), (300, 1050),
                 (320, 100), (250, 250), (468, 60), (120, 600), (970, 66),
                 (640, 360))
    redirect_params = ('adurl', 'url', 'r', 'dest', 'clickurl', 'redirect',
                       'u')
    slot_mark = 'data-lq-slot'
    max_ad_slots = 20
    scan_budget = 60
    min_slot_size = (100, 50)
    full_shot_max_height = 4000
    full_shot_quality = 80
    full_shot_kinds = ('ok', 'consent_wall')
    deny_paths = ('/tag/', '/tags/', '/video', '/videos', '/login',
                  '/signin', '/subscribe', '/account', '/search', '/author',
                  '/wiki/', '/forum', '/deals')
    article_min_text = 15
    article_long_slug = 20
    candidate_selector = (
        'iframe, ins.adsbygoogle, [id^=div-gpt-ad], [data-google-query-id],'
        ' [id*="taboola"], [id*="outbrain"], .OUTBRAIN, [id*="mgid"],'
        ' [id*="revcontent"], [data-widget-id], [id^="ad-"], [id*="-ad-"],'
        ' [id$="-ad"], [class*="ad-slot"], [class*="ad_slot"],'
        ' [class*="adunit"], [class*="ad-unit"], [class*="advert"],'
        ' [data-ad], [data-ad-unit], [data-ad-slot], [class*="sponsored"],'
        ' video')
    article_links_script = (
        "const sel = 'article a[href], main a[href], h2 a[href],"
        " h3 a[href], [class*=headline] a[href], [class*=card] a[href]';"
        "const seen = new Set();"
        "return [...document.querySelectorAll(sel)].filter(a => {"
        "  if (seen.has(a.href)) return false;"
        "  seen.add(a.href); return true;"
        "}).slice(0, 60).map(a => ({href: a.href,"
        "  text: (a.innerText || a.textContent || '').trim().slice(0, 200)"
        "}));")
    page_meta_script = (
        "const abs = h => { try { return new URL(h, document.baseURI).href; }"
        "  catch (e) { return ''; } };"
        "const tags = {};"
        "document.querySelectorAll('meta[property], meta[name]')"
        "  .forEach(m => {"
        "    const k = (m.getAttribute('property')"
        "      || m.getAttribute('name') || '').toLowerCase();"
        "    const v = (m.getAttribute('content') || '').slice(0, 1000);"
        "    if (k && v) (tags[k] = tags[k] || []).push(v);"
        "  });"
        "const under = p => Object.fromEntries(Object.entries(tags)"
        "  .filter(([k]) => k.startsWith(p))"
        "  .map(([k, v]) => [k.slice(p.length), v[0]]));"
        "const keep = ['@type', 'headline', 'name', 'description', 'image',"
        "  'datePublished', 'dateModified', 'author', 'articleSection',"
        "  'keywords', 'url', 'publisher'];"
        "const ld = [];"
        "document.querySelectorAll('script[type=\"application/ld+json\"]')"
        "  .forEach(s => { try {"
        "    [].concat(JSON.parse(s.textContent)).forEach(n => {"
        "      if (n) ld.push(...[].concat(n['@graph'] || n));"
        "    });"
        "  } catch (e) {} });"
        "const when = document.querySelector('time[datetime]');"
        "const canon = document.querySelector('link[rel=canonical]');"
        "return {"
        "  ld: ld.filter(n => n && typeof n === 'object').slice(0, 20)"
        "    .map(n => Object.fromEntries(keep.filter(k => k in n)"
        "      .map(k => [k, n[k]]))),"
        "  og: under('og:'), twitter: under('twitter:'),"
        "  article: Object.assign(under('article:'),"
        "    {tag: (tags['article:tag'] || []).slice(0, 20)}),"
        "  meta: {title: document.title || '',"
        "    description: (tags['description'] || [''])[0],"
        "    author: (tags['author'] || [''])[0],"
        "    time: when ? when.getAttribute('datetime') : '',"
        "    canonical: canon ? abs(canon.getAttribute('href')) : ''},"
        "  feeds: [...document.querySelectorAll('link[rel=alternate]')]"
        "    .filter(l => /rss|atom/i.test(l.getAttribute('type') || ''))"
        "    .map(l => abs(l.getAttribute('href') || ''))"
        "    .filter(h => h).slice(0, 10)};")
    article_types = ('newsarticle', 'article', 'blogposting', 'review',
                     'videogamereview', 'reportagenewsarticle',
                     'liveblogposting', 'opinionnewsarticle',
                     'analysisnewsarticle', 'techarticle')
    meta_places = {
        'title': (('ld', 'headline'), ('ld', 'name'), ('og', 'title'),
                  ('twitter', 'title'), ('meta', 'title')),
        'description': (('ld', 'description'), ('og', 'description'),
                        ('twitter', 'description'),
                        ('meta', 'description')),
        'image': (('ld', 'image'), ('og', 'image'), ('twitter', 'image')),
        'published': (('ld', 'datePublished'),
                      ('article', 'published_time'), ('meta', 'time')),
        'modified': (('ld', 'dateModified'),
                     ('article', 'modified_time')),
        'author': (('ld', 'author'), ('meta', 'author')),
        'section': (('ld', 'articleSection'), ('article', 'section')),
        'tags': (('ld', 'keywords'), ('article', 'tag')),
        'canonical': (('ld', 'url'), ('og', 'url'), ('meta', 'canonical')),
        'site_name': (('ld', 'publisher'), ('og', 'site_name'))}
    meta_caps = {'title': 300, 'description': 500, 'image': 500,
                 'published': 40, 'modified': 40, 'author': 120,
                 'section': 80, 'tags': 80, 'canonical': 500,
                 'site_name': 120}
    meta_url_fields = ('image', 'canonical')
    meta_url_keys = ('url', 'contentUrl', '@id')
    meta_joined_fields = ('author',)
    meta_tag_limit = 10
    meta_feed_limit = 3
    frame_read_script = (
        "return {hrefs: [...document.querySelectorAll('a[href]')]"
        "  .slice(0, 20).map(a => a.href),"
        " imgs: [...document.querySelectorAll('img')].slice(0, 10)"
        "  .map(i => ({src: i.src, alt: i.alt || ''})),"
        " labels: [...document.querySelectorAll('[aria-label]')]"
        "  .slice(0, 10).map(e => e.getAttribute('aria-label')),"
        " text: (document.body ? document.body.innerText : '')"
        "  .slice(0, 500)};")
    candidates_script = (
        "const sel = arguments[1];"
        "const min = arguments[2];"
        "return [...document.querySelectorAll(sel)].map((el, n) => {"
        "  const r = el.getBoundingClientRect();"
        "  el.setAttribute(arguments[0], String(n));"
        "  return {n: n, tag: el.tagName.toLowerCase(), id: el.id || '',"
        "    src: el.getAttribute('src') || '',"
        "    width: Math.round(r.width), height: Math.round(r.height),"
        "    top: Math.round(r.top + window.scrollY),"
        "    left: Math.round(r.left + window.scrollX),"
        "    attrs: [el.id, el.className,"
        "            ...[...el.attributes].map(a => a.name)].join(' ')};"
        "}).filter(c => c.width >= min[0] && c.height >= min[1]);")
    vitals_script = (
        "window.__lqVitals = {lcp: 0, cls: 0};"
        "try {"
        "  new PerformanceObserver(list => {"
        "    for (const e of list.getEntries()) {"
        "      window.__lqVitals.lcp = e.renderTime || e.loadTime"
        "        || e.startTime;"
        "    }"
        "  }).observe({type: 'largest-contentful-paint', buffered: true});"
        "  new PerformanceObserver(list => {"
        "    for (const e of list.getEntries()) {"
        "      if (!e.hadRecentInput) window.__lqVitals.cls += e.value;"
        "    }"
        "  }).observe({type: 'layout-shift', buffered: true});"
        "} catch (e) {}")
    vitals_read_script = (
        "const v = window.__lqVitals || {};"
        "const nav = performance.getEntriesByType('navigation')[0];"
        "const res = performance.getEntriesByType('resource');"
        "const host = location.hostname.replace(/^www\\./, '');"
        "const bytes = res.reduce((n, r) => n + (r.transferSize || 0), 0)"
        "  + (nav ? (nav.transferSize || 0) : 0);"
        "const hosts = new Set();"
        "res.forEach(r => { try {"
        "  const h = new URL(r.name).hostname.replace(/^www\\./, '');"
        "  if (h && h !== host && !h.endsWith(`.${host}`)) hosts.add(h);"
        "} catch (e) {} });"
        "return {lab_lcp_ms: v.lcp ? Math.round(v.lcp) : null,"
        "  lab_cls: (typeof v.cls === 'number')"
        "    ? Math.round(v.cls * 1000) / 1000 : null,"
        "  lab_ttfb_ms: nav ? Math.round(nav.responseStart) : null,"
        "  page_kilobytes: Math.round(bytes / 1024),"
        "  third_party_hosts: hosts.size,"
        "  viewport: {w: window.innerWidth, h: window.innerHeight}};")

    def __init__(self, mobile=False, headless=True, page_load_strategy='',
                 capture=None):
        """:param capture: CaptureOptions, the baseline when omitted"""
        self.mobile = mobile
        self.capture = capture or self.capture
        self.headless = self.open_display(headless)
        self.page_load_strategy = page_load_strategy
        try:
            self.browser, self.co = self.init_browser(self.headless)
        except Exception:
            self.release_display()
            raise
        self.base_window = self.browser.window_handles[0]
        self.select_id = By.ID
        self.select_class = By.CLASS_NAME
        self.select_xpath = By.XPATH
        self.select_css = By.CSS_SELECTOR
        self.use_js_click = False
        self.default_elem_sleep = 2
        self.last_full_shot = ('', None)

    def __enter__(self):
        """Support ``with SeleniumWrapper() as sw`` — teardown by
        construction instead of by every caller remembering a
        ``finally``.

        :return: the wrapper itself
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Quit the browser on scope exit, never swallowing the
        body's exception.

        :param exc_type: exception class raised in the body, if any
        :param exc_value: the exception instance
        :param exc_tb: its traceback
        :return: False, so any exception propagates
        """
        self.quit()
        return False

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

    def create_browser(self, co):
        """Chrome via the locally downloaded driver when one exists
        (kept current by the retry in ``init_browser`` whenever Chrome
        auto-updates past the driver), else whatever is on PATH."""
        local_driver = os.path.join(
            self.driver_path, 'chromedriver-win64', 'chromedriver.exe')
        service_kwargs = {}
        if os.path.exists(local_driver):
            service_kwargs['executable_path'] = local_driver
        if self.display:
            service_kwargs['env'] = dict(os.environ,
                                         DISPLAY=self.display.name)
        if service_kwargs:
            service = wd.chrome.service.Service(**service_kwargs)
            return wd.Chrome(service=service, options=co)
        return wd.Chrome(options=co)

    def open_display(self, headless):
        """The headless flag to launch with, starting a virtual display
        when the capture options ask for a window and the box has one."""
        if not self.capture.headed:
            return headless
        if not VirtualDisplay.available():
            logging.warning('No virtual display here for a windowed '
                            'browser, launching headless.')
            return True
        self.display = VirtualDisplay()
        try:
            self.display.start()
        except OSError as e:
            logging.warning(f'Virtual display did not start, launching '
                            f'headless: {e}')
            self.display = None
            return True
        return False

    def release_display(self):
        """Stop the virtual display this browser holds, if any."""
        if self.display:
            self.display.stop()
            self.display = None

    def launch_browser(self, co):
        """Start chrome, retrying with a growing pause when the
        new-session call times out on a loaded box."""
        for attempt in range(1, self.launch_attempts + 1):
            try:
                return self.create_browser(co)
            except self.launch_errors as e:
                if attempt == self.launch_attempts:
                    raise
                logging.warning(
                    f'Browser launch attempt {attempt} of '
                    f'{self.launch_attempts} failed, retrying: {e}')
                time.sleep(self.launch_pause * attempt)

    def launch_args(self, headless, use_profile=True):
        """Chrome's shared launch arguments under the capture options'
        drops, extras and, unless ``use_profile`` is off, profile
        folder."""
        args = ['--headless=new',
                '--window-position=-32000,-32000'] if headless else []
        width, height = self.window_size
        args += ['--lang=en-US', f'--window-size={width},{height}',
                 '--start-maximized', '--no-sandbox', '--disable-gpu',
                 '--disable-blink-features=AutomationControlled']
        args = [x for x in args
                if not x.startswith(self.capture.drop_args)]
        args += self.capture.extra_args
        if use_profile and self.capture.profile_dir:
            args += [f'{self.profile_arg}{self.capture.profile_dir}',
                     self.profile_cache_arg]
        return args

    def chrome_options(self, args, download_path):
        """Chrome options carrying ``args`` and the shared prefs."""
        co = wd.chrome.options.Options()
        if self.page_load_strategy:
            co.page_load_strategy = self.page_load_strategy
        for arg in args:
            co.add_argument(arg)
        prefs = {'download.default_directory': download_path,
                 "credentials_enable_service": False,
                 "profile.password_manager_enabled": False
                 }
        co.add_experimental_option('prefs', prefs)
        co.add_experimental_option('excludeSwitches', ['enable-automation'])
        co.add_experimental_option('useAutomationExtension', False)
        if self.mobile:
            co.add_experimental_option('mobileEmulation',
                                       dict(self.mobile_emulation))
        return co

    def init_browser(self, headless):
        RemoteConnection.set_timeout(self.command_timeout)
        download_path = os.path.join(os.getcwd(), 'tmp')
        co = self.chrome_options(self.launch_args(headless), download_path)
        try:
            browser = self.launch_browser(co)
        except (ex.SessionNotCreatedException, FileNotFoundError) as e:
            logging.warning(e)
            if self.capture.profile_dir and self.profile_locked_mark in str(e):
                logging.warning('Profile folder would not open, launching '
                                'without it.')
                co = self.chrome_options(
                    self.launch_args(headless, use_profile=False),
                    download_path)
            else:
                chrome_version = self.get_chrome_version()
                driver_version = self.get_chromedriver_version(
                    chrome_version)
                self.download_chromedriver(driver_version)
            browser = self.launch_browser(co)
        try:
            self.configure_browser(browser, headless, download_path)
        except Exception:
            try:
                browser.quit()
            except Exception as quit_error:
                logging.warning(
                    'Error quitting mid-init: {}'.format(quit_error))
            raise
        return browser, co

    def configure_browser(self, browser, headless, download_path):
        """Post-spawn setup: stealth shims, user agent, window size,
        timeouts and the headless download directory.

        Runs under :func:`init_browser`'s guard: every statement here
        talks to a chrome that already exists, so a raise would
        otherwise strand that process where nothing can quit it.

        :param browser: the freshly created driver
        :param headless: whether the browser was launched headless
        :param download_path: directory downloads land in
        :return: None
        """
        browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                                {'source': self.vitals_script})
        if self.capture.native_identity:
            self.set_native_identity(browser)
        else:
            self.set_patched_identity(browser)
        if self.capture.profile_dir:
            browser.execute_cdp_cmd('Network.clearBrowserCache', {})
        if headless or self.capture.headed:
            browser.set_window_size(*self.window_size)
        else:
            browser.maximize_window()
        browser.set_script_timeout(10)
        browser.set_page_load_timeout(min(self.capture.page_load_timeout,
                                          self.command_timeout - 15))
        self.enable_download_in_headless_chrome(browser, download_path)
        self.browser_profile = self.profile_of(browser)

    def headful_agent(self, browser):
        """A headless desktop browser's agent with the headless mark
        off; '' for a phone or an agent that has none to lose."""
        agent = browser.execute_script('return navigator.userAgent;') or ''
        if self.mobile or self.headless_mark not in agent:
            return ''
        return agent.replace(self.headless_mark, 'Chrome')

    def set_patched_identity(self, browser):
        """Script patches over ``navigator`` and the headless mark off
        a desktop agent: the identity every caller has had."""
        browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                                {'source': self.stealth_script})
        browser.execute_script(self.stealth_script)
        agent = self.headful_agent(browser)
        if agent:
            browser.execute_cdp_cmd('Network.setUserAgentOverride', {
                'userAgent': agent, 'acceptLanguage': 'en-US,en;q=0.9'})

    def read_client_hints(self, browser):
        """Chrome's own client hints over ``client_hint_fields``, read
        off a secure Chrome page; {} when it names none."""
        try:
            browser.get(self.client_hints_page)
            hints = browser.execute_async_script(self.client_hints_script)
        except self.browser_errors as e:
            logging.warning(f'Client hints not read: {e}')
            return {}
        hints = hints if isinstance(hints, dict) else {}
        return {k: hints[k] for k in self.client_hint_fields if k in hints}

    def set_native_identity(self, browser):
        """Leave the browser saying what Chrome says of itself: only a
        headless desktop agent loses its mark, keeping its client
        hints, and the language is left to Chrome's own header order.
        """
        browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                                {'source': self.webdriver_guard_script})
        agent = self.headful_agent(browser)
        if not agent:
            return
        override = {'userAgent': agent}
        hints = self.read_client_hints(browser)
        if hints:
            override['userAgentMetadata'] = hints
        browser.execute_cdp_cmd('Emulation.setUserAgentOverride', override)

    def profile_of(self, browser):
        """The profile folder of the Chrome behind ``browser``, which
        finds that Chrome again once its driver dies."""
        try:
            held = (browser.capabilities.get('chrome') or {}).get(
                'userDataDir')
        except (AttributeError, TypeError) as e:
            logging.warning(f'Browser profile not read: {e}')
            return ''
        return str(held or self.capture.profile_dir or '')

    @classmethod
    def chrome_commands(cls):
        """``[(pid, command line)]`` of the Chrome processes on this
        box, [] when they cannot be listed."""
        if os.name == 'nt':
            return cls.windows_chrome_commands()
        found = []
        for pid in [x for x in os.listdir('/proc') if x.isdigit()]:
            try:
                with open(f'/proc/{pid}/cmdline', 'rb') as f:
                    line = f.read().replace(b'\x00', b' ').decode(
                        'utf-8', 'replace')
            except OSError:
                continue
            if 'chrom' in line:
                found.append((int(pid), line))
        return found

    @staticmethod
    def windows_chrome_commands():
        """``chrome_commands`` on Windows, which has no ``/proc``."""
        query = ("Get-CimInstance Win32_Process -Filter "
                 "\"Name='chrome.exe'\" | ForEach-Object "
                 "{ \"$($_.ProcessId)|$($_.CommandLine)\" }")
        try:
            listed = subprocess.run(
                ['powershell', '-NoProfile', '-Command', query],
                capture_output=True, text=True, timeout=60).stdout
        except (OSError, subprocess.SubprocessError) as e:
            logging.warning(f'Chrome processes not listed: {e}')
            return []
        rows = [x.split('|', 1) for x in listed.splitlines() if '|' in x]
        return [(int(pid), line) for pid, line in rows if pid.isdigit()]

    @classmethod
    def profile_pids(cls, profile):
        """The pids of the browser processes (not their children)
        running on ``profile``."""
        folded = os.path.normcase(profile)
        return [pid for pid, line in cls.chrome_commands()
                if folded in os.path.normcase(line)
                and '--type=' not in line]

    def reap_browser(self):
        """Kill the Chrome a dead driver left behind and drop its
        throwaway profile; returns how many browsers were killed."""
        profile, self.browser_profile = self.browser_profile, ''
        if not profile:
            return 0
        killed = 0
        for pid in self.profile_pids(profile):
            try:
                os.kill(pid, signal.SIGTERM)
                killed += 1
            except OSError as e:
                logging.warning(f'Browser {pid} was already gone: {e}')
        if killed:
            logging.warning(f'Killed {killed} browser(s) a dead driver '
                            f'left on {profile}.')
        if profile != self.capture.profile_dir:
            shutil.rmtree(profile, ignore_errors=True)
        return killed

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

    def restart_browser(self):
        """Replace a wedged driver with a fresh one.

        A command that times out at the socket leaves the session in an
        unknown state, so the only safe recovery is a new browser.
        ``browser.quit`` stops the driver process even when the session
        is already unreachable, so no close is attempted first.
        """
        logging.warning('Restarting browser.')
        try:
            self.browser.quit()
        except Exception as e:
            logging.warning('Error during browser quit: {}'.format(e))
        self.reap_browser()
        self.release_display()
        self.headless = self.open_display(self.headless)
        self.browser, self.co = self.init_browser(self.headless)
        self.base_window = self.browser.window_handles[0]

    def restart_and_get(self, url):
        """Rebuild the browser and retry ``url`` once.

        :param url: the url to load on the new browser
        :return: whether the reload succeeded
        """
        try:
            self.restart_browser()
            self.browser.get(url)
        except Exception as e:
            logging.error('Restart and reload failed: {}'.format(e))
            return False
        return True

    def go_to_url(self, url, sleep=5, elem_id='', max_attempts=10):
        """Navigate to a url, rebuilding the browser if it stops
        answering.

        :param url: the url to load
        :param sleep: seconds to settle when no elem_id is given
        :param elem_id: element to wait on instead of sleeping
        :param max_attempts: navigation attempts before the browser is
            rebuilt. A socket-level timeout skips straight to the
            rebuild -- the session is already gone, so further attempts
            would each burn another full command_timeout.
        :return: whether the url was reached
        """
        logging.info('Going to url {}.'.format(url))
        self.partial_load = False
        self.last_nav_error = ''
        for x in range(max_attempts):
            self.nav_started = time.time()
            try:
                self.browser.get(url)
                break
            except self.browser_errors as e:
                msg = 'Exception attempt: {}, retrying: \n {}'.format(x + 1, e)
                logging.warning(msg)
                self.last_nav_error = self.describe_nav_error(e)
                if self.keep_partial(e):
                    break
                dead_session = isinstance(e, url_ex.HTTPError)
                if not (dead_session or x >= max_attempts - 1):
                    continue
                if not self.restart_and_get(url):
                    return False
                break
        if elem_id:
            self.wait_for_elem_load(elem_id)
        else:
            time.sleep(sleep)
        return True

    @staticmethod
    def describe_nav_error(error):
        """A navigation failure as its class and Chrome's own error
        code, so a timeout reads apart from a refused connection."""
        code = re.search(r'net::ERR_\w+', str(error))
        name = type(error).__name__
        return f'{name} {code.group(0)}' if code else name

    def keep_partial(self, error):
        """Whether a timed-out navigation left a document of its own
        worth shooting, noted on ``partial_load``."""
        if not (self.capture.shoot_partial
                and isinstance(error, ex.TimeoutException)):
            return False
        try:
            self.browser.execute_script('window.stop();')
            state = self.browser.execute_script(
                self.page_state_script) or {}
        except self.browser_errors as e:
            logging.warning(f'Partial page not read: {e}')
            return False
        born = float(state.get('origin') or 0) / 1000
        self.partial_load = (
            not str(state.get('proto') or '').startswith('chrome-error')
            and not str(state.get('url') or '').startswith(
                ('about:', 'data:'))
            and int(state.get('len') or 0) > 0
            and born >= self.nav_started)
        return self.partial_load

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
                self.browser.execute_script(
                    "var el = arguments[0];"
                    "['mousedown', 'mouseup', 'click'].forEach("
                    "    function (t) { el.dispatchEvent(new MouseEvent("
                    "        t, {bubbles: true, cancelable: true,"
                    "            view: window})); });", elem)
                return True
            except (ex.ElementNotInteractableException,
                    ex.ElementClickInterceptedException,
                    ex.StaleElementReferenceException) as js_error:
                logging.warning('Could not click JS: {}'.format(js_error))
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
        except ex.StaleElementReferenceException as scroll_error:
            logging.warning(scroll_error)
        time.sleep(.1)
        return False

    def click_on_xpath(self, xpath='', sleep=None, elem=None):
        if sleep is None:
            sleep = self.default_elem_sleep
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
            raise ClickFailedException(msg)
        return elem_click

    def quit(self):
        """Tear down the browser, swallowing driver errors.

        Callers run this from a ``finally``, so it must never raise. A
        wedged or already-dead session would otherwise mask the real
        exception and strand the chrome process this is meant to reap,
        so a driver that did not answer has its browser killed.
        """
        answered = True
        try:
            self.browser.close()
        except Exception as e:
            answered = False
            logging.warning('Error closing: {}'.format(e))
        try:
            self.browser.quit()
        except Exception as e:
            answered = False
            logging.warning('Error during browser quit: {}'.format(e))
        if not answered:
            self.reap_browser()
        self.release_display()

    @staticmethod
    def get_file_as_df(temp_path=None):
        """Poll ``temp_path`` for a downloaded csv (500s ceiling,
        matching the old 100 x 5s loop) and read it once its size is
        stable — i.e. the browser finished writing."""
        df = pd.DataFrame()
        deadline = time.time() + 500
        logging.info('Checking for file in {}.'.format(temp_path))
        while time.time() < deadline:
            files = [x for x in os.listdir(temp_path) if x[-4:] == '.csv']
            if not files:
                time.sleep(.25)
                continue
            temp_file = os.path.join(temp_path, files[-1])
            last_size = -1
            while time.time() < deadline:
                size = os.path.getsize(temp_file)
                if size and size == last_size:
                    break
                last_size = size
                time.sleep(.25)
            logging.info('File downloaded.')
            df = import_read_csv(temp_file, empty_df=True)
            os.remove(temp_file)
            break
        shutil.rmtree(temp_path)
        return df

    @classmethod
    def get_accept_xpath(cls):
        """Xpath matching a cookie-consent accept control.

        Scoped to clickable nodes and matched on lowercased text. An
        unscoped ``//*`` walk matches the banner's own body copy as
        readily as its button, so the first hit is regularly a
        paragraph rather than something worth clicking.

        :return: an xpath string
        """
        return cls.control_xpath(cls.accept_exact, cls.accept_contains)

    @classmethod
    def control_xpath(cls, exact, contains=(), root='//'):
        """Xpath of the clickable nodes under ``root`` worded as one of
        ``exact`` or holding one of ``contains``, case and apostrophe
        folded."""
        upper = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ\u2019'
        lower = "abcdefghijklmnopqrstuvwxyz'"
        conds = []
        for prop in ['normalize-space(.)', 'normalize-space(@value)']:
            text = 'translate({},"{}","{}")'.format(prop, upper, lower)
            conds += ['{}="{}"'.format(text, x) for x in exact]
            conds += ['contains({},"{}")'.format(text, x)
                      for x in contains]
        cond = ' or '.join(conds)
        tags = ['button', 'a', 'input', '*[@role="button"]']
        return ' | '.join('{}{}[{}]'.format(root, x, cond) for x in tags)

    def find_accept_buttons(self, btn_xpath):
        """Visible consent controls in the current frame: a consent
        platform's own button by id or class first, else one by text.

        :param btn_xpath: xpath from ``get_accept_xpath``
        :return: list of visible WebElements
        """
        cmp = self.browser.find_elements(By.CSS_SELECTOR,
                                         ', '.join(self.consent_selectors))
        return ([x for x in cmp if self.elem_visible(x)]
                or [x for x in self.browser.find_elements(By.XPATH, btn_xpath)
                    if self.elem_visible(x)])

    def click_accept_buttons(self, btn_xpath, wait=0):
        """Click the first visible consent control in the current frame.

        :param btn_xpath: xpath from ``get_accept_xpath``
        :param wait: seconds to poll for a control before giving up.
            Worth spending on the page itself, where a banner script
            may still be injecting, and not on an ad iframe that will
            never hold one.
        :return: whether a control was clicked
        """
        deadline = time.time() + wait
        buttons = self.find_accept_buttons(btn_xpath)
        while not buttons and time.time() < deadline:
            time.sleep(.25)
            buttons = self.find_accept_buttons(btn_xpath)
        if not buttons:
            return False
        try:
            self.click_on_xpath(sleep=1, elem=buttons[0])
        except (ex.WebDriverException, ClickFailedException) as e:
            logging.warning('Could not accept cookies: {}'.format(e))
            return False
        return True

    def switch_to_frame(self, iframe=None, parent=False):
        """Switch into a frame, up to its parent, or back out to the
        page.

        :param iframe: frame WebElement, or None for default content
        :param parent: step up one frame instead
        :return: whether the switch succeeded
        """
        try:
            if parent:
                self.browser.switch_to.parent_frame()
            elif iframe is None:
                self.browser.switch_to.default_content()
            else:
                self.browser.switch_to.frame(iframe)
        except ex.WebDriverException as e:
            logging.warning('Could not switch frame: {}'.format(e))
            return False
        return True

    def is_consent_frame(self, iframe):
        """Whether a frame's src, id or title marks it as a consent
        platform's."""
        try:
            marks = ' '.join(iframe.get_attribute(a) or ''
                             for a in ('src', 'id', 'title')).lower()
        except ex.StaleElementReferenceException:
            return False
        return any(x in marks for x in self.consent_frame_marks)

    def accept_cookies(self, wait=None):
        """Dismiss a cookie banner on the page or in one of its frames.

        Stops at the first banner accepted, trying consent platform
        frames first: every further frame is a driver round trip.

        :param wait: seconds to poll the page itself, ``cookie_wait``
            by default
        """
        btn_xpath = self.get_accept_xpath()
        wait = self.cookie_wait if wait is None else wait
        if self.click_accept_buttons(btn_xpath, wait=wait):
            return
        deadline = time.time() + self.cookie_timeout
        frames = [x for x in self.browser.find_elements(By.TAG_NAME, 'iframe')
                  if self.elem_visible(x)]
        frames.sort(key=lambda x: not self.is_consent_frame(x))
        for iframe in frames:
            if time.time() > deadline:
                logging.warning('Timed out looking for a cookie banner.')
                return
            if self.is_ad_frame(iframe) and not self.is_consent_frame(iframe):
                continue
            if not self.switch_to_frame(iframe):
                continue
            accepted = self.click_accept_buttons(btn_xpath)
            if not self.switch_to_frame() or accepted:
                return

    def dismiss_consent(self):
        """A longer second try at a consent wall, then Escape."""
        self.accept_cookies(wait=self.cookie_wait * 2)
        try:
            self.browser.find_element(By.TAG_NAME, 'body').send_keys(
                Keys.ESCAPE)
        except ex.WebDriverException as e:
            logging.warning('Could not send Escape: {}'.format(e))

    def find_closers(self, box):
        """The visible close controls in ``box``, marked ones first."""
        marked = box.find_elements(By.CSS_SELECTOR, self.close_selector)
        worded = box.find_elements(
            By.XPATH, self.control_xpath(self.dismiss_exact, root='.//'))
        return [x for x in marked + worded if self.elem_visible(x)]

    def dismiss_dialog(self):
        """Close a sign-up prompt over the page with Escape, then its
        own close control; a page with no dialog is left untouched."""
        try:
            boxes = [x for x in self.browser.find_elements(
                By.CSS_SELECTOR, self.dialog_selector)
                if self.elem_visible(x)]
            if not boxes:
                return
            self.browser.find_element(By.TAG_NAME, 'body').send_keys(
                Keys.ESCAPE)
            closer = next((x for box in boxes
                           for x in self.find_closers(box)), None)
            if closer:
                self.click_on_xpath(sleep=1, elem=closer)
        except self.browser_errors + (ClickFailedException,) as e:
            logging.warning(f'Could not dismiss the dialog: {e}')

    @staticmethod
    def join_detail(*parts):
        """The parts of a verdict's detail that say something, spaced."""
        return ' '.join(str(x) for x in parts if x)

    @classmethod
    def is_solid_png(cls, png_bytes):
        """Whether a shot is one colour give or take ``blank_span``
        grey levels, or unreadable: a page that had not painted."""
        if not png_bytes:
            return True
        try:
            with Image.open(io.BytesIO(png_bytes)) as img:
                grey = img.convert('L')
                grey.thumbnail((128, 128))
                lo, hi = grey.getextrema()
                hist = grey.histogram()
        except (OSError, ValueError):
            return True
        if hi - lo <= cls.blank_span:
            return True
        mode = max(range(len(hist)), key=hist.__getitem__)
        near = sum(hist[max(0, mode - cls.blank_span):
                        mode + cls.blank_span + 1])
        return near / float(sum(hist) or 1) >= cls.blank_ratio

    @classmethod
    def _marker_hit(cls, text, kind):
        """The first ``kind`` marker found in ``text``, or ''."""
        return next((m for m in cls.verdict_markers[kind] if m in text),
                    '')

    @classmethod
    def classify_page(cls, state, png_bytes=None):
        """``(kind, detail)`` naming what stood between the browser and
        the page, or ``('ok', '')``; markers are read before pixels since
        every interstitial is mostly one colour too.

        :param state: dict from ``page_state_script``
        :param png_bytes: the shot, for the solid-colour check
        """
        state = state or {}
        title = str(state.get('title', '')).lower().replace('\u2019', "'")
        text = f"{state.get('title', '')} {state.get('text', '')}".lower()
        text = text.replace('\u2019', "'")
        url = str(state.get('url') or '')
        body_length = int(state.get('len') or 0)
        if str(state.get('proto') or '').startswith('chrome-error'):
            kind = ('ssl_error' if cls._marker_hit(text, 'ssl_error')
                    else 'error_page')
            return kind, cls._marker_hit(text, kind) or url[:120]
        for kind in ('bot_check', 'blocked', 'ssl_error'):
            hit = cls._marker_hit(text, kind)
            if hit:
                return kind, hit
        path = urlparse(url).path.lower()
        login = next((m for m in cls.login_markers if m in text), '')
        if any(path.startswith(p) for p in cls.login_paths):
            return 'login_wall', path[:120]
        if login and body_length < cls.login_body_max:
            return 'login_wall', login
        dialog = ' '.join(str(state.get('dialog') or '').lower().split())
        cover = state.get('cover')
        covers = cover is None or float(cover) >= cls.consent_cover_min
        holds = covers or bool(state.get('modal'))
        if holds and any(w in dialog for w in cls.login_dialog_words):
            return 'login_wall', dialog[:120]
        if covers and any(w in dialog for w in cls.consent_words):
            return 'consent_wall', dialog[:120]
        if png_bytes is not None and cls.is_solid_png(png_bytes):
            return 'blank', 'single-colour page'
        hit = cls._marker_hit(title, 'error_page')
        if not hit and body_length < cls.error_body_max:
            hit = cls._marker_hit(text, 'error_page')
        if hit:
            return 'error_page', hit
        return 'ok', ''

    def capture_verdict(self, png_bytes=None):
        """``classify_page`` for the page in the browser now, shooting
        it unless ``png_bytes`` is given; a dead driver is ``error_page``.
        """
        try:
            state = self.browser.execute_script(self.page_state_script)
            if png_bytes is None:
                png_bytes = self.browser.get_screenshot_as_png()
        except self.browser_errors as e:
            return 'error_page', str(e)[:120]
        return self.classify_page(state, png_bytes)

    def wait_ready(self, seconds):
        """Poll for ``document.readyState == 'complete'``."""
        deadline = time.time() + seconds
        while time.time() < deadline:
            try:
                if self.browser.execute_script(
                        'return document.readyState;') == 'complete':
                    return True
            except self.browser_errors:
                return False
            time.sleep(.25)
        return False

    def wait_painted(self, seconds):
        """The first shot that is not one colour within ``seconds``;
        None when the page never drew."""
        deadline = time.time() + seconds
        while time.time() < deadline:
            try:
                probe = self.browser.execute_script(
                    self.paint_probe_script) or {}
                drawn = (probe.get('lcp') or 0) > 0 or (
                    probe.get('len') or 0) > 0
                png = self.browser.get_screenshot_as_png() if drawn else b''
            except self.browser_errors as e:
                logging.warning(f'Paint not read: {e}')
                return None
            if drawn and not self.is_solid_png(png):
                return png
            time.sleep(self.paint_poll)
        return None

    def forget_visits(self):
        """Clear the browser's cookies so the next page is a first
        visit, which some publishers never check; False on failure."""
        try:
            self.browser.execute_cdp_cmd('Network.clearBrowserCookies', {})
        except self.browser_errors as e:
            logging.warning(f'Cookies not cleared: {e}')
            return False
        return True

    def prepare_shot(self, url, scroll_first):
        """Accept cookies, walk the page when asked and return to the
        top before a shot."""
        if url:
            self.accept_cookies()
        if scroll_first:
            self.scroll_through()
        self.browser.execute_script(self.scroll_top_script)

    def wait_challenge(self):
        """``(kind, detail, seconds)`` once a bot check clears by itself
        or ``challenge_wait`` runs out; it is watched, never touched."""
        start = time.time()
        kind, detail = 'bot_check', ''
        while (kind == 'bot_check'
               and time.time() - start < self.capture.challenge_wait):
            time.sleep(self.challenge_poll)
            kind, detail = self.capture_verdict()
        return kind, detail, int(time.time() - start)

    def clear_interstitial(self, kind):
        """Give a ``kind`` of interstitial the nudge that clears it."""
        if kind == 'blank':
            self.wait_ready(self.ready_wait)
        elif kind == 'consent_wall':
            self.dismiss_consent()
        elif kind == 'login_wall':
            self.dismiss_dialog()

    def settle_and_reshoot(self, png, kind, detail, url=None,
                           scroll_first=False):
        """``(png, kind, detail)`` after re-shooting, within
        ``verdict_budget``, an interstitial that time or a click clears:
        a blank page, a bot check, a consent wall or a sign-up prompt;
        under ``challenge_wait`` a bot check is waited out instead.
        """
        wait = self.capture.challenge_wait
        deadline = time.time() + self.verdict_budget + wait
        settles = {k: list(v) for k, v in self.retry_settles.items()}
        note = ''
        while settles.get(kind) and time.time() < deadline:
            pause = settles[kind].pop(0)
            ready = False
            if kind == 'bot_check' and wait:
                kind, polled, waited = self.wait_challenge()
                if kind == 'bot_check':
                    return png, kind, polled or detail
                note = f'cleared after {waited}s'
                ready = kind not in self.refused_kinds
            else:
                self.clear_interstitial(kind)
                time.sleep(pause)
            try:
                if ready:
                    self.prepare_shot(url, scroll_first)
                else:
                    self.browser.execute_script(self.scroll_top_script)
                png = self.browser.get_screenshot_as_png()
            except self.browser_errors as e:
                return png, 'error_page', str(e)[:120]
            kind, detail = self.capture_verdict(png)
            if kind not in self.refused_kinds:
                detail = self.join_detail(detail, note)
        return png, kind, detail

    def take_screenshot(self, url=None, file_name=None, max_attempts=2,
                        scroll_first=False, sleep=5, full_file_name=None):
        """Save a screenshot of ``url``, or of the current page, and
        return ``classify_page``'s verdict on it.

        :param url: page to load first; omit to shoot what is loaded
        :param file_name: path the png is written to
        :param max_attempts: navigation attempts, kept low because a
            screenshot run walks a whole site list and one unreachable
            site should not hold up the rest
        :param scroll_first: walk the page before the shot so lazily
            loaded slots render
        :param sleep: seconds to let ``url`` settle after loading; the
            capture options' ``paint_wait`` polls for the paint instead
        :param full_file_name: jpeg path for a capped full-page shot
            of a shown page, reported on ``last_full_shot``
        """
        logging.info('Getting screenshot from {} and '
                     'saving to {}.'.format(url, file_name))
        self.last_full_shot = ('', None)
        paint_wait = self.capture.paint_wait
        if url and self.capture.fresh_cookies:
            self.forget_visits()
        if url and not self.go_to_url(url, sleep=0 if paint_wait else sleep,
                                      max_attempts=max_attempts):
            error = self.last_nav_error
            return 'error_page', (f'page unreachable: {error}' if error
                                  else 'page unreachable')
        if url and paint_wait:
            self.wait_painted(paint_wait)
        self.prepare_shot(url, scroll_first)
        png = self.browser.get_screenshot_as_png()
        png, kind, detail = self.settle_and_reshoot(
            png, *self.capture_verdict(png), url=url,
            scroll_first=scroll_first)
        if self.partial_load and kind in self.partial_kinds:
            detail = self.join_detail(self.partial_tag, detail)
        if file_name:
            with open(file_name, 'wb') as f:
                f.write(png)
        if full_file_name and kind in self.full_shot_kinds:
            self.last_full_shot = self.shoot_full_page(full_file_name)
        return kind, detail

    def shoot_full_page(self, file_name):
        """Write a jpeg of the page beyond the viewport, capped at
        ``full_shot_max_height`` so an endless feed stays one file.

        :param file_name: jpeg path to write
        :return: ``(path or '', page height in px or None)``
        """
        try:
            size = self.browser.execute_script(
                'return {w: window.innerWidth,'
                ' h: document.documentElement.scrollHeight};') or {}
            width, page_height = int(size.get('w') or 0), int(
                size.get('h') or 0)
            if not width or not page_height:
                return '', None
            height = min(page_height, self.full_shot_max_height)
            self.write_clip(file_name, {'x': 0, 'y': 0, 'width': width,
                                        'height': height},
                            format='jpeg', quality=self.full_shot_quality)
        except (AttributeError, KeyError, ValueError,
                TypeError) + self.browser_errors as e:
            logging.warning(f'Full-page shot not taken: {e}')
            return '', None
        return file_name, page_height

    def write_clip(self, path, clip, **shot):
        """Write the page's capture of ``clip`` (page pixels, past the
        viewport) to ``path``, with ``shot`` as further capture params;
        raises what the driver or the decode does, writing nothing."""
        answer = self.browser.execute_cdp_cmd(
            'Page.captureScreenshot',
            dict(shot, captureBeyondViewport=True, clip=dict(clip, scale=1)))
        data = base64.b64decode(answer['data'])
        with open(path, 'wb') as f:
            f.write(data)

    def scroll_through(self, steps=3, pause=0.7):
        """Walk the page top to bottom and back so lazily loaded ad
        slots render before the shot.

        :param steps: viewport stops on the way down
        :param pause: seconds to settle at each stop
        """
        for i in range(1, steps + 1):
            self.browser.execute_script(self.scroll_part_script,
                                        i / float(steps))
            time.sleep(pause)
        self.browser.execute_script(self.scroll_top_script)
        time.sleep(pause)

    def take_elem_screenshot(self, url=None, xpath=None, file_name=None):
        logging.info('Getting screenshot from {} and '
                     'saving to {}.'.format(url, file_name))
        self.go_to_url(url, sleep=10)
        elem = self.browser.find_element_by_xpath(xpath)
        elem.screenshot(file_name)

    @staticmethod
    def url_host(url):
        """The host of a url, lower-cased and without ``www.``; '' for
        anything that is not a url."""
        host = urlparse(url or '').netloc.lower()
        return host[4:] if host.startswith('www.') else host

    @staticmethod
    def host_under(host, domain):
        """Whether ``host`` is ``domain`` or one of its subdomains."""
        return host == domain or host.endswith(f'.{domain}')

    @classmethod
    def is_ad_host(cls, host):
        return any(cls.host_under(host, x) for x in cls.ad_hosts)

    def is_ad_frame(self, iframe):
        """Whether a frame is served by an ad host. A frame that went
        stale counts as one: nothing in it should be clicked."""
        try:
            return self.is_ad_host(
                self.url_host(iframe.get_attribute('src') or ''))
        except ex.StaleElementReferenceException:
            return True

    def find_ad_candidates(self):
        """Visible frames, native widgets and ad containers on the page
        of at least ``min_slot_size``, each stamped with a slot number
        so it can be found again after a frame switch, in one driver
        round trip.

        :return: list of dicts with n, tag, id, src, width, height, attrs
        """
        return self.browser.execute_script(
            self.candidates_script, self.slot_mark, self.candidate_selector,
            list(self.min_slot_size))

    @classmethod
    def container_hit(cls, cand):
        """The ad-container word a candidate's id or attributes carry,
        or '' -- a wrapper bigger than ``container_max`` is a page
        region, never a slot."""
        width, height = cand.get('width') or 0, cand.get('height') or 0
        if width > cls.container_max[0] or height > cls.container_max[1]:
            return ''
        text = f"{cand.get('id', '')} {cand.get('attrs', '')}".lower()
        hit = next((x for x in cls.container_markers if x in text), '')
        if not hit and cls.container_id_pattern.search(
                str(cand.get('id', '')).lower()):
            hit = str(cand.get('id'))
        return hit

    @classmethod
    def slot_evidence(cls, cand):
        """The ad signals a candidate carries: an ad-server host in its
        src, a known slot marker in its id or attributes, a native
        widget's name, or -- with none of those -- an ad-container name
        within ``container_max``; then a standard ad size. Size alone
        counts only for a frame, so a bare ``video`` never does.

        :param cand: one dict from ``find_ad_candidates``
        :return: list of evidence strings, empty when it is not a slot
        """
        found = []
        host = cls.url_host(cand.get('src', ''))
        if cls.is_ad_host(host):
            found.append('src host {}'.format(host))
        text = '{} {}'.format(cand.get('id', ''), cand.get('attrs', ''))
        found += ['marker {}'.format(x) for x in cls.ad_markers if x in text]
        lowered = text.lower()
        found += [f'native {x}' for x in cls.native_markers
                  if x in lowered]
        container = '' if found else cls.container_hit(cand)
        if container:
            found.append(f'container {container}')
        size = (cand.get('width'), cand.get('height'))
        if size in cls.iab_sizes and (found or cand.get('tag') == 'iframe'):
            found.append('size {}x{}'.format(*size))
        return found

    @classmethod
    def pick_article_links(cls, links, base_url, limit):
        """The urls of ``headline_links``, at most ``limit``."""
        return [x['href']
                for x in cls.headline_links(links, base_url, limit)]

    @classmethod
    def headline_links(cls, links, base_url, limit):
        """The article pages among a home page's links: same host as
        ``base_url``, a path deep or long enough to be a story, not a
        section or account page, with a real headline; deduped by path.

        :param links: dicts of href and text from
            ``article_links_script``
        :param base_url: the home page the links were read from
        :param limit: articles to keep
        :return: list of ``{'href', 'text'}``, at most ``limit``
        """
        base_host = cls.url_host(base_url)
        base_path = urlparse(base_url).path.rstrip('/')
        picked, seen = [], set()
        for link in links:
            href = str(link.get('href') or '')
            text = ' '.join(str(link.get('text') or '').split())
            parsed = urlparse(href)
            path = parsed.path.rstrip('/')
            segments = [s for s in path.split('/') if s]
            deep = len(segments) >= 2 or (
                segments and len(segments[-1]) >= cls.article_long_slug)
            if (parsed.scheme not in ('http', 'https')
                    or not cls.host_under(cls.url_host(href), base_host)
                    or not deep or path == base_path or path in seen
                    or len(text) < cls.article_min_text
                    or any(d in path.lower() for d in cls.deny_paths)):
                continue
            seen.add(path)
            picked.append({'href': parsed._replace(fragment='').geturl(),
                           'text': text})
            if len(picked) >= limit:
                break
        return picked

    def read_article_links(self):
        """The shown page's candidate links, or [] on failure."""
        try:
            return self.browser.execute_script(
                self.article_links_script) or []
        except self.browser_errors as e:
            logging.warning(f'Article links not read: {e}')
            return []

    def find_article_links(self, base_url, limit=1):
        """Article urls on the shown home page, or [] when the driver
        cannot read them; nothing is clicked.

        :param base_url: the home page in the browser
        :param limit: articles to keep
        """
        return self.pick_article_links(self.read_article_links(),
                                       base_url, limit)

    def read_headlines(self, base_url, limit):
        """The shown home page's ``headline_links``; [] on failure."""
        return self.headline_links(self.read_article_links(), base_url,
                                   limit)

    def read_page_meta(self):
        """The shown page's ``clean_page_meta``; {} on failure."""
        try:
            raw = self.browser.execute_script(self.page_meta_script)
        except self.browser_errors as e:
            logging.warning(f'Page meta not read: {e}')
            return {}
        return self.clean_page_meta(raw)

    @classmethod
    def article_node(cls, nodes):
        """The first JSON-LD node typed as an article, or {}."""
        nodes = nodes if isinstance(nodes, list) else [nodes]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            kinds = node.get('@type') or ''
            kinds = kinds if isinstance(kinds, list) else [kinds]
            if any(str(k).lower() in cls.article_types for k in kinds):
                return node
        return {}

    @classmethod
    def meta_strings(cls, value, keys=('name',)):
        """Every string a meta value, JSON-LD object (by ``keys``) or
        list holds, whitespace collapsed."""
        if isinstance(value, str):
            text = ' '.join(value.split())
            return [text] if text else []
        if isinstance(value, dict):
            held = next((value[k] for k in keys if value.get(k)), '')
            return cls.meta_strings(held, keys)[:1]
        if isinstance(value, list):
            return [s for item in value
                    for s in cls.meta_strings(item, keys)]
        return []

    @classmethod
    def is_web_url(cls, text):
        """Whether ``text`` is an http(s) url short enough to keep."""
        return (urlparse(text).scheme in ('http', 'https')
                and len(text) <= cls.meta_caps['canonical'])

    @classmethod
    def meta_value(cls, field, value):
        """One ``clean_page_meta`` field from one source's raw value;
        empty when it has nothing usable."""
        cap = cls.meta_caps[field]
        if field in cls.meta_url_fields:
            found = cls.meta_strings(value, cls.meta_url_keys)
            return next((s for s in found if cls.is_web_url(s)), '')
        found = cls.meta_strings(value)
        if field == 'tags':
            tags = [t.strip() for s in found for t in s.split(',')]
            return [t[:cap] for t in tags if t][:cls.meta_tag_limit]
        if field in cls.meta_joined_fields:
            return ', '.join(found)[:cap].strip()
        return found[0][:cap].strip() if found else ''

    @classmethod
    def clean_page_meta(cls, raw):
        """A page's editorial fields from ``page_meta_script``'s read,
        each from the best source in ``meta_places`` order, with
        ``sources`` naming where each came from."""
        raw = raw if isinstance(raw, dict) else {}
        found = dict(raw, ld=cls.article_node(raw.get('ld') or []))
        out, sources = {}, {}
        for field, places in cls.meta_places.items():
            for source, key in places:
                held = found.get(source)
                value = cls.meta_value(
                    field, held.get(key) if isinstance(held, dict) else '')
                if value:
                    out[field], sources[field] = value, source
                    break
        feeds = [s for s in cls.meta_strings(raw.get('feeds') or [])
                 if cls.is_web_url(s)]
        feeds = list(dict.fromkeys(feeds))[:cls.meta_feed_limit]
        if feeds:
            out['feeds'] = feeds
        if sources:
            out['sources'] = sources
        return out

    @classmethod
    def landing_domain(cls, hrefs):
        """The page an ad's links lead to, read off the links: a
        redirect parameter's target when one names a non-ad host, else
        the first link host that is not an ad server.

        :param hrefs: link urls read out of the frame
        :return: a host, or '' when every link stays on ad servers
        """
        pattern = re.compile('[?&;](?:{})=([^&;#]+)'.format(
            '|'.join(cls.redirect_params)))
        fallback = ''
        for href in hrefs:
            for target in pattern.findall(href):
                host = cls.url_host(unquote(target))
                if host and not cls.is_ad_host(host):
                    return host
            host = cls.url_host(href)
            if host and not fallback and not cls.is_ad_host(host):
                fallback = host
        return fallback

    def read_ad_frame(self, iframe, depth=0):
        """Read an ad frame's links, images and text without touching
        anything in it. One level of nesting is followed, since a
        SafeFrame wraps the creative in a frame of its own.

        :param iframe: the frame WebElement, from the current context
        :param depth: nesting level already entered
        :return: dict of hrefs, imgs, labels (lists) and text (str)
        """
        found = {'hrefs': [], 'imgs': [], 'labels': [], 'text': ''}
        if not self.switch_to_frame(iframe):
            return found
        try:
            found = self.browser.execute_script(self.frame_read_script)
            inner = [] if depth else [
                x for x in self.browser.find_elements(By.TAG_NAME, 'iframe')
                if self.elem_visible(x)][:2]
            for child in inner:
                got = self.read_ad_frame(child, depth=1)
                for key in ('hrefs', 'imgs', 'labels'):
                    found[key] += got[key]
                found['text'] = '{} {}'.format(found['text'], got['text'])
        except ex.WebDriverException as e:
            logging.warning('Could not read ad frame: {}'.format(e))
        finally:
            self.switch_to_frame(parent=bool(depth))
        return found

    def shoot_elem(self, elem, path):
        """Screenshot one element to ``path`` as a clip of the page's
        capture, since the driver's element shot blanks later pages;
        '' when the driver could not."""
        try:
            box = self.browser.execute_script(self.elem_box_script,
                                              elem) or {}
            if not (box.get('width') and box.get('height')):
                return ''
            time.sleep(self.slot_settle)
            self.write_clip(path, box, format='png')
        except (KeyError, TypeError, ValueError) + self.browser_errors as e:
            logging.warning('Could not shoot ad slot: {}'.format(e))
            return ''
        return path

    def page_vitals(self):
        """The shown page's lab LCP, CLS, TTFB, kilobytes, third-party
        hosts, viewport and ad frames; {} when the driver cannot say."""
        try:
            out = self.browser.execute_script(self.vitals_read_script) or {}
        except self.browser_errors as e:
            logging.warning(f'Page vitals not read: {e}')
            return {}
        out['ad_frames'] = self.ad_frame_count()
        return out

    def ad_frame_count(self):
        """Frames Chrome tagged as ads on the CDP frame tree, or None
        when the driver cannot say."""
        try:
            tree = self.browser.execute_cdp_cmd('Page.getFrameTree', {})
        except (AttributeError,) + self.browser_errors as e:
            logging.warning(f'Ad frame tree not read: {e}')
            return None
        return self.count_ad_frames((tree or {}).get('frameTree') or {})

    @classmethod
    def count_ad_frames(cls, node):
        """Ad-tagged frames in one CDP frame-tree node and its children."""
        frame = node.get('frame') or {}
        kind = (frame.get('adFrameStatus') or {}).get('adFrameType', 'none')
        count = 0 if kind in ('none', None) else 1
        return count + sum(cls.count_ad_frames(child)
                           for child in node.get('childFrames') or [])

    def read_ad_slot(self, cand, evidence, shot_prefix):
        """One slot's shot and reading, or None when it vanished.

        :param cand: one dict from ``find_ad_candidates``
        :param evidence: its ``slot_evidence``
        :param shot_prefix: path prefix for the slot shot, '' to skip
        :return: the ad dict, or None
        """
        try:
            elem = self.browser.find_element(
                By.CSS_SELECTOR,
                '[{}="{}"]'.format(self.slot_mark, cand['n']))
            self.scroll_to_elem(elem)
            shot_path = self.shoot_elem(
                elem, '{}_ad{}.png'.format(shot_prefix, cand['n'])
            ) if shot_prefix else ''
            found = (self.read_ad_frame(elem) if cand['tag'] == 'iframe'
                     else {'hrefs': [], 'imgs': [], 'labels': [], 'text': ''})
        except self.browser_errors as e:
            logging.warning('Ad slot {} not read: {}'.format(cand['n'], e))
            return None
        text = ' '.join([x['alt'] for x in found['imgs'] if x['alt']]
                        + found['labels'] + [found['text']])
        hosts = sorted({self.url_host(x) for x in found['hrefs']}
                       | {self.url_host(x['src']) for x in found['imgs']}
                       - {''})
        frame_host = self.url_host(cand['src'])
        return {'shot_path': shot_path, 'width': cand['width'],
                'height': cand['height'], 'top': cand.get('top'),
                'left': cand.get('left'), 'frame_host': frame_host,
                'hosts': hosts,
                'links': [x[:300] for x in found['hrefs'][:8]],
                'vendor': self.vendor_of(hosts, frame_host, evidence),
                'landing_domain': self.landing_domain(found['hrefs']),
                'text': ' '.join(text.split())[:200], 'evidence': evidence}

    @classmethod
    def vendor_of(cls, hosts, frame_host='', evidence=()):
        """Who served an ad slot: its creative's hosts, then its frame's,
        then a Google slot marker; else the site's own, or '' for
        nothing to go on.
        """
        for host in [*hosts, frame_host]:
            for suffix, name in cls.ad_vendors:
                if cls.host_under(host, suffix):
                    return name
        if any(x.startswith('marker') for x in evidence):
            return 'Google'
        return cls.site_vendor if (hosts or frame_host) else ''

    def scan_ad_slots(self, shot_prefix='', max_slots=None, budget_s=None):
        """Photograph and read the page's ad slots without clicking
        any of them.

        Each slot is shot from the top document and, when it is a
        frame, read for its links, images and text; the click-through
        is inferred from link parameters, never by following one. Work
        is bounded per page so one heavy site cannot stall the sweep.

        :param shot_prefix: path prefix for slot shots (``_ad<n>.png``)
        :param max_slots: slots to keep, default ``max_ad_slots``
        :param budget_s: seconds for the whole scan, default
            ``scan_budget``
        :return: list of ad dicts (shot_path, width, height,
            frame_host, landing_domain, text, evidence)
        """
        deadline = time.time() + (budget_s or self.scan_budget)
        slots = []
        try:
            for cand in self.find_ad_candidates():
                if len(slots) >= (max_slots or self.max_ad_slots):
                    break
                if time.time() > deadline:
                    logging.warning('Ad scan budget exhausted.')
                    break
                evidence = self.slot_evidence(cand)
                if evidence:
                    slots.append(self.read_ad_slot(cand, evidence,
                                                   shot_prefix))
        except self.browser_errors as e:
            logging.warning('Ad scan stopped: {}'.format(e))
        finally:
            self.switch_to_frame()
        return [x for x in slots if x]

    def clear_elem(self, elem_id, attempts=10, sleep_time=.1):
        """Clear an input, retrying while it is not yet interactable.

        Mirrors ``send_keys_wrapper``: re-fetches the element between
        attempts so stale references don't propagate, and gives the
        browser a 100ms polling window for cells that transition into
        editable inputs asynchronously after a click.
        """
        scroll_js = (
            "arguments[0].scrollIntoView({block:'center'});"
            "const r=arguments[0].getBoundingClientRect();"
            "const tb=parseFloat(getComputedStyle(document.documentElement)"
            ".getPropertyValue('--lq-topbar-h'))||48;"
            "if(r.top<tb+8)window.scrollBy(0,r.top-tb-12);")
        for attempt in range(attempts):
            try:
                elem = self.browser.find_element(By.ID, elem_id)
                self.browser.execute_script(scroll_js, elem)
                elem.clear()
                return
            except (ex.ElementNotInteractableException,
                    ex.InvalidElementStateException,
                    ex.StaleElementReferenceException):
                if attempt == attempts - 1:
                    self.browser.execute_script(
                        "arguments[0].value='';"
                        "['input','change'].forEach(t=>arguments[0]"
                        ".dispatchEvent(new Event(t,{bubbles:true})));",
                        elem)
                    return
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
        if clear_existing:
            if is_liquid or clear_specified:
                self._click_liquid_clear_button(elem)
            else:
                elem.clear()
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
            if self.wait_for_elem_load(item[1], new_value=item[0],
                                       attempts=25, raise_exception=False):
                break
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
            if new_value:
                elem = self.send_key_new_value_check(
                    item, get_xpath_from_id, clear_existing, send_escape,
                    elem=elem)
            if not self._is_liquid_xpath(item[1], get_xpath_from_id):
                continue
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

    def xpath_from_id_and_click(self, elem_id, sleep=None, load_elem_id=''):
        if sleep is None:
            sleep = self.default_elem_sleep
        if load_elem_id:
            sleep = .01
        elem_xpath = self.get_xpath_from_id(elem_id)
        self.click_on_xpath(elem_xpath, sleep)
        if load_elem_id:
            try:
                self.wait_for_elem_load(load_elem_id, attempts=200)
            except Exception as e:
                logging.warning('Attempt to re-click: {}'.format(e))
                if self.browser.find_elements(By.XPATH, elem_xpath):
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
                    except ex.WebDriverException:
                        # Handle caught mid-reload: stale, or a node the
                        # new document does not own. Re-find next poll.
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
                file_name = os.environ.get(
                    'LQ_ERROR_SHOT', 'NOT_FOUND_ERROR.png')
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

        Counted in a single script so the walk is atomic: callers poll
        this while the table re-renders, and a per-row round trip goes
        stale the moment a row is replaced mid-count.
        """
        return self.browser.execute_script(
            "const root = arguments[0] "
            "  ? document.getElementById(arguments[0]) : document;"
            "if (!root) return 0;"
            "const tbody = root.querySelector('table > tbody');"
            "if (!tbody) return 0;"
            "return Array.from(tbody.querySelectorAll('tr')).filter("
            "  r => !(r.id || '').includes('Hidden')).length;",
            elem_id or None)

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
        if new_value and elem_form:
            first_id = elem_form[0][1]
            base_id = (first_id.replace(
                f'-{self.selectize_xpath}', '').replace(
                f'-{self.liquid_xpath}', ''))
            self.wait_for_elem_load(base_id, new_value=new_value)
        if submit_id:
            self.xpath_from_id_and_click(submit_id, .01)

    def _looks_like_select_id(self, raw_id, select_form_names):
        """True when ``raw_id`` names a LiquidSelect-backed field."""
        return (
            raw_id in select_form_names
            or 'cur' in raw_id or 'Select' in raw_id
            or '-selectized' in raw_id or '-liquid' in raw_id)

    @staticmethod
    def elem_visible(elem):
        """True when an element already in hand is still displayed.

        ``elem_displayed`` takes an id and re-queries for it; this
        takes the WebElement the caller is already holding, which the
        frame and consent-button scans both are.

        :param elem: a WebElement
        :return: whether it is displayed
        """
        try:
            return elem.is_displayed()
        except ex.StaleElementReferenceException:
            return False

    def elem_displayed(self, elem_id, selector=None):
        """True when an element with this id exists and is visible.

        Swallows not-found and stale-element races so it is safe to
        call repeatedly inside a poll condition (``is_displayed`` on a
        captured WebElement raises once the DOM re-renders).
        """
        selector = selector if selector else self.select_id
        try:
            elems = self.browser.find_elements(selector, elem_id)
            return bool(elems) and elems[0].is_displayed()
        except ex.StaleElementReferenceException:
            return False

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

    def run_worker(self, worker, attempts=None, raise_on_fail=True,
                   click_elem_id='', load_elem_id=''):
        """
        Drain the given RQ worker until a task completes.

        :param worker: Worker exposing ``.work(burst=True)``
        :param attempts: Max polling attempts; ``None`` resolves to
            ``default_worker_attempts`` (20 unless an instance sets it)
        :param raise_on_fail: Raise if no task completes within attempts
        :param click_elem_id: Element to re-click halfway through the
            attempts — for launches whose original click can no-op
        :param load_elem_id: Element that loads after the re-click
        :return: Whether a task completed
        """
        if attempts is None:
            if raise_on_fail:
                attempts = getattr(self, 'default_worker_attempts', 20)
            else:
                attempts = getattr(
                    self, 'default_worker_idle_attempts',
                    getattr(self, 'default_worker_attempts', 20))
        sleep = getattr(self, 'default_worker_sleep', .1)
        return poll_until_true(
            lambda worker: worker.work(burst=True),
            {'worker': worker}, attempts, sleep, raise_on_fail,
            'Worker did not complete task.',
            click_elem_id=click_elem_id, load_elem_id=load_elem_id, sw=self)

    def click_and_run_worker(self, elem_id, worker, load_elem_id='',
                             worker_attempts=20):
        """Click an element by id then drain ``worker``."""
        self.xpath_from_id_and_click(elem_id, load_elem_id=load_elem_id)
        return self.run_worker(worker, attempts=worker_attempts)

    def wait_for_elem_with_worker(self, worker, elem_id, rounds=20,
                                  attempts=20, sleep_time=.05, **kwargs):
        """
        Wait for an element, re-draining ``worker`` between checks.

        ``run_worker`` returns once any single task completes, which may
        be a leftover from a prior action rather than the task this
        element waits on -- and a task the page enqueues after that
        return is then never picked up. Re-draining covers both.

        :param worker: Worker exposing ``.work(burst=True)``
        :param elem_id: Identifier of the element to wait for
        :param rounds: Max drain/check cycles before the final wait
        :param attempts: Element polling attempts per cycle
        :param sleep_time: Time in seconds to sleep between polls
        :param kwargs: Passed through to ``wait_for_elem_load``
        :return: Whether the element loaded
        """
        for _ in range(rounds):
            if self.wait_for_elem_load(elem_id, attempts=attempts,
                                       sleep_time=sleep_time,
                                       raise_exception=False, **kwargs):
                return True
            worker.work(burst=True)
        return self.wait_for_elem_load(elem_id, **kwargs)

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

    def search_liquid_table(self, table_name, search_val=None, submit_id=''):
        """
        Clears and/or enters new value into the search bar of a liquid table.

        :param table_name: Name of liquid table to search
        :param search_val: Value to enter into the search bar after clearing it,
            if any
        :param submit_id: ID of HTML element to click after the search bar has
            been modified, if any (e.x. the id of a row to open/ reveal the
            hidden row of)

        """
        search_id = 'tableSearchInput{}Table'.format(table_name)
        search_elem = self.browser.find_element_by_id(search_id)
        search_elem.clear()
        if search_val:
            self.submit_form(form_names=[search_id], submit_id=submit_id,
                             test_name=search_val)
        else:
            search_elem.send_keys(Keys.ENTER)


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
