import os
import sys
import logging
import pandas as pd
import datetime as dt
import reporting.vmcolumns as vmc
import reporting.dictcolumns as dctc

config_path = 'config/'
raw_path = 'raw_data/'
error_path = 'ERROR_REPORTS/'
dict_path = 'dictionaries/'

RULE_METRIC = 'METRIC'
RULE_QUERY = 'QUERY'
RULE_FACTOR = 'FACTOR'
RULE_CONST = [RULE_METRIC, RULE_QUERY, RULE_FACTOR]
PRE = 'PRE'
POST = 'POST'


def dir_check(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def import_read_csv(filename, path=None):
    if path:
        filename = os.path.join(path, filename)
    if not os.path.isfile(filename):
        logging.warning('{} not found.  Continuing.'.format(filename))
        return pd.DataFrame()
    try:
        df = pd.read_csv(filename, parse_dates=True, encoding='utf-8')
    except pd.io.common.CParserError:
        df = pd.read_csv(filename, parse_dates=True, sep=None, engine='python')
    except UnicodeDecodeError:
        df = pd.read_csv(filename, parse_dates=True, encoding='iso-8859-1')
    except pd.io.common.EmptyDataError:
        logging.warning('Raw Data {} empty.  Continuing.'.format(filename))
        df = None
    return df


def exceldate_to_datetime(excel_date):
    epoch = dt.datetime(1899, 12, 30)
    delta = dt.timedelta(hours=round(excel_date * 24))
    return epoch + delta


def string_to_date(my_string):
    if ('/' in my_string and my_string[-4:][:2] != '20' and
            ':' not in my_string and len(my_string) in [6, 7, 8]):
        return dt.datetime.strptime(my_string, '%m/%d/%y')
    elif ('/' in my_string and my_string[-4:][:2] == '20' and
            ':' not in my_string):
        return dt.datetime.strptime(my_string, '%m/%d/%Y')
    elif (((len(my_string) == 5) and (my_string[0] == '4')) or
            ((len(my_string) == 7) and ('.' in my_string))):
        return exceldate_to_datetime(float(my_string))
    elif len(my_string) == 8 and my_string[0].isdigit():
        return dt.datetime.strptime(my_string, '%Y%m%d')
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
    else:
        return my_string


def data_to_type(df, float_col=None, date_col=None, str_col=None):
    if float_col is None:
        float_col = []
    if date_col is None:
        date_col = []
    if str_col is None:
        str_col = []
    for col in float_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: x.replace(',', ''))
        df[col] = df[col].replace(['nan', 'NA'], 0)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].astype(float)
    for col in date_col:
        if col not in df:
            continue
        df[col] = df[col].replace(['1/0/1900', '1/1/1970'], '0')
        df[col] = df[col].fillna(dt.date.today())
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: string_to_date(x))
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()
    for col in str_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
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
        sys.exit(0)
    return df


def date_removal(df, date_col_name, start_date, end_date):
    if (end_date.date() is not pd.NaT and
            end_date.date() != dt.date.today()):
        df = df[df[date_col_name] <= end_date]
    if (start_date.date() is not pd.NaT and
            start_date.date() != dt.date.today()):
        df = df[df[date_col_name] >= start_date]
    return df


def col_removal(df, key, removal_cols):
    logging.debug('Dropping unnecessary columns')
    if removal_cols == ['ALL']:
        removal_cols = [x for x in df.columns
                        if x not in dctc.COLS + vmc.datacol + vmc.ad_rep_cols]
    for col in removal_cols:
        if col not in df:
            if col == 'nan':
                continue
            logging.warning('{} is not or is no longer in {}.'
                            'It was not removed.'.format(col, key))
            continue
        df = df.drop(col, axis=1)
    return df


def apply_rules(df, vm_rules, pre_or_post, **kwargs):
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
        if len(metrics) == 3:
            df[metrics[2]] = df[metrics[1]]
            metrics[1] = metrics[2]
        tdf = df
        metrics = metrics[1].split('|')
        queries = queries.split('|')
        for query in queries:
            query = query.split('::')
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
                tdf = tdf.loc[tdf[query[0]].isin(values)]
        tdf = list(tdf.index.values)
        for metric in metrics:
            if metric not in df:
                logging.warning('{} not in data for rule {}.  '
                                'The rule did not run.'.format(metric, rule))
                continue
            df = data_to_type(df, float_col=[metric])
            df.ix[tdf, metric] = (df.ix[tdf, metric].astype(float) *
                                  factor.astype(float))
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
