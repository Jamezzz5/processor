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
backup_path = 'backup/'

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
    if sheet_names:
        df = pd.concat(df, ignore_index=True, sort=True)
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
    elif (len(my_string) == 8 and my_string[0].isdigit()
            and my_string[0] == '2'):
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


def data_to_type(df, float_col=None, date_col=None, str_col=None, int_col=None):
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
        df[col] = df[col].fillna(dt.datetime.today())
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: string_to_date(x))
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()
    for col in str_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
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


def col_removal(df, key, removal_cols, warn=True):
    logging.debug('Dropping unnecessary columns')
    if 'ALL' in removal_cols:
        removal_cols = [x for x in df.columns
                        if x not in dctc.COLS + vmc.datacol + vmc.ad_rep_cols +
                        removal_cols]
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
