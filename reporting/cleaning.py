import logging
import sys
import os
import datetime as dt
import numpy as np
import pandas as pd
import vmcolumns as vmc

RULEMETRIC = 'METRIC'
RULEQUERY = 'QUERY'
RULEFACTOR = 'FACTOR'
RULECONST = [RULEMETRIC, RULEQUERY, RULEFACTOR]


def dircheck(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def exceldate_to_datetime(xlDate):
    epoch = dt.datetime(1899, 12, 30)
    delta = dt.timedelta(hours=round(xlDate*24))
    return epoch + delta


def string_to_date(my_string):
    if '/' in my_string and my_string[-4:][:2] != '20':
        return dt.datetime.strptime(my_string, '%m/%d/%y')
    elif '/' in my_string and my_string[-4:][:2] == '20':
        return dt.datetime.strptime(my_string, '%m/%d/%Y')
    elif ((len(my_string) == 5) or
            ((len(my_string) == 7) and ('.' in my_string))):
        return exceldate_to_datetime(float(my_string))
    elif len(my_string) == 8:
        return dt.datetime.strptime(my_string, '%Y%m%d')
    elif my_string == '0' or my_string == '0.0':
        return dt.date.today() - dt.timedelta(weeks=520)
    elif ((len(my_string) == 22) and (':' in my_string) and
            ('+' in my_string)):
        my_string = my_string[:-6]
        return dt.datetime.strptime(my_string, '%Y-%m-%d %M:%S')
    else:
        return my_string


def data_to_type(df, floatcol=[], datecol=[], strcol=[]):
    for col in floatcol:
        if col not in df:
            continue
        df[col] = df[col].astype(str)
        df[col] = df[col].apply(lambda x: x.replace(',', ''))
        df[col] = df[col].astype(float)
    for col in datecol:
        if col not in df:
            continue
        df[col] = df[col].replace('1/0/1900', '0')
        df[col] = df[col].fillna(0)
        df[col] = df[col].astype(str)
        df[col] = df[col].apply(lambda x: string_to_date(x))
        df[col] = df[col].astype('datetime64[ns]')
    for col in strcol:
        if col not in df:
            continue
        df[col] = df[col].astype(str)
    return df


def firstlastadj(df, firstrow, lastrow):
    logging.debug('Removing First & Last Rows')
    firstrow = int(firstrow)
    lastrow = int(lastrow)
    if firstrow > 0:
        df.columns = df.loc[firstrow-1]
        df = df.ix[firstrow:]
    if lastrow > 0:
        df = df[:-lastrow]
    if pd.isnull(df.columns.values).any():
        logging.warn('At least one column name is undefined.  Your first row '
                     'is likely incorrect. For reference the first few rows '
                     'are:\n' + str(df.head()))
        sys.exit(0)
    return df


def date_removal(df, datecolname, startdate, enddate):
    if enddate.date() != (dt.date.today() - dt.timedelta(weeks=520)):
        df = df[df[datecolname] <= enddate]
    df = df[df[datecolname] >= startdate]
    return df


def col_removal(df, key, removalcols):
    logging.debug('Dropping unnecessary columns')
    for col in removalcols:
        if col not in df:
            if col == 'nan':
                continue
            logging.warn(col + ' is not or is no longer in ' +
                         key + '.  It was not removed.')
            continue
        df = df.drop(col, axis=1)
    return df


def null_items(df, key, vencolname, nullcoldic, **kwargs):
    logging.debug('Nulling vendor metrics')
    for col in nullcoldic:
        if col not in df.columns:
            continue
        for ven in kwargs[nullcoldic[col]]:
            df[col] = np.where((df[vencolname] == ven), 0, df[col])
    return df


def null_items_date(df, key, datecol, nulldatedic, **kwargs):
    logging.debug('Nulling vendor metrics by date')
    for col in nulldatedic:
        if col not in df.columns:
            continue
        for sd, ed in zip(kwargs[nulldatedic[col][0]],
                          kwargs[nulldatedic[col][1]]):
            if sd == 'nan' or ed == 'nan':
                continue
            sd = string_to_date(sd)
            ed = string_to_date(ed)
            df[col] = np.where((df[datecol] >= sd) & (df[datecol] <= ed),
                               0, df[col])
    return df


def apply_rules(df, vmrules, **kwargs):
    for rule in vmrules:
        for item in RULECONST:
            if item not in vmrules[rule].keys():
                logging.warn(item + ' not in vendormatrix for rule ' +
                             rule + '.  The rule did not run.')
                return df
        metrics = kwargs[vmrules[rule][RULEMETRIC]]
        queries = kwargs[vmrules[rule][RULEQUERY]]
        factor = kwargs[vmrules[rule][RULEFACTOR]]
        if (str(metrics) == 'nan' or str(queries) == 'nan' or
           str(factor) == 'nan'):
            continue
        tdf = df
        metrics = metrics.split('|')
        queries = queries.split('|')
        for query in queries:
            query = query.split(':')
            values = query[1].split(',')
            if query[0] not in df.columns:
                continue
            if query[0] == vmc.date:
                sd = string_to_date(values[0])
                ed = string_to_date(values[1])
                tdf = tdf.loc[(df[query[0]] >= sd) & (df[query[0]] <= ed)]
            else:
                tdf = tdf.loc[tdf[query[0]].isin(values)]
        tdf = list(tdf.index.values)
        for metric in metrics:
            df.ix[tdf, metric] = df.ix[tdf, metric] * factor
        df.to_csv('test.csv')
    return df
