import logging
import datetime as dt
import numpy as np
import pandas as pd

log = logging.getLogger()


def exceldate_to_datetime(xlDate):
    epoch = dt.datetime(1899, 12, 30)
    delta = dt.timedelta(hours=round(xlDate*24))
    return epoch + delta


def string_to_date(my_string):
    if '/' in my_string and (len(my_string) == 7 or len(my_string) == 8):
        return dt.datetime.strptime(my_string, '%m/%d/%Y')
    elif '/' in my_string and (len(my_string) == 10 or len(my_string) == 9):
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


def data_to_type(df, floatcol, datecol, strcol):
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
    if firstrow > 0:
        df.columns = df.iloc[firstrow-1]
        df = df.ix[firstrow:]
    if lastrow > 0:
        df = df[:-lastrow]
    df = df.reset_index()
    if pd.isnull(df.columns.values).any():
        logging.warn('At least one column name is undefined.  Your first row '
                     'is likely incorrect. For reference the first few rows '
                     'are:\n' + str(df.head()))
        quit()
    return df


def date_removal(df, datecolname, startdate, enddate):
    if enddate != dt.date.today() - dt.timedelta(weeks=520):
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
