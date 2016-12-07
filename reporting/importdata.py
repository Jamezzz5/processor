import pandas as pd
import vendormatrix as vm
import cleaning as cln
import dictionary as dct
import errorreport as er
import calc as cal
import logging

log = logging.getLogger()


def import_readcsv(csvpath, filename):
    rawfile = csvpath + filename
    dataframe = pd.read_csv(rawfile, parse_dates=True)
    return dataframe


def full_placement_creation(df, key, fullcol, fullplacecols):
    logging.debug('Creating Full Placement Name')
    df[fullcol] = ''
    i = 0
    for col in fullplacecols:
        if col not in df:
            logging.warn(col + ' was not in ' + key + '.  It was not ' +
                         'included in Full Placement Name.  For reference '
                         'column names are as follows: \n' +
                         str(df.columns.values.tolist()))
            continue
        df[col] = df[col].astype(str)
        if i == 0:
            df[fullcol] = df[col]
        else:
            df[fullcol] = (df[fullcol] + '_' + df[col])
        i = i + 1
    return df


def combining_data(df, key, **kwargs):
    logging.debug('Combining Data.')
    for col in vm.datacol:
        if pd.isnull(kwargs[col]):
            continue
        if kwargs[col] not in df:
            logging.warn(kwargs[col] + ' is not in ' + key +
                         '.  It was not put in ' + col)
            continue
        df[col] = df[kwargs[col]]
    return df


def import_data(key, **kwargs):
    data = import_readcsv(vm.pathraw, kwargs[vm.filename])
    data = cln.firstlastadj(data, kwargs[vm.firstrow], kwargs[vm.lastrow])
    data = full_placement_creation(data, key, dct.FPN,
                                   kwargs[vm.fullplacename])
    dic = dct.Dict(kwargs[vm.filenamedict])
    err = er.ErrorReport(data, dic, kwargs[vm.placement],
                         kwargs[vm.filenameerror])
    dic.auto(err, kwargs[vm.autodicord], kwargs[vm.placement])
    data = dic.merge(data, dct.FPN)
    data = combining_data(data, key, **kwargs)

    data = cln.data_to_type(data, vm.datafloatcol, vm.datadatecol, [])
    data = cln.date_removal(data, kwargs[vm.startdate])
    data = cal.adcost_calculation(data)
    data = cln.col_removal(data, key, kwargs[vm.dropcol])
    data = cln.null_items(data, key, **kwargs)
    data = cln.null_items_date(data, key, **kwargs)

    return data


def import_plan_data(key, data, **kwargs):
    data = data.loc[:, kwargs[vm.fullplacename]]
    data = full_placement_creation(data, key, dct.FPN,
                                   kwargs[vm.fullplacename])
    dic = dct.Dict(kwargs[vm.filenamedict])
    er.ErrorReport(data, dic, kwargs[vm.placement], kwargs[vm.filenameerror])
    data = dic.merge(data, dct.FPN)
    data = data.drop_duplicates()
    undersplit = lambda x: pd.Series([i for i in (x.split('_'))])
    data[vm.fullplacename] = (data[dct.FPN].apply(undersplit))
    return data
