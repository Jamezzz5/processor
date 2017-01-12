import logging
import os.path
import pandas as pd
import numpy as np
import cleaning as cln
import dictionary as dct
import errorreport as er

log = logging.getLogger()

csv = 'Config/Vendormatrix.csv'
pathraw = 'Raw Data/'

vendorkey = 'Vendor Key'
filename = 'FILENAME'
firstrow = 'FIRSTROW'
lastrow = 'LASTROW'
fullplacename = 'Full Placement Name'
placement = 'Placement Name'
filenamedict = 'FILENAME_DICTIONARY'
filenameerror = 'FILENAME_ERROR'
startdate = 'START DATE'
enddate = 'END DATE'
dropcol = 'DROP_COLUMNS'
autodicord = 'AUTO DICTIONARY ORDER'
apifile = 'API_FILE'
apifields = 'API_FIELDS'
apimerge = 'API_MERGE'
date = 'Date'
impressions = 'Impressions'
clicks = 'Clicks'
cost = 'Net Cost'
conv1 = 'Conv1 - CPA'
conv2 = 'Conv2'
conv3 = 'Conv3'
conv4 = 'Conv4'
conv5 = 'Conv5'
conv6 = 'Conv6'
conv7 = 'Conv7'
conv8 = 'Conv8'
conv9 = 'Conv9'
conv10 = 'Conv10'
nullimps = 'NULL_IMPRESSIONS'
nullclicks = 'NULL_CLICKS'
nullcost = 'NULL_COST'
nullconv1 = 'NULL_CONV'
nullconv2 = 'NULL_CONV2'
nullconv3 = 'NULL_CONV3'
nullconv4 = 'NULL_CONV4'
nullconv5 = 'NULL_CONV5'
nullconv6 = 'NULL_CONV6'
nullconv7 = 'NULL_CONV7'
nullconv8 = 'NULL_CONV8'
nullconv9 = 'NULL_CONV9'
nullconv10 = 'NULL_CONV10'
nullimpssd = 'NULL IMPS - SD'
nullimpsed = 'NULL IMPS - ED'
nullclicksd = 'NULL CLICK - SD'
nullclicked = 'NULL CLICK - ED'
nullcostsd = 'NULL COST - SD'
nullcosted = 'NULL COST - ED'
nullconv1sd = 'NULL CONV1 - SD'
nullconv1ed = 'NULL CONV1 - ED'
nullconv2sd = 'NULL CONV2 - SD'
nullconv2ed = 'NULL CONV2 - ED'
nullconv3sd = 'NULL CONV3 - SD'
nullconv3ed = 'NULL CONV3 - ED'
nullconv4sd = 'NULL CONV4 - SD'
nullconv4ed = 'NULL CONV4 - ED'
nullconv5sd = 'NULL CONV5 - SD'
nullconv5ed = 'NULL CONV5 - ED'
nullconv6sd = 'NULL CONV6 - SD'
nullconv6ed = 'NULL CONV6 - ED'
nullconv7sd = 'NULL CONV7 - SD'
nullconv7ed = 'NULL CONV7 - ED'
nullconv8sd = 'NULL CONV8 - SD'
nullconv8ed = 'NULL CONV8 - ED'
nullconv9sd = 'NULL CONV9 - SD'
nullconv9ed = 'NULL CONV9 - ED'
nullconv10sd = 'NULL CONV10 - SD'
nullconv10ed = 'NULL CONV10 - ED'

vmkeys = [filename, firstrow, lastrow, fullplacename, placement, filenamedict,
          filenameerror, startdate, enddate, dropcol, autodicord, apifile,
          apifields, apimerge]

datacol = [date, impressions, clicks, cost, conv1, conv2, conv3, conv4, conv5,
           conv6, conv7, conv8, conv9, conv10]

nullcol = [nullimps, nullclicks, nullcost, nullconv1, nullconv2,
           nullconv3, nullconv4, nullconv5, nullconv6, nullconv7, nullconv8,
           nullconv9, nullconv10]

nulldate = [nullimpssd, nullimpsed, nullclicksd, nullclicked, nullcostsd,
            nullcosted, nullconv1sd, nullconv1ed, nullconv2sd, nullconv2ed,
            nullconv3sd, nullconv3ed, nullconv4sd, nullconv4ed, nullconv5sd,
            nullconv5ed, nullconv6sd, nullconv6ed, nullconv7sd, nullconv7ed,
            nullconv8sd, nullconv8ed, nullconv9sd, nullconv9ed, nullconv10sd,
            nullconv10ed]

vmkeys = vmkeys + datacol + nullcol + nulldate
barsplitcol = ([fullplacename, dropcol, autodicord, apifields] + nullcol +
               nulldate)

datecol = [startdate, enddate] + nulldate
datadatecol = [date] + nulldate
datafloatcol = [impressions, clicks, cost, conv1, conv2, conv3, conv4, conv5,
                conv6, conv7, conv8, conv9, conv10]

nullcoldic = dict(zip(datafloatcol, nullcol))
nulldatedic = dict(zip(datafloatcol, zip(nulldate, nulldate[1:])[::2]))

plankey = 'Plan Net'
ADCOST = 'Adserving Cost'
AM_CPM = 'CPM'


class VendorMatrix(object):
    def __init__(self):
        log.info('Initializing Vendor Matrix')
        self.vm_parse()
        self.vm_importkeys()

    def vm_parse(self):
        self.vm = pd.DataFrame(columns=[datacol])
        self.vm = pd.read_csv(csv)
        drop = [item for item in self.vm.columns.values.tolist()
                if (item[0] == '|')]
        self.vm = cln.col_removal(self.vm, 'vm', drop)
        planrow = (self.vm.loc[self.vm[vendorkey] == plankey])
        self.vm = self.vm[self.vm[vendorkey] != plankey]
        self.vm = self.vm.append(planrow).reset_index()
        self.vm = cln.data_to_type(self.vm, [], datecol, barsplitcol)
        self.vl = self.vm[vendorkey].tolist()
        self.vm = self.vm.set_index(vendorkey).to_dict()
        for col in barsplitcol:
            self.vm[col] = ({key: list(str(value).split('|')) for key, value in
                            self.vm[col].items()})

    def vm_importkeys(self):
        self.apifbkey = []
        self.apiawkey = []
        self.apitwkey = []
        self.ftpszkey = []
        for vk in self.vl:
            vksplit = {vk: vk.split('_')}
            if vksplit[vk][0] == 'API':
                if vksplit[vk][1] == 'Adwords':
                    self.apiawkey.append(vk)
                if vksplit[vk][1] == 'Facebook':
                    self.apifbkey.append(vk)
                if vksplit[vk][1] == 'Twitter':
                    self.apitwkey.append(vk)
            if vksplit[vk][0] == 'FTP':
                if vksplit[vk][1] == 'Sizmek':
                    self.ftpszkey.append(vk)

    def vendor_set(self, vk):
        venparam = {}
        for key in vmkeys:
            value = self.vm[key][vk]
            venparam[key] = value
        return venparam

    def vendor_check(self, vk):
        if os.path.isfile(pathraw + self.vm[filename][vk]) or vk == plankey:
            return True
        else:
            return False

    def vmloop(self):
        logging.info('Initializing Vendor Matrix Loop')
        self.df = pd.DataFrame(columns=[datacol])
        for vk in self.vl:
            if self.vendor_check(vk):
                self.venparam = self.vendor_set(vk)
                logging.info('Initializing ' + vk)
                if vk == plankey:
                    self.tdf = import_plan_data(vk, self.df, **self.venparam)
                    print self.tdf
                else:
                    self.tdf = import_data(vk, **self.venparam)
                self.df = self.df.append(self.tdf, ignore_index=True)
        self.df = full_placement_creation(self.df, plankey, dct.PFPN,
                                          self.vm[fullplacename][plankey])
        return self.df


def import_readcsv(csvpath, filename):
    rawfile = csvpath + filename
    df = pd.read_csv(rawfile, parse_dates=True)
    return df


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
    for col in datacol:
        if pd.isnull(kwargs[col]):
            continue
        if kwargs[col] not in df:
            logging.warn(kwargs[col] + ' is not in ' + key +
                         '.  It was not put in ' + col)
            continue
        df[col] = df[kwargs[col]]
    return df


def adcost_calculation(df):
    if clicks not in df:
        return df
    df[ADCOST] = np.where(df[dct.AM] == AM_CPM,
                          df[dct.AR] * df[impressions]/1000,
                          df[dct.AR] * df[clicks])
    return df


def import_data(key, **kwargs):
    df = import_readcsv(pathraw, kwargs[filename])
    df = cln.firstlastadj(df, kwargs[firstrow], kwargs[lastrow])
    df = full_placement_creation(df, key, dct.FPN, kwargs[fullplacename])
    dic = dct.Dict(kwargs[filenamedict])
    err = er.ErrorReport(df, dic, kwargs[placement], kwargs[filenameerror])
    dic.auto(err, kwargs[autodicord], kwargs[placement])
    df = dic.merge(df, dct.FPN)
    df = combining_data(df, key, **kwargs)
    df = cln.data_to_type(df, datafloatcol, datadatecol, [])
    df = cln.date_removal(df, date, kwargs[startdate], kwargs[enddate])
    df = adcost_calculation(df)
    df = cln.col_removal(df, key, kwargs[dropcol])
    df = cln.null_items(df, key, dct.VEN, nullcoldic, **kwargs)
    df = cln.null_items_date(df, key, date, nulldatedic, **kwargs)
    return df


def import_plan_data(key, df, **kwargs):
    df = df.loc[:, kwargs[fullplacename]]
    df = full_placement_creation(df, key, dct.FPN, kwargs[fullplacename])
    dic = dct.Dict(kwargs[filenamedict])
    er.ErrorReport(df, dic, kwargs[placement], kwargs[filenameerror])
    df = dic.merge(df, dct.FPN)
    df = df.drop_duplicates()
    barsplit = lambda x: pd.Series([i for i in (x.split('|'))])
    df[fullplacename] = (df[dct.FPN].apply(barsplit))
    return df
