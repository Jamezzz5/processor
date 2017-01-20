import logging
import sys
import os.path
import pandas as pd
import numpy as np
import vmcolumns as vmc
import cleaning as cln
import dictionary as dct
import dictcolumns as dctc
import errorreport as er

log = logging.getLogger()

csvpath = 'Config/'
csv = csvpath + 'Vendormatrix.csv'
plankey = 'Plan Net'
ADCOST = 'Adserving Cost'
AM_CPM = 'CPM'


class VendorMatrix(object):
    def __init__(self):
        log.info('Initializing Vendor Matrix')
        cln.dircheck(csvpath)
        self.vm_parse()
        self.vm_importkeys()

    def read(self):
        if not os.path.isfile(csv):
            logging.info('Creating Vendor Matrix.  Populate it and run again')
            vm = pd.DataFrame(columns=[vmc.vendorkey] + vmc.vmkeys, index=None)
            vm.to_csv(csv, index=False)
        self.vm = pd.read_csv(csv)

    def plannetcheck(self):
        if not self.vm['Vendor Key'].isin(['Plan Net']).any():
            logging.error('No Plan Net key in Vendor Matrix.  Add it.')
            sys.exit(0)

    def vm_parse(self):
        self.vm = pd.DataFrame(columns=vmc.datacol)
        self.read()
        self.plannetcheck()
        drop = [item for item in self.vm.columns.values.tolist()
                if (item[0] == '|')]
        self.vm = cln.col_removal(self.vm, 'vm', drop)
        planrow = (self.vm.loc[self.vm[vmc.vendorkey] == plankey])
        self.vm = self.vm[self.vm[vmc.vendorkey] != plankey]
        self.vm = self.vm.append(planrow).reset_index()
        self.vm = cln.data_to_type(self.vm, [], vmc.datecol, vmc.barsplitcol)
        self.vl = self.vm[vmc.vendorkey].tolist()
        self.vm = self.vm.set_index(vmc.vendorkey).to_dict()
        for col in vmc.barsplitcol:
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
        for key in vmc.vmkeys:
            value = self.vm[key][vk]
            venparam[key] = value
        return venparam

    def vendor_check(self, vk):
        if (os.path.isfile(vmc.pathraw + self.vm[vmc.filename][vk]) or
           vk == plankey):
            return True
        else:
            return False

    def vendor_get(self, vk):
        self.venparam = self.vendor_set(vk)
        logging.info('Initializing ' + vk)
        if vk == plankey:
            self.tdf = import_plan_data(vk, self.df, **self.venparam)
        else:
            self.tdf = import_data(vk, **self.venparam)
        return self.tdf

    def vmloop(self):
        logging.info('Initializing Vendor Matrix Loop')
        self.df = pd.DataFrame(columns=[vmc.datacol])
        for vk in self.vl:
            if self.vendor_check(vk):
                self.tdf = self.vendor_get(vk)
                self.df = self.df.append(self.tdf, ignore_index=True)
        self.df = full_placement_creation(self.df, plankey, dctc.PFPN,
                                          self.vm[vmc.fullplacename][plankey])
        if not os.listdir(er.csvpath):
            if os.path.isdir(er.csvpath):
                logging.info('All placements defined.  Deleting Error report' +
                             ' directory.')
                os.rmdir(er.csvpath)
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
    for col in vmc.datacol:
        if pd.isnull(kwargs[col]):
            continue
        if kwargs[col] not in df:
            logging.warn(kwargs[col] + ' is not in ' + key +
                         '.  It was not put in ' + col)
            continue
        df[col] = df[kwargs[col]]
    return df


def adcost_calculation(df):
    if (vmc.impressions not in df) or (vmc.clicks not in df):
        return df
    df[ADCOST] = np.where(df[dctc.AM] == AM_CPM,
                          df[dctc.AR] * df[vmc.impressions]/1000,
                          df[dctc.AR] * df[vmc.clicks])
    return df


def import_data(key, **kwargs):
    df = import_readcsv(vmc.pathraw, kwargs[vmc.filename])
    df = cln.firstlastadj(df, kwargs[vmc.firstrow], kwargs[vmc.lastrow])
    df = full_placement_creation(df, key, dctc.FPN, kwargs[vmc.fullplacename])
    dic = dct.Dict(kwargs[vmc.filenamedict])
    err = er.ErrorReport(df, dic, kwargs[vmc.placement],
                         kwargs[vmc.filenameerror])
    dic.auto(err, kwargs[vmc.autodicord], kwargs[vmc.placement])
    df = dic.merge(df, dctc.FPN)
    df = combining_data(df, key, **kwargs)
    df = cln.data_to_type(df, vmc.datafloatcol, vmc.datadatecol, [])
    df = cln.date_removal(df, vmc.date, kwargs[vmc.startdate],
                          kwargs[vmc.enddate])
    df = adcost_calculation(df)
    df = cln.col_removal(df, key, kwargs[vmc.dropcol])
    df = cln.null_items(df, key, dctc.VEN, vmc.nullcoldic, **kwargs)
    df = cln.null_items_date(df, key, vmc.date, vmc.nulldatedic, **kwargs)
    return df


def import_plan_data(key, df, **kwargs):
    df = df.loc[:, kwargs[vmc.fullplacename]]
    df = full_placement_creation(df, key, dctc.FPN, kwargs[vmc.fullplacename])
    dic = dct.Dict(kwargs[vmc.filenamedict])
    er.ErrorReport(df, dic, kwargs[vmc.placement], kwargs[vmc.filenameerror])
    df = dic.merge(df, dctc.FPN)
    df = df.drop_duplicates()
    barsplit = lambda x: pd.Series([i for i in (x.split('|'))])
    df[vmc.fullplacename] = (df[dctc.FPN].apply(barsplit))
    return df


def vm_update(oldfile='Config/OldVendorMatrix.csv'):
    logging.info('Updating Vendor Matrix')
    ovm = pd.read_csv(oldfile)
    nvm = pd.DataFrame(columns=[vmc.vendorkey] + vmc.vmkeys)
    vm = nvm.append(ovm)
    for col in [vmc.fullplacename, vmc.dropcol, vmc.autodicord] + vmc.nullcol:
        vm[col] = vm[col].replace({'_': '|'}, regex=True)
    vm[vmc.firstrow] = np.where(vm['FIRSTROWADJ'] == True,
                                vm[vmc.firstrow] + 1, vm[vmc.firstrow])
    vm = cln.col_removal(vm, 'vm',
                         ['FIRSTROWADJ', 'LASTROWADJ', 'AUTO DICTIONARY'])
    vm = vm.reindex_axis([vmc.vendorkey] + vmc.vmkeys, axis=1)
    vm.to_csv(csv, index=False)
