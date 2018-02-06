import logging
import sys
import os.path
import pandas as pd
import numpy as np
import reporting.vmcolumns as vmc
import reporting.utils as utl
import reporting.dictionary as dct
import reporting.dictcolumns as dctc
import reporting.errorreport as er

log = logging.getLogger()

csv_path = 'Config/'
csv = csv_path + 'Vendormatrix.csv'
plan_key = 'Plan Net'


class VendorMatrix(object):
    def __init__(self):
        log.info('Initializing Vendor Matrix')
        utl.dir_check(csv_path)
        self.vm = None
        self.vl = []
        self.api_fb_key = []
        self.api_aw_key = []
        self.api_tw_key = []
        self.api_ttd_key = []
        self.api_ga_key = []
        self.api_nb_key = []
        self.api_af_key = []
        self.ftp_sz_key = []
        self.db_dna_key = []
        self.s3_dna_key = []
        self.vm_rules_dict = {}
        self.ven_param = None
        self.tdf = None
        self.df = None
        self.vm_parse()
        self.vm_import_keys()
        self.vm_rules()
        self.plan_omit_list = [k for k, v in self.vm[vmc.omit_plan].items()
                               if str(v) != str('nan')]

    def read(self):
        if not os.path.isfile(csv):
            logging.info('Creating Vendor Matrix.  Populate it and run again')
            vm = pd.DataFrame(columns=[vmc.vendorkey] + vmc.vmkeys, index=None)
            vm.to_csv(csv, index=False)
        try:
            self.vm = pd.read_csv(csv, encoding='utf-8')
        except UnicodeDecodeError:
            self.vm = pd.read_csv(csv, encoding='iso-8859-1')

    def plan_net_check(self):
        if not self.vm['Vendor Key'].isin(['Plan Net']).any():
            logging.error('No Plan Net key in Vendor Matrix.  Add it.')
            sys.exit(0)

    def vm_parse(self):
        self.vm = pd.DataFrame(columns=vmc.datacol)
        self.read()
        self.plan_net_check()
        drop = [item for item in self.vm.columns.values.tolist()
                if (item[0] == '|')]
        self.vm = utl.col_removal(self.vm, 'vm', drop)
        plan_row = (self.vm.loc[self.vm[vmc.vendorkey] == plan_key])
        self.vm = self.vm[self.vm[vmc.vendorkey] != plan_key]
        self.vm = self.vm.append(plan_row).reset_index()
        self.vm = utl.data_to_type(self.vm, [], vmc.datecol, vmc.barsplitcol)
        self.vl = self.vm[vmc.vendorkey].tolist()
        self.vm = self.vm.set_index(vmc.vendorkey).to_dict()
        for col in vmc.barsplitcol:
            self.vm[col] = ({key: list(value.split('|')) for key, value in
                            self.vm[col].items()})

    def vm_import_keys(self):
        for vk in self.vl:
            vk_split = {vk: vk.split('_')}
            if vk_split[vk][0] == 'API':
                if vk_split[vk][1] == 'Adwords':
                    self.api_aw_key.append(vk)
                if vk_split[vk][1] == 'Facebook':
                    self.api_fb_key.append(vk)
                if vk_split[vk][1] == 'Twitter':
                    self.api_tw_key.append(vk)
                if vk_split[vk][1] == 'TTD':
                    self.api_ttd_key.append(vk)
                if vk_split[vk][1] == 'GA':
                    self.api_ga_key.append(vk)
                if vk_split[vk][1] == 'Netbase':
                    self.api_nb_key.append(vk)
                if vk_split[vk][1] == 'AppsFlyer':
                    self.api_af_key.append(vk)
            if vk_split[vk][0] == 'FTP':
                if vk_split[vk][1] == 'Sizmek':
                    self.ftp_sz_key.append(vk)
            if vk_split[vk][0] == 'DB':
                if vk_split[vk][1] == 'DNA':
                    self.db_dna_key.append(vk)
            if vk_split[vk][0] == 'S3':
                if vk_split[vk][1] == 'DNA':
                    self.s3_dna_key.append(vk)

    def vm_rules(self):
        for key in self.vm:
            key_split = key.split('_')
            if key_split[0] == 'RULE':
                if key_split[1] in self.vm_rules_dict:
                    (self.vm_rules_dict[key_split[1]].
                        update({key_split[2]: key}))
                else:
                    self.vm_rules_dict[key_split[1]] = {key_split[2]: key}

    def vendor_set(self, vk):
        ven_param = {}
        for key in self.vm:
            value = self.vm[key][vk]
            ven_param[key] = value
        return ven_param

    def vendor_check(self, vk):
        if (os.path.isfile(vmc.pathraw + self.vm[vmc.filename][vk]) or
           vk == plan_key):
            return True
        else:
            return False

    def vm_change(self, vk, col, newvalue):
        self.vm[col][vk] = newvalue

    def vendor_get(self, vk):
        self.ven_param = self.vendor_set(vk)
        logging.info('Initializing ' + vk)
        if vk == plan_key:
            self.tdf = import_plan_data(vk, self.df, self.plan_omit_list,
                                        **self.ven_param)
        else:
            self.tdf = import_data(vk, self.vm_rules_dict, **self.ven_param)
        return self.tdf

    def vm_loop(self):
        logging.info('Initializing Vendor Matrix Loop')
        self.df = pd.DataFrame(columns=[vmc.datacol])
        for vk in self.vl:
            if self.vendor_check(vk):
                self.tdf = self.vendor_get(vk)
                self.df = self.df.append(self.tdf, ignore_index=True)
        self.df = full_placement_creation(self.df, plan_key, dctc.PFPN,
                                          self.vm[vmc.fullplacename][plan_key])
        if not os.listdir(er.csvpath):
            if os.path.isdir(er.csvpath):
                logging.info('All placements defined.  Deleting Error report' +
                             ' directory.')
                os.rmdir(er.csvpath)
        return self.df


def full_placement_creation(df, key, full_col, full_place_cols):
    logging.debug('Creating Full Placement Name')
    df[full_col] = ''
    i = 0
    for col in full_place_cols:
        if col not in df:
            logging.warning(col + ' was not in ' + key + '.  It was not ' +
                            'included in Full Placement Name.  For reference '
                            'column names are as follows: \n' +
                            str(df.columns.values.tolist()))
            continue
        df[col] = df[col].astype(str)
        if i == 0:
            df[full_col] = df[col]
        else:
            df[full_col] = (df[full_col] + '_' + df[col])
        i += 1
    return df


def combining_data(df, key, columns, **kwargs):
    logging.debug('Combining Data.')
    for col in columns:
        if col not in df.columns:
            df[col] = 0
        for item in kwargs[col]:
            if str(item) == 'nan':
                continue
            if item not in df:
                logging.warning(item + ' is not in ' + key +
                                '.  It was not put in ' + col)
                continue
            if col == item:
                continue
            if col in vmc.datafloatcol:
                df = utl.data_to_type(df, float_col=[col, item])
                df[col] += df[item]
            else:
                df[col] = df[item]
    return df


def ad_cost_calculation(df):
    if (vmc.impressions not in df) or (vmc.clicks not in df):
        return df
    df[vmc.AD_COST] = np.where(df[dctc.AM] == vmc.AM_CPM,
                               df[dctc.AR] * df[vmc.impressions] / 1000,
                               df[dctc.AR] * df[vmc.clicks])
    return df


def import_data(key, vm_rules, **kwargs):
    df = utl.import_read_csv(vmc.pathraw, kwargs[vmc.filename])
    df = utl.add_header(df, kwargs[vmc.header], kwargs[vmc.firstrow])
    df = utl.first_last_adj(df, kwargs[vmc.firstrow], kwargs[vmc.lastrow])
    df = utl.df_transform(df, kwargs[vmc.transform])
    df = full_placement_creation(df, key, dctc.FPN, kwargs[vmc.fullplacename])
    dic = dct.Dict(kwargs[vmc.filenamedict])
    err = er.ErrorReport(df, dic, kwargs[vmc.placement],
                         kwargs[vmc.filenameerror])
    dic.auto(err, kwargs[vmc.autodicord], kwargs[vmc.autodicplace])
    dic.apply_constants()
    dic.apply_translation()
    dic.apply_relation()
    df = dic.merge(df, dctc.FPN)
    df = combining_data(df, key, vmc.datadatecol, **kwargs)
    df = utl.data_to_type(df, date_col=vmc.datadatecol)
    df = utl.apply_rules(df, vm_rules, utl.PRE, **kwargs)
    df = combining_data(df, key, vmc.datafloatcol, **kwargs)
    df = utl.data_to_type(df, vmc.datafloatcol, vmc.datadatecol)
    df = utl.date_removal(df, vmc.date, kwargs[vmc.startdate],
                          kwargs[vmc.enddate])
    df = ad_cost_calculation(df)
    df = utl.col_removal(df, key, kwargs[vmc.dropcol])
    df = utl.apply_rules(df, vm_rules, utl.POST, **kwargs)
    df[vmc.vendorkey] = key
    return df


def import_plan_data(key, df, plan_omit_list, **kwargs):
    df = df.loc[~df[vmc.vendorkey].isin(plan_omit_list)]
    df = df.loc[:, kwargs[vmc.fullplacename]]
    df = full_placement_creation(df, key, dctc.FPN, kwargs[vmc.fullplacename])
    df = df.drop_duplicates()
    dic = dct.Dict(kwargs[vmc.filenamedict])
    df_fpn = pd.DataFrame(df[dctc.FPN])
    er.ErrorReport(df_fpn, dic, None, kwargs[vmc.filenameerror])
    merge_col = list(set(dic.data_dict.columns).intersection(df.columns))
    dic.data_dict = dic.data_dict.merge(df, on=merge_col, how='left')
    dic.apply_constants()
    dic.apply_translation()
    dic.apply_relation()
    dic.data_dict = utl.data_to_type(dic.data_dict, date_col=vmc.datadatecol)
    return dic.data_dict


def vm_update_rule_check(vm, vm_col):
    vm[vm_col] = vm[vm_col].astype(str)
    vm[vm_col] = np.where(
                vm[vm_col].str.contains('|'.join(['PRE::', 'POST::', 'nan'])),
                vm[vm_col],
                'POST::' + (vm[vm_col]).astype(str))
    return vm


def vm_update(old_path='Config/', old_file='OldVendorMatrix.csv'):
    logging.info('Updating Vendor Matrix')
    ovm = utl.import_read_csv(path=old_path, filename=old_file)
    rules = [col for col in ovm.columns if 'RULE_' in col]
    rule_metrics = [col for col in ovm.columns if '_METRIC' in col]
    nvm = pd.DataFrame(columns=[vmc.vendorkey] + vmc.vmkeys)
    vm = nvm.append(ovm)
    for col in [vmc.fullplacename, vmc.dropcol, vmc.autodicord]:
        vm[col] = vm[col].astype(str).replace({'_': '|'}, regex=True)
    if 'FIRSTROWADJ' in vm.columns:
        vm[vmc.firstrow] = np.where(vm['FIRSTROWADJ'] == True,
                                    vm[vmc.firstrow] + 1, vm[vmc.firstrow])
    if vmc.autodicplace not in ovm.columns:
        vm[vmc.autodicplace] = vmc.fullplacename
    vm = utl.col_removal(vm, 'vm',
                         ['FIRSTROWADJ', 'LASTROWADJ', 'AUTO DICTIONARY'])
    vm = vm.reindex_axis([vmc.vendorkey] + vmc.vmkeys + rules, axis=1)
    for col in rule_metrics:
        vm = vm_update_rule_check(vm, col)
    vm = vm.fillna('')
    vm = vm.replace('nan', '')
    vm.to_csv(csv, index=False)
