import os
import sys
import shutil
import logging
import numpy as np
import pandas as pd
import reporting.utils as utl
import reporting.vmcolumns as vmc
import reporting.dictionary as dct
import reporting.errorreport as er
import reporting.dictcolumns as dctc

log = logging.getLogger()

csv_path = utl.config_path
csv_file = 'Vendormatrix.csv'
csv_full_file = os.path.join(csv_path, csv_file)
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
        self.api_sc_key = []
        self.api_aj_key = []
        self.api_dc_key = []
        self.api_rs_key = []
        self.api_db_key = []
        self.api_vk_key = []
        self.api_rc_key = []
        self.api_szk_key = []
        self.api_red_key = []
        self.ftp_sz_key = []
        self.db_dna_key = []
        self.s3_dna_key = []
        self.vm_rules_dict = {}
        self.ven_param = None
        self.plan_omit_list = None
        self.process_omit_list = None
        self.tdf = None
        self.df = None
        self.vm_parse()
        self.vm_import_keys()
        self.vm_rules()
        self.make_omit_lists()

    def read(self):
        if not os.path.isfile(os.path.join(csv_path, csv_file)):
            logging.info('Creating Vendor Matrix.  Populate it and run again')
            vm = pd.DataFrame(columns=[vmc.vendorkey] + vmc.vmkeys, index=None)
            vm.to_csv(csv_full_file, index=False, encoding='utf-8')
        self.vm = utl.import_read_csv(csv_file, csv_path)

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
                if vk_split[vk][1] == 'Snapchat':
                    self.api_sc_key.append(vk)
                if vk_split[vk][1] == 'Adjust':
                    self.api_aj_key.append(vk)
                if vk_split[vk][1] == 'DCM':
                    self.api_dc_key.append(vk)
                if vk_split[vk][1] == 'Redshell':
                    self.api_rs_key.append(vk)
                if vk_split[vk][1] == 'DBM':
                    self.api_db_key.append(vk)
                if vk_split[vk][1] == 'VK':
                    self.api_vk_key.append(vk)
                if vk_split[vk][1] == 'Revcontent':
                    self.api_rc_key.append(vk)
                if vk_split[vk][1] == 'Sizmek':
                    self.api_szk_key.append(vk)
                if vk_split[vk][1] == 'Reddit':
                    self.api_red_key.append(vk)
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

    def make_omit_lists(self):
        self.plan_omit_list = [k for k, v in self.vm[vmc.omit_plan].items()
                               if str(v) == 'PLAN']
        self.process_omit_list = [k for k, v in self.vm[vmc.omit_plan].items()
                                  if str(v) == 'ALL']

    def vendor_set(self, vk):
        ven_param = {x: self.vm[x][vk] for x in self.vm}
        return ven_param

    def vm_change(self, vk, col, newvalue):
        self.vm[col][vk] = newvalue

    def vendor_get(self, vk):
        self.ven_param = self.vendor_set(vk)
        logging.info('Initializing {}'.format(vk))
        if vk == plan_key:
            self.tdf = import_plan_data(vk, self.df, self.plan_omit_list,
                                        **self.ven_param)
        else:
            self.tdf = import_data(vk, self.vm_rules_dict, **self.ven_param)
        return self.tdf

    def set_full_filename(self):
        self.vm[vmc.filename] = {x: self.vm[vmc.filename][x]
                                 if '/' in self.vm[vmc.filename][x]
                                 else os.path.join(utl.raw_path,
                                                   self.vm[vmc.filename][x])
                                 for x in self.vm[vmc.filename]}

    def sort_vendor_list(self):
        self.set_full_filename()
        self.vl = sorted((x for x in self.vl if x not in self.process_omit_list
                          and os.path.isfile(self.vm[vmc.filename][x])),
                         key=lambda x: os.stat(self.vm[vmc.filename][x]))
        self.vl.append(plan_key)

    def vm_loop(self):
        logging.info('Initializing Vendor Matrix Loop')
        self.df = pd.DataFrame()
        self.sort_vendor_list()
        for vk in self.vl:
            self.tdf = self.vendor_get(vk)
            self.df = self.df.append(self.tdf, ignore_index=True, sort=True)
        self.df = full_placement_creation(self.df, plan_key, dctc.PFPN,
                                          self.vm[vmc.fullplacename][plan_key])
        if not os.listdir(er.csvpath):
            if os.path.isdir(er.csvpath):
                logging.info('All placements defined.  Deleting Error report'
                             ' directory.')
                os.rmdir(er.csvpath)
        self.df = utl.data_to_type(self.df, vmc.datafloatcol, vmc.datadatecol)
        return self.df


def full_placement_creation(df, key, full_col, full_place_cols):
    logging.debug('Creating Full Placement Name')
    df[full_col] = ''
    df = utl.data_to_type(df, str_col=full_place_cols)
    for idx, col in enumerate(full_place_cols):
        if col[:2] == '::':
            col = col[2:]
            df[col] = df[col].replace({'_': ''}, regex=True)
        if col not in df:
            logging.warning('{} was not in {}.  It was not included in '
                            'Full Placement Name.  For reference column names'
                            ' are as follows: \n {}'
                            .format(col, key, df.columns.values.tolist()))
            continue
        if idx == 0:
            df[full_col] = df[col]
        else:
            df[full_col] = (df[full_col] + '_' + df[col])
    return df


def combining_data(df, key, columns, **kwargs):
    logging.debug('Combining Data.')
    for col in columns:
        if col in df.columns and col not in kwargs[col]:
            df[col] = 0
        for item in kwargs[col]:
            if str(item) == 'nan' and col == vmc.date:
                df[vmc.date] = 0
            elif str(item) == 'nan' or col == item:
                continue
            if item not in df:
                logging.warning('{} is not in {}.  It was not'
                                'put in {}'.format(item, key, col))
                continue
            if col not in df.columns:
                df[col] = 0
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
    df[vmc.REP_COST] = np.where(df[dctc.RFM] == vmc.AM_CPM,
                                df[dctc.RFR] * df[vmc.impressions] / 1000,
                                df[dctc.RFR] * df[vmc.clicks])
    df[vmc.VER_COST] = np.where(df[dctc.VFM] == vmc.AM_CPM,
                                df[dctc.VFR] * df[vmc.impressions] / 1000,
                                df[dctc.VFR] * df[vmc.clicks])
    return df


def import_data(key, vm_rules, **kwargs):
    df = utl.import_read_csv(kwargs[vmc.filename])
    if df is None or df.empty:
        return df
    df = utl.add_header(df, kwargs[vmc.header], kwargs[vmc.firstrow])
    df = utl.first_last_adj(df, kwargs[vmc.firstrow], kwargs[vmc.lastrow])
    df = df_transform(df, kwargs[vmc.transform])
    df = full_placement_creation(df, key, dctc.FPN, kwargs[vmc.fullplacename])
    dic = dct.Dict(kwargs[vmc.filenamedict])
    err = er.ErrorReport(df, dic, kwargs[vmc.placement],
                         kwargs[vmc.filenameerror])
    dic.auto_functions(err, kwargs[vmc.autodicord], kwargs[vmc.autodicplace])
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
    dic.data_dict = utl.data_to_type(dic.data_dict, str_col=merge_col)
    dic.data_dict = dic.data_dict.merge(df, on=merge_col, how='left')
    dic.apply_functions()
    dic.data_dict = utl.data_to_type(dic.data_dict, date_col=vmc.datadatecol)
    return dic.data_dict


def vm_update_rule_check(vm, vm_col):
    vm[vm_col] = vm[vm_col].astype('U')
    vm[vm_col] = np.where(
                vm[vm_col].str.contains('|'.join(['PRE::', 'POST::', 'nan'])),
                vm[vm_col],
                'POST::' + (vm[vm_col]).astype('U'))
    return vm


def df_transform(df, transform):
    if str(transform) == 'nan':
        return df
    transform = transform.split('::')
    transform_type = transform[0]
    if transform_type == 'MixedDateColumn':
        mixed_col = transform[1]
        date_col = transform[2]
        df[date_col] = df[mixed_col]
        df = utl.data_to_type(df, date_col=[date_col])
        df['temp'] = df[date_col]
        df[date_col] = df[date_col].fillna(method='ffill')
        df = df[df['temp'].isnull()].reset_index(drop=True)
        df.drop('temp', axis=1, inplace=True)
    if transform_type == 'Pivot':
        pivot_col = transform[1]
        val_col = transform[2].split('|')
        df = df.fillna(0)
        index_cols = [x for x in df.columns if x not in val_col + [pivot_col]]
        df = pd.pivot_table(df, index=index_cols, columns=[pivot_col],
                            aggfunc='sum')
        if len(val_col) != 1:
            df.columns = df.columns.map('_'.join)
        df = df.reset_index()
    if transform_type == 'Merge':
        merge_file = transform[1]
        left_merge = transform[2]
        right_merge = transform[3]
        merge_df = pd.read_csv(merge_file)
        dfs = {left_merge: df, right_merge: merge_df}
        for col in dfs:
            if dfs[col][col].dtype == 'float64':
                dfs[col][col] = dfs[col][col].fillna(0).astype('int')
            dfs[col][col] = dfs[col][col].astype('U')
            dfs[col][col] = dfs[col][col].str.strip('.0')
        filename = 'Merge-{}-{}.csv'.format(left_merge, right_merge)
        err = er.ErrorReport(df, merge_df, None, filename,
                             merge_col=[left_merge, right_merge])
        df = err.merge_df
        df = df.drop('_merge', axis=1)
    if transform_type == 'DateSplit':
        start_date = transform[1]
        end_date = transform[2]
        if len(transform) == 4:
            exempt_col = transform[3].split('|')
        else:
            exempt_col = []
        df = utl.data_to_type(df, date_col=[end_date, start_date])
        df['days'] = (df[end_date] - df[start_date]).dt.days + 1
        n_cols = [x for x in df.columns if df[x].dtype in ['int64', 'float64']
                  and x not in exempt_col + ['days']]
        df[n_cols] = df[n_cols].div(df['days'], axis=0)
        df = df.loc[df.index.repeat(df['days'])]
        df[start_date] = (df.groupby(level=0)[start_date].transform(
            lambda x: pd.date_range(start=x.iat[0], periods=len(x))))
        df = df.drop('days', axis=1)
        df = df.reset_index(drop=True)  # type: pd.DataFrame
    if transform_type == 'Stack':
        header_col_name = transform[1]
        hold_col_name = transform[2]
        df.columns = [df.columns[idx - 1] if 'Unnamed' in x else x
                      for idx, x in enumerate(df.columns)]
        hdf = pd.DataFrame(df[hold_col_name])
        ndf = pd.DataFrame()
        for x in set(y for y in df.columns if y != hold_col_name):
            tdf = df[x]
            tdf.columns = tdf.loc[0]
            tdf = tdf.iloc[1:]
            tdf[header_col_name] = x
            ndf = ndf.append(tdf)
        df = pd.concat([ndf, hdf], axis=1, join='inner')
        df = df.reset_index(drop=True)  # type: pd.DataFrame
    return df


def vm_update(old_path=utl.config_path, old_file='OldVendorMatrix.csv'):
    logging.info('Updating Vendor Matrix')
    shutil.copyfile(csv_full_file, os.path.join(old_path, old_file))
    ovm = utl.import_read_csv(filename=old_file, path=old_path)
    rules = [col for col in ovm.columns if 'RULE_' in col]
    rule_metrics = [col for col in ovm.columns if '_METRIC' in col]
    nvm = pd.DataFrame(columns=[vmc.vendorkey] + vmc.vmkeys)
    vm = nvm.append(ovm, sort=True)
    if 'FIRSTROWADJ' in vm.columns:
        vm[vmc.firstrow] = np.where(vm['FIRSTROWADJ'],
                                    vm[vmc.firstrow] + 1, vm[vmc.firstrow])
    if vmc.autodicplace not in ovm.columns:
        vm[vmc.autodicplace] = vmc.fullplacename
    vm = utl.col_removal(vm, 'vm',
                         ['FIRSTROWADJ', 'LASTROWADJ', 'AUTO DICTIONARY'],
                         warn=False)
    vm = vm.reindex([vmc.vendorkey] + vmc.vmkeys + rules, axis=1)
    for col in rule_metrics:
        vm = vm_update_rule_check(vm, col)
    vm = vm.fillna('')
    vm = vm.replace('nan', '')
    vm.to_csv(csv_full_file, index=False, encoding='utf-8')
