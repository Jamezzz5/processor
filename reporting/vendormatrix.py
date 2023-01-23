import os
import sys
import json
import yaml
import urllib
import shutil
import logging
import numpy as np
import pandas as pd
import datetime as dt
import reporting.utils as utl
import reporting.calc as cal
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
    def __init__(self, display_log=True):
        if display_log:
            log.info('Initializing Vendor Matrix')
        utl.dir_check(csv_path)
        self.vm = None
        self.vm_df = pd.DataFrame()
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
        self.api_dv_key = []
        self.api_adk_key = []
        self.api_inn_key = []
        self.api_tik_key = []
        self.api_amz_key = []
        self.api_cri_key = []
        self.api_pm_key = []
        self.api_sam_key = []
        self.api_gs_key = []
        self.api_qt_key = []
        self.api_yv_key = []
        self.api_amd_key = []
        self.api_ss_key = []
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
        self.sort_vendor_list()

    @staticmethod
    def read():
        if not os.path.isfile(os.path.join(csv_path, csv_file)):
            logging.info('Creating Vendor Matrix.  Populate it and run again')
            vm = pd.DataFrame(columns=[vmc.vendorkey] + vmc.vmkeys, index=None)
            vm.to_csv(csv_full_file, index=False, encoding='utf-8')
        vm = utl.import_read_csv(csv_file, csv_path)
        return vm

    def write(self):
        logging.info('Writing vendormatrix to {}.'.format(csv_full_file))
        rules = [x for x in self.vm_df.columns if 'RULE' in x]
        cols = [vmc.vendorkey] + vmc.vmkeys + rules
        self.vm_df[cols].to_csv(csv_full_file, index=False, encoding='utf-8')

    def plan_net_check(self):
        if not self.vm['Vendor Key'].isin(['Plan Net']).any():
            logging.warning('No Plan Net key in Vendor Matrix.  Add it.')
            return False

    def add_file_name_col(self):
        self.vm_df[vmc.filename_true] = self.vm_df[vmc.filename].str.split(
            utl.sheet_name_splitter).str[0]
        return self.vm_df

    def vm_parse(self, df=pd.DataFrame()):
        if not df.empty:
            self.vm_df = df
        else:
            self.vm_df = pd.DataFrame(columns=vmc.datacol)
            self.vm_df = self.read()
        self.vm_df = self.add_file_name_col()
        self.vm = self.vm_df.copy()
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
                if vk_split[vk][1] == vmc.api_aw_key:
                    self.api_aw_key.append(vk)
                if vk_split[vk][1] == vmc.api_fb_key:
                    self.api_fb_key.append(vk)
                if vk_split[vk][1] == vmc.api_tw_key:
                    self.api_tw_key.append(vk)
                if vk_split[vk][1] == vmc.api_ttd_key:
                    self.api_ttd_key.append(vk)
                if vk_split[vk][1] == vmc.api_ga_key:
                    self.api_ga_key.append(vk)
                if vk_split[vk][1] == vmc.api_nb_key:
                    self.api_nb_key.append(vk)
                if vk_split[vk][1] == vmc.api_af_key:
                    self.api_af_key.append(vk)
                if vk_split[vk][1] == vmc.api_sc_key:
                    self.api_sc_key.append(vk)
                if vk_split[vk][1] == vmc.api_aj_key:
                    self.api_aj_key.append(vk)
                if vk_split[vk][1] == vmc.api_dc_key:
                    self.api_dc_key.append(vk)
                if vk_split[vk][1] == vmc.api_rs_key:
                    self.api_rs_key.append(vk)
                if vk_split[vk][1] == vmc.api_db_key:
                    self.api_db_key.append(vk)
                if vk_split[vk][1] == vmc.api_vk_key:
                    self.api_vk_key.append(vk)
                if vk_split[vk][1] == vmc.api_rc_key:
                    self.api_rc_key.append(vk)
                if vk_split[vk][1] == vmc.api_szk_key:
                    self.api_szk_key.append(vk)
                if vk_split[vk][1] == vmc.api_red_key:
                    self.api_red_key.append(vk)
                if vk_split[vk][1] == vmc.api_dv_key:
                    self.api_dv_key.append(vk)
                if vk_split[vk][1] == vmc.api_adk_key:
                    self.api_adk_key.append(vk)
                if vk_split[vk][1] == vmc.api_inn_key:
                    self.api_inn_key.append(vk)
                if vk_split[vk][1] == vmc.api_tik_key:
                    self.api_tik_key.append(vk)
                if vk_split[vk][1] == vmc.api_amz_key:
                    self.api_amz_key.append(vk)
                if vk_split[vk][1] == vmc.api_cri_key:
                    self.api_cri_key.append(vk)
                if vk_split[vk][1] == vmc.api_pm_key:
                    self.api_pm_key.append(vk)
                if vk_split[vk][1] == vmc.api_sam_key:
                    self.api_sam_key.append(vk)
                if vk_split[vk][1] == vmc.api_gs_key:
                    self.api_gs_key.append(vk)
                if vk_split[vk][1] == vmc.api_qt_key:
                    self.api_qt_key.append(vk)
                if vk_split[vk][1] == vmc.api_yv_key:
                    self.api_yv_key.append(vk)
                if vk_split[vk][1] == vmc.api_amd_key:
                    self.api_amd_key.append(vk)
                if vk_split[vk][1] == vmc.api_ss_key:
                    self.api_ss_key.append(vk)
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
        ven_param = {x: self.vm[x][vk] for x in self.vm if vk in self.vm[x]}
        return ven_param

    def vm_change_on_key(self, vk, col, new_value):
        idx = self.vm_df[self.vm_df[vmc.vendorkey] == vk].index
        self.vm_change(idx, col, new_value)

    def vm_change(self, index, col, new_value):
        self.vm_df.loc[index, col] = new_value

    def get_all_data_sources(self, default_param=None):
        data_sources = self.get_import_data_sources(default_param=default_param)
        non_import = self.get_data_sources()
        non_import = [x for x in non_import if x.key not in
                      [y.key for y in data_sources]]
        data_sources.extend(non_import)
        return data_sources

    def set_data_sources(self, data_sources):
        for source in data_sources:
            vendor_key = source['original_vendor_key']
            logging.info('Setting datasource for {}.'.format(vendor_key))
            index = self.vm_df[self.vm_df[vmc.vendorkey] == vendor_key].index[0]
            for col in [vmc.autodicplace, vmc.placement, vmc.vendorkey]:
                self.vm_change(index, col, source[col])
            for col in [vmc.autodicord, vmc.fullplacename]:
                new_value = '|'.join(str(x) for x in source[col].split('\r\n'))
                self.vm_change(index, col, new_value)
            active_metric_cols = list(source['active_metrics'].keys())
            for col in vmc.datacol:
                if col in active_metric_cols:
                    new_value = '|'.join(str(x)
                                         for x in source['active_metrics'][col])
                else:
                    new_value = ''
                self.vm_change(index, col, new_value)
        self.write()

    def get_import_data_sources(self, import_type='API_', default_param=None):
        ic = ImportConfig(matrix=self, default_param_ic=default_param)
        current_imports = ic.get_current_imports(import_type, matrix=self)
        vendor_keys = ['{}{}_{}'.format(import_type, x['Key'], x['name'])
                       if x['name'] else '{}{}'.format(import_type, x['Key'])
                       for x in current_imports]
        data_sources = [self.get_data_source(vk) for vk in vendor_keys]
        for ds in data_sources:
            ds.add_import_config_params(import_type, self, ic)
        return data_sources

    def get_data_sources(self):
        self.sort_vendor_list()
        return [self.get_data_source(vk) for vk in self.vl]

    def get_data_source(self, vk):
        try:
            self.ven_param = self.vendor_set(vk)
        except KeyError:
            self.ven_param = self.vendor_set('{}_'.format(vk))
        ds = DataSource(vk, self.vm_rules_dict, **self.ven_param)
        return ds

    def vendor_get(self, vk):
        self.ven_param = self.vendor_set(vk)
        logging.info('Initializing {}'.format(vk))
        if vk == plan_key:
            self.tdf = import_plan_data(vk, self.df, self.plan_omit_list,
                                        **self.ven_param)
        else:
            ds = DataSource(vk, self.vm_rules_dict, **self.ven_param)
            self.tdf = ds.import_data()
        return self.tdf

    def set_full_filename(self):
        for col in [vmc.filename, vmc.filename_true]:
            self.vm[col] = {
                x: self.vm[col][x] if '/' in self.vm[col][x]
                else os.path.join(utl.raw_path, self.vm[col][x])
                for x in self.vm[col]}

    def sort_vendor_list(self):
        self.set_full_filename()
        self.vl = self.vm_df[vmc.vendorkey].to_list()
        self.vl = sorted(
            (x for x in self.vl
             if x not in self.process_omit_list
             and os.path.isfile(self.vm[vmc.filename][x].
                                split(utl.sheet_name_splitter)[0])),
            key=lambda x: os.path.getsize(self.vm[vmc.filename][x].
                                          split(utl.sheet_name_splitter)[0]))
        self.vl.append(plan_key)

    def vm_loop(self):
        logging.info('Initializing Vendor Matrix Loop')
        self.df = pd.DataFrame(columns=[vmc.date, dctc.FPN, dctc.PN, dctc.BM])
        self.sort_vendor_list()
        for vk in self.vl:
            self.tdf = self.vendor_get(vk)
            self.df = self.df.append(self.tdf, ignore_index=True, sort=True)
        self.df = full_placement_creation(self.df, plan_key, dctc.PFPN,
                                          self.vm[vmc.fullplacename][plan_key])
        if not os.listdir(er.csv_path):
            if os.path.isdir(er.csv_path):
                logging.info('All placements defined.  Deleting Error report'
                             ' directory.')
                os.rmdir(er.csv_path)
        self.df = utl.data_to_type(self.df, vmc.datafloatcol, vmc.datadatecol)
        return self.df

    @staticmethod
    def write_output_data(df, output_file):
        try:
            logging.info('Writing to: {}'.format(output_file))
            df.to_csv(output_file, index=False, encoding='utf-8')
            logging.info('Final Output Successfully generated')
        except IOError:
            logging.warning('{} could not be opened.  '
                            'Final Output not updated.'.format(output_file))

    def vm_loop_with_costs(self, output_file):
        df = self.vm_loop()
        df = cal.calculate_cost(df)
        self.write_output_data(df, output_file)
        return df


class ImportConfig(object):
    key = 'Key'
    config_file = vmc.apifile
    account_id = 'ID'
    filter = 'Filter'
    account_id_parent = 'ID Parent'
    account_id_pre = 'ID Pre'
    file_name = 'import_config.csv'
    file_path = utl.config_path

    def __init__(self, matrix=None, default_param_ic=None, base_path=None):
        self.matrix = None
        self.df = None
        self.matrix_df = None
        self.base_path = base_path
        self.default_param_ic = default_param_ic
        if matrix:
            self.import_vm()
        if not self.default_param_ic:
            self.default_param_ic = self

    def import_vm(self):
        self.matrix = VendorMatrix(display_log=False)
        self.matrix_df = self.matrix.read()
        self.df = self.read()

    def read(self):
        df = utl.import_read_csv(self.file_name, self.file_path)
        return df

    def get_default_params(self, import_key, default_param=False):
        if default_param:
            df = self.default_param_ic.df.copy()
        else:
            df = self.df.copy()
        params = df[df[self.key] == import_key].copy()
        params = params.reset_index(drop=True)
        params = params.to_dict(orient='index')
        if len(params) > 0:
            params = params[0]
        elif not default_param:
            logging.debug('Param not in vm, using default values.')
            params = self.get_default_params(import_key, default_param=True)
        return params

    @staticmethod
    def append_str_before_filetype(name, append_val):
        if str(name) == 'nan':
            return append_val
        name_list = name.split('.')
        new_name = '{}_{}'.format(name_list[0], append_val)
        if len(name_list) > 1:
            new_name = '{}.{}'.format(new_name, name_list[1])
        return new_name

    def get_new_name(self, search_col, search_val):
        file_list = sorted([x for x in self.matrix_df[search_col].unique()
                            if search_val in str(x)])
        if len(file_list) > 0:
            append_val = len(file_list)
            file_name = self.append_str_before_filetype(search_val, append_val)
        else:
            file_name = search_val
        return file_name

    @staticmethod
    def get_config_file_value(config_file, name, nest=None):
        if not pd.isna(nest) and name in config_file[nest]:
            value = config_file[nest][name]
        elif name not in config_file:
            value = ''
        else:
            value = config_file[name]
        return value

    @staticmethod
    def set_config_file_value(config_file, name, new_val, nest=None):
        if not pd.isna(nest):
            config_file[nest][name] = new_val
        else:
            config_file[name] = new_val
        return config_file

    def load_file(self, file_name, file_library):
        file_name = os.path.join(self.file_path, file_name)
        if not os.path.exists(file_name):
            if self.base_path:
                file_name = os.path.join(self.base_path, 'processor', file_name)
                if not os.path.exists(file_name):
                    return None
            else:
                return None
        with open(file_name, 'r') as f:
            config_file = file_library.load(f)
        return config_file

    def make_new_json(self, params, new_file, account_id, import_filter=None,
                      file_library=json):
        config_file = self.load_file(params[self.config_file], file_library)
        if not pd.isna(params[self.account_id_pre]):
            account_id = '{}{}'.format(params[self.account_id_pre], account_id)
        config_file = self.set_config_file_value(
            config_file=config_file, name=params[self.account_id],
            new_val=account_id, nest=params[self.account_id_parent])
        if not pd.isna(import_filter):
            config_file = self.set_config_file_value(
                config_file=config_file, name=params[self.filter],
                new_val=import_filter, nest=params[self.account_id_parent])
        new_file = os.path.join(self.file_path, new_file)
        with open(new_file, 'w') as f:
            file_library.dump(config_file, f)

    @staticmethod
    def set_config_file_lib(file_name):
        file_type = file_name.split('.')[1]
        if file_type == 'json':
            f_lib = json
        elif file_type == 'yaml':
            f_lib = yaml
        else:
            f_lib = json
        return f_lib

    def make_new_config(self, params, new_file, account_id, import_filter=None):
        f_lib = self.set_config_file_lib(params[self.config_file])
        self.make_new_json(params, new_file, account_id, import_filter, f_lib)

    def set_new_value(self, df, col_name, key_name):
        new_name = self.append_str_before_filetype(df[col_name][0], key_name)
        new_name = self.get_new_name(col_name, new_name)
        df[col_name] = new_name
        return df

    def get_default_vm_value(self, import_key, import_type,
                             default_param=False):
        if default_param:
            df = self.default_param_ic.matrix_df.copy()
        else:
            df = self.matrix_df.copy()
        original_keys = [import_key, '{}_{}'.format(import_type, import_key)]
        df = df[df[vmc.vendorkey].isin(original_keys)].copy()
        df = df.reset_index(drop=True)
        df = df.iloc[0:]
        if df.empty and not default_param:
            df = self.get_default_vm_value(import_key, import_type, True)
        return df

    def add_to_vm(self, import_key, new_file, start_date, api_fields,
                  key_name='', import_type='API'):
        df = self.get_default_vm_value(import_key, import_type)
        for col in [vmc.vendorkey, vmc.filename, vmc.filenamedict]:
            df = self.set_new_value(df, col, key_name)
        df[vmc.vendorkey] = '{}_{}'.format(import_type, df[vmc.vendorkey][0])
        df[vmc.apifile] = new_file
        df[vmc.startdate] = start_date
        if api_fields:
            df[vmc.apifields] = api_fields
        self.matrix_df = self.matrix_df.append(df, ignore_index=True,
                                               sort=False)
        return df[vmc.vendorkey][0]

    def add_import_to_vm(self, import_key, account_id, import_filter=None,
                         start_date=None, api_fields=None, key_name=''):
        params = self.get_default_params(import_key)
        search_name = self.append_str_before_filetype(
            params[self.config_file], key_name)
        file_name = self.get_new_name(search_col=vmc.apifile,
                                      search_val=search_name)
        if account_id:
            self.make_new_config(params, file_name, account_id, import_filter)
        vk = self.add_to_vm(import_key, file_name, start_date, api_fields,
                            key_name)
        return vk

    def add_and_remove_from_vm(self, import_dicts, matrix=None):
        current_imports = self.get_current_imports(matrix=matrix)
        for cur_import in current_imports:
            if cur_import not in import_dicts:
                if (vmc.vendorkey in cur_import and (
                        cur_import[vmc.vendorkey] in
                        [x[vmc.vendorkey] for x in import_dicts])):
                    import_dict = [x for x in import_dicts if x[vmc.vendorkey]
                                   == cur_import[vmc.vendorkey]][0]
                    self.update_import(import_dict, cur_import)
                else:
                    key_name = 'API_{}_{}'.format(cur_import[self.key],
                                                  cur_import['name'])
                    drop_idx = self.matrix_df[self.matrix_df[vmc.vendorkey] ==
                                              key_name].copy()
                    drop_idx = drop_idx.index.values[0]
                    self.matrix_df = self.matrix_df.drop(drop_idx)
                    self.matrix_df.reset_index()
        self.add_imports_to_vm(import_dicts)

    def add_imports_to_vm(self, import_dicts):
        vks = []
        for import_dict in import_dicts:
            current_imports = self.get_current_imports()
            if import_dict in current_imports:
                continue
            import_key = import_dict[self.key]
            account_id = import_dict[self.account_id]
            import_filter = import_dict[self.filter]
            start_date = import_dict[vmc.startdate]
            api_fields = import_dict[vmc.apifields]
            key_name = import_dict['name']
            vk = self.add_import_to_vm(import_key, account_id, import_filter,
                                       start_date, api_fields, key_name)
            vks.append(vk)
        self.matrix.vm_df = self.matrix_df
        self.matrix.write()
        return vks

    def update_import(self, import_dict, old_import_dict):
        up_idx = self.matrix_df[self.matrix_df[vmc.vendorkey] ==
                                import_dict[vmc.vendorkey]].copy()
        up_idx = up_idx.index.values[0]
        for col in [vmc.startdate, vmc.apifields]:
            self.matrix_df.loc[up_idx, col] = import_dict[col]
        if import_dict['name'] != old_import_dict['name']:
            for col in [vmc.vendorkey, vmc.filename, vmc.filenamedict]:
                self.matrix_df.loc[up_idx, col] = \
                    self.matrix_df.loc[up_idx, col].replace(
                        old_import_dict['name'], import_dict['name'])
        file_name = self.matrix_df.loc[up_idx, vmc.apifile]
        if not ((import_dict[self.account_id] ==
                 old_import_dict[self.account_id]) and
                (import_dict[self.filter] == old_import_dict[self.filter])):
            params = self.get_default_params(import_dict[self.key])
            if import_dict[self.account_id]:
                self.make_new_config(params, file_name,
                                     import_dict[self.account_id],
                                     import_dict[self.filter])

    def get_datasource(self, api_key):
        df = self.matrix_df[self.matrix_df[vmc.vendorkey] == api_key].copy()
        df = df.reset_index(drop=True)
        df = df.iloc[0]
        return df

    def get_import_params(self, api_key, import_type):
        api_key_split = api_key.split('_')
        api_key_type = api_key_split[1]
        if len(api_key_split) > 2:
            api_key_name = api_key.split('{}{}_'.format(
                import_type, api_key_type))[1]
        else:
            api_key_name = ''
        def_params = self.get_default_params(api_key_type)
        params = self.get_datasource(api_key)
        account_id, filter_val = self.get_import_params_from_config_file(
            params=params, def_params=def_params)
        if pd.isna(params[vmc.apifields]):
            api_fields = ''
        else:
            api_fields = params[vmc.apifields]
        start_date = params[vmc.startdate]
        import_dict = {
            self.key: api_key_type,
            self.account_id: account_id,
            self.filter: filter_val,
            vmc.startdate: start_date,
            vmc.apifields: api_fields,
            'name': api_key_name
        }
        return import_dict

    def get_import_params_from_config_file(self, params, def_params):
        f_lib = self.set_config_file_lib(params[self.config_file])
        config_file = self.load_file(params[self.config_file], f_lib)
        if not config_file:
            return '', ''
        account_id = self.get_config_file_value(
            config_file, def_params[self.account_id],
            def_params[self.account_id_parent])
        if not pd.isna(def_params[self.account_id_pre]):
            account_id = account_id.replace(def_params[self.account_id_pre], '')
        if not pd.isna(def_params[self.filter]):
            filter_val = self.get_config_file_value(
                config_file, def_params[self.filter],
                def_params[self.account_id_parent])
        else:
            filter_val = ''
        return account_id, filter_val

    def get_current_imports(self, import_type='API_', matrix=None):
        if matrix:
            self.import_vm()
        import_dicts = []
        api_keys = [x for x in self.matrix_df[vmc.vendorkey]
                    if x[:4] == import_type]
        for api_key in api_keys:
            import_dict = self.get_import_params(api_key, import_type)
            import_dict[vmc.vendorkey] = api_key
            import_dicts.append(import_dict)
        for cur_import in import_dicts:
            if not isinstance(cur_import[vmc.startdate], dt.date):
                if (str(cur_import[vmc.startdate]) == 'nan'
                        or not cur_import[vmc.startdate]):
                    cur_import[vmc.startdate] = None
                else:
                    start_date = utl.string_to_date(cur_import[vmc.startdate])
                    cur_import[vmc.startdate] = start_date.date()
        return import_dicts


def full_placement_creation(df, key, full_col, full_place_cols):
    logging.debug('Creating Full Placement Name')
    df[full_col] = ''
    df = utl.data_to_type(df, str_col=[x[2:] if x[:2] == '::' else x
                                       for x in full_place_cols])
    for idx, col in enumerate(full_place_cols):
        if col[:2] == '::':
            col = col[2:]
            if col in df.columns:
                df[col] = df[col].str.replace('_', '', regex=True)
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
    combine_cols = [x for x in columns if kwargs[x] != ['nan']]
    for col in combine_cols:
        if col in df.columns and col not in kwargs[col]:
            df[col] = 0
        for item in kwargs[col]:
            if col == item:
                continue
            if item not in df:
                logging.warning('{} is not in {}.  It was not '
                                'put in {}'.format(item, key, col))
                continue
            if col not in df.columns:
                df[col] = 0
            if col in vmc.datafloatcol:
                df = utl.data_to_type(df, float_col=[col, item])
                df[col] += df[item]
            else:
                df[col] = df[item]
    for col in [x for x in columns if x not in combine_cols]:
        if col in df.columns or col == vmc.date:
            df[col] = 0
    return df


def ad_cost_calculation(df):
    if df.empty:
        return df
    df = df.copy()
    for cost_cols in [(vmc.AD_COST, dctc.AM, dctc.AR),
                      (vmc.REP_COST, dctc.RFM, dctc.RFR),
                      (vmc.VER_COST, dctc.VFM, dctc.VFR)]:
        for col in [cost_cols[0], vmc.impressions, vmc.clicks]:
            if col not in df:
                df[col] = 0
        calc_ser = (df[df[cost_cols[1]].isin(cal.BUY_MODELS) &
                       df[cost_cols[2]] != 0].
                    apply(cal.net_cost, cost_col=cost_cols[0],
                          bm_col=cost_cols[1], br_col=cost_cols[2], axis=1))
        if not calc_ser.empty:
            df[cost_cols[0]].update(calc_ser)
    return df


class DataSource(object):
    def __init__(self, key, vm_rules, **ven_param):
        self.key = key
        self.vm_rules = vm_rules
        self.params = ven_param
        self.ic_params = None
        self.p = self.params
        self.df = pd.DataFrame()
        for k in ven_param:
            setattr(self, k, ven_param[k])

    def get_active_metrics(self):
        active_metrics = {x: self.params[x] for x in self.params
                          if x in vmc.datacol and self.params[x] != ['nan']}
        return active_metrics

    def set_in_vendormatrix(self, col, new_value, matrix=None):
        if not matrix:
            matrix = VendorMatrix()
        index = matrix.vm_df[matrix.vm_df[vmc.vendorkey] == self.key].index[0]
        matrix.vm_df.loc[index, col] = new_value
        return matrix

    def add_new_rule(self, new_rule, matrix=None):
        last_val = max(self.vm_rules.keys())
        vm_rule = self.vm_rules[last_val]
        vm_rule = {x: vm_rule[x].replace(last_val, str(int(last_val) + 1))
                   for x in vm_rule}
        for col in vm_rule:
            self.set_in_vendormatrix(vm_rule[col], new_rule[col], matrix)
        return matrix

    def get_raw_df_before_transform(self, nrows=None):
        if vmc.filename not in self.p:
            return pd.DataFrame()
        df = utl.import_read_csv(self.p[vmc.filename], nrows=nrows)
        if df is None or df.empty:
            return df
        df = utl.add_header(df, self.p[vmc.header], self.p[vmc.firstrow])
        df = utl.first_last_adj(df, self.p[vmc.firstrow], self.p[vmc.lastrow])
        return df

    def get_raw_df(self, nrows=None):
        df = self.get_raw_df_before_transform(nrows=nrows)
        if df is None or df.empty:
            return df
        df = df_transform(df, self.p[vmc.transform])
        df = full_placement_creation(df, self.key, dctc.FPN,
                                     self.p[vmc.fullplacename])
        return df

    def get_raw_columns(self):
        if self.df.columns.empty:
            self.df = self.get_raw_df()
        return self.df.columns

    def get_dict_order_df(self, include_index=True, include_full_name=False):
        self.df = self.get_raw_df()
        if self.df.empty:
            return self.df
        dic = dct.Dict()
        rc = dct.RelationalConfig()
        rc.read(dctc.filename_rel_config)
        rc_auto_tuple = rc.get_auto_tuple()
        err = er.ErrorReport(self.df, dic, self.p[vmc.placement],
                             self.p[vmc.filenameerror])
        error = dic.split_error_df(err, self.p[vmc.autodicord],
                                   self.p[vmc.autodicplace],
                                   include_index=include_index,
                                   include_full_name=include_full_name)
        error = dic.translate_relation_cols(error, rc_auto_tuple,
                                            to_component=True,
                                            fix_bad_delim=False)
        return error

    def get_and_merge_dictionary(self, df):
        dic = dct.Dict(self.p[vmc.filenamedict])
        err = er.ErrorReport(df, dic, self.p[vmc.placement],
                             self.p[vmc.filenameerror])
        rc = dct.RelationalConfig()
        rc.read(dctc.filename_rel_config)
        rc_auto_tuple = rc.get_auto_tuple()
        dic.auto_functions(err=err, autodicord=self.p[vmc.autodicord],
                           placement=self.p[vmc.autodicplace],
                           rc_auto=rc_auto_tuple)
        df = dic.merge(df, dctc.FPN)
        return df

    def combine_data(self, df):
        """
        Moves float and date columns of data source to column names specified
        in vm while also converting type and applying rules.

        :param df: the raw df to act on
        :returns: the df with data under correct columns and types
        """
        df = combining_data(df, self.key, vmc.datadatecol, **self.p)
        df = utl.data_to_type(df, date_col=vmc.datadatecol)
        df = utl.apply_rules(df, self.vm_rules, utl.PRE, **self.p)
        if self.p[vmc.omit_plan] == 'MEDIAPLAN':
            float_cols = []
            for col in vmc.datafloatcol:
                self.p[col + vmc.planned_suffix] = self.p[col]
                float_cols.append(col + vmc.planned_suffix)
        else:
            float_cols = vmc.datafloatcol
        df = combining_data(df, self.key, float_cols, **self.p)
        df = utl.data_to_type(df, float_cols, vmc.datadatecol)
        return df

    def remove_cols_and_make_calculations(self, df):
        df = utl.date_removal(df, vmc.date, self.p[vmc.startdate],
                              self.p[vmc.enddate])
        df = ad_cost_calculation(df)
        df = utl.col_removal(df, self.key, self.p[vmc.dropcol])
        df = utl.apply_rules(df, self.vm_rules, utl.POST, **self.p)
        return df

    def import_data(self):
        self.df = self.get_raw_df()
        if self.df is None or self.df.empty:
            return self.df
        self.df = self.get_and_merge_dictionary(self.df)
        self.df = self.combine_data(self.df)
        self.df = self.remove_cols_and_make_calculations(self.df)
        self.df[vmc.vendorkey] = self.key
        return self.df

    def add_import_config_params(self, import_type='API_', matrix=None,
                                 ic=None):
        if not matrix:
            matrix = VendorMatrix()
        if not ic:
            ic = ImportConfig(matrix=matrix)
        current_imports = ic.get_current_imports(matrix=True)
        for x in current_imports:
            possible_keys = ['{}{}_{}'.format(import_type, x['Key'], x['name']),
                             '{}{}{}'.format(import_type, x['Key'], x['name'])]
            if self.key in possible_keys:
                self.ic_params = x
                return self.ic_params

    def write(self, df=None):
        utl.write_file(df, self.p[vmc.filename_true])


def import_plan_data(key, df, plan_omit_list, **kwargs):
    if df is None or df.empty:
        df = pd.DataFrame(columns=kwargs[vmc.fullplacename] + [vmc.vendorkey])
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


def df_transform(df, transform, skip_transforms=[]):
    if str(transform) == 'nan':
        return df
    split_transform = transform.split(':::')
    for t in split_transform:
        if t.split('::')[0] not in skip_transforms:
            df = df_single_transform(df, t)
    return df


def df_single_transform(df, transform):
    if str(transform) == 'nan':
        return df
    transform = transform.split('::')
    transform_type = transform[0]
    if transform_type == 'MixedDateColumn':
        mixed_col = transform[1]
        date_col = transform[2]
        if mixed_col not in df.columns:
            log.warning('Unable to execute {} transform. Column "{}" not '
                        'in datasource.'.format(transform_type, mixed_col))
            return df
        df[date_col] = df[mixed_col]
        df = utl.data_to_type(df, date_col=[date_col])
        df['temp'] = df[date_col]
        df[date_col] = df[date_col].fillna(method='ffill')
        df = df[df['temp'].isnull()].reset_index(drop=True)
        df.drop('temp', axis=1, inplace=True)
    if transform_type == 'Pivot':
        pivot_col = transform[1]
        val_col = transform[2].split('|')
        if pivot_col not in df.columns:
            log.warning('Unable to execute {} transform. Column "{}" '
                        'not in datasource.'.format(transform_type, pivot_col))
            return df
        df = df.fillna(0)
        index_cols = [x for x in df.columns if x not in val_col + [pivot_col]]
        df = pd.pivot_table(df, index=index_cols, columns=[pivot_col],
                            aggfunc='sum')
        if len(val_col) != 1:
            df.columns = df.columns.map('_'.join)
        if type(df.columns) == pd.MultiIndex:
            df.columns = [' - '.join([str(y) for y in x]) for x in df.columns]
        df = df.reset_index()
    if (transform_type == 'Merge' or transform_type == 'MergeReplace'
            or transform_type == 'MergeReplaceExclude'):
        merge_file = transform[1]
        if '.' in merge_file:
            merge_df = pd.read_csv(merge_file)
        else:
            matrix = VendorMatrix(display_log=False)
            matrix.sort_vendor_list()
            ven_param = matrix.vendor_set(merge_file)
            ds = DataSource(merge_file, matrix.vm_rules_dict, **ven_param)
            merge_df = ds.get_raw_df_before_transform()
            if not merge_df.empty and merge_df is not None:
                merge_df = df_transform(merge_df, ds.p[vmc.transform],
                                        skip_transforms=['Merge',
                                                         'MergeReplace',
                                                         'MergeReplaceExclude']
                                        )
        if merge_df is None or merge_df.empty:
            logging.error('Unable to execute merge transform. Requested merge '
                          'source {} returned empty dataframe.'
                          .format(merge_file))
            return df
        merge_cols = transform[2:]
        left_merge = merge_cols[::2]
        right_merge_full = merge_cols[1::2]
        right_merge = [x.split('|')[0] for x in right_merge_full]
        dfs = {'left': {'cols': left_merge, 'df': df},
               'right': {'cols': right_merge, 'df': merge_df}}
        for side in dfs:
            col = dfs[side]['cols']
            cdf = dfs[side]['df']
            for c in col:
                if cdf[c].dtype == 'float64':
                    cdf[c] = cdf[c].fillna(0).astype('int')
                cdf[c] = cdf[c].astype('U')
                cdf[c] = cdf[c].str.strip('.0')
            cdf = full_placement_creation(cdf, 'None', 'merge-col', col)
            dfs[side]['df'] = cdf
        df = dfs['left']['df']
        merge_df = dfs['right']['df']
        if (transform_type == 'MergeReplace'
                or transform_type == 'MergeReplaceExclude'):
            for idx, col in enumerate(right_merge_full):
                cols = col.split('|')
                col_id = cols[0]
                col_name = cols[1]
                replace_col = left_merge[idx]
                ndf = merge_df[cols].drop_duplicates()
                ndf = ndf.set_index(col_id).to_dict(orient='dict')[col_name]
                if transform_type == 'MergeReplaceExclude':
                    mask = df[replace_col].astype('U').isin(ndf.keys())
                    df = df[mask].reset_index(drop=True)
                df[replace_col] = df[replace_col].astype('U').replace(ndf)
        else:
            filename = 'Merge-{}-{}.csv'.format(
                '-'.join(left_merge), '-'.join(right_merge))
            err = er.ErrorReport(df, merge_df, None, filename,
                                 merge_col=['merge-col', 'merge-col'])
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
        if hold_col_name not in df.columns:
            log.warning('Unable to execute {} transform. Column "{}" '
                        'not in datasource.'.format(transform_type,
                                                    hold_col_name))
            return df
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
    if transform_type == 'Melt':
        header_col_name = transform[1]
        variable_cols = transform[2].split('|')
        missing_cols = [x for x in variable_cols if x not in df.columns]
        for col in missing_cols:
            df[col] = 0
        df = df.melt(id_vars=[x for x in df.columns if x not in variable_cols],
                     value_vars=[x for x in variable_cols if x in df.columns],
                     var_name='{}-variable'.format(header_col_name),
                     value_name='{}-value'.format(header_col_name))
        df = df.reset_index(drop=True)
    if transform_type == 'RawTranslate':
        tc = dct.DictTranslationConfig()
        tc.read(dctc.filename_tran_config)
        df = tc.apply_translation_to_dict(df)
    if transform_type == 'AddColumn':
        col_name = transform[1]
        col_val = transform[2]
        df[col_name] = col_val
    if transform_type == 'FilterCol':
        col_name = transform[1]
        col_val = transform[2]
        exclude_toggle = False
        if len(transform) > 3:
            if transform[3] == 'Exclude':
                exclude_toggle = True
        df = utl.filter_df_on_col(df, col_name, col_val, exclude_toggle)
    if transform_type == 'CombineColumns':
        cols = transform[1].split('|')
        if cols[0] not in df.columns or cols[1] not in df.columns:
            log.warning('Unable to execute {} transform. Column "{}" or "{}" '
                        'not in datasource.'.format(transform_type, cols[0],
                                                    cols[1]))
            return df
        df[cols[0]] = df[cols[0]].combine_first(df[cols[1]])
        df.drop(cols[1], axis=1, inplace=True)
    if transform_type == 'EqualReplace':
        col = transform[1]
        comp_cols = transform[2].split('|')
        replace_val = transform[3]
        comp_col = comp_cols[0]
        delimiter = comp_cols[1]
        idx = comp_cols[2]
        if col not in df.columns or comp_col not in df.columns:
            log.warning('Unable to execute {} transform. Column "{}" or "{}" '
                        'not in datasource.'.format(transform_type, col,
                                                    comp_col))
            return df
        comp = df[col].str.split(delimiter).str[int(idx)] == df[
            comp_col].astype('U')
        df[col] = np.where(comp, replace_val, df[col])
    if transform_type == 'RenameCol':
        cols = transform[1:]
        replace_dict = {x.split('|')[0]: x.split('|')[1] for x in cols}
        df = df.rename(columns=replace_dict)
    if transform_type == 'PercentDecode':
        cols = transform[1:]
        for col in cols:
            df[col] = df[col].map(
                lambda x: urllib.parse.unquote(
                    x, encoding='utf-8', errors='replace'))
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
