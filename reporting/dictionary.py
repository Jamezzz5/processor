import sys
import os
import logging
import numpy as np
import pandas as pd
import reporting.utils as utl
import reporting.dictcolumns as dctc

csv_path = utl.dict_path


class Dict(object):
    def __init__(self, filename=None):
        utl.dir_check(csv_path)
        if str(filename) == 'nan':
            logging.error('No dictionary file provided.  Aborting.')
            sys.exit(0)
        self.filename = filename
        self.comb_key = ':::'
        self.dict_path = csv_path
        self.data_dict = pd.DataFrame(columns=dctc.COLS, index=None)
        if filename:
            self.dict_path_filename = os.path.join(self.dict_path,
                                                   self.filename)
            self.read()

    def create_new_dictionary(self):
        logging.info('Creating {}'.format(self.filename))
        if self.filename == dctc.PFN:
            data_dict = pd.DataFrame(columns=dctc.PCOLS, index=None)
        else:
            data_dict = pd.DataFrame(columns=dctc.COLS, index=None)
        data_dict.to_csv(self.dict_path_filename, index=False,
                         encoding='utf-8')
        return data_dict

    def read(self):
        if not os.path.isfile(self.dict_path_filename):
            self.create_new_dictionary()
        self.data_dict = utl.import_read_csv(self.filename, self.dict_path)
        if not isinstance(self.data_dict, pd.DataFrame) and not self.data_dict:
            self.data_dict = self.create_new_dictionary()
        self.clean()
        self.data_dict = self.data_dict.drop_duplicates()

    def get(self):
        return self.data_dict

    def merge(self, df, colname):
        logging.info('Merging {}'.format(self.filename))
        df = df.merge(self.data_dict, on=colname, how='left')
        return df

    def auto_functions(self, err, autodicord, placement, rc_auto):
        self.auto(err, autodicord, placement, rc_auto)
        self.apply_functions()

    @staticmethod
    def split_error_df(err, autodicord, placement, include_index=False,
                       include_full_name=False):
        error = err.get()
        error.columns = [dctc.FPN, dctc.PN]
        if include_full_name:
            max_placement_name_length = max(
                map(len, error[placement].str.split('_')))
            if len(autodicord) < max_placement_name_length:
                length_diff = max_placement_name_length - len(autodicord)
                autodicord.extend([dctc.MIS] * length_diff)
            autodicord = list(utl.rename_duplicates(autodicord))
        for i, value in enumerate(autodicord):
            if include_index:
                col_name = '{}-{}'.format(i, value)
            else:
                col_name = value
            error[col_name] = error[placement].str.split('_').str[i]
        return error

    def auto(self, err, autodicord, placement, rc_auto):
        error = err.get()
        if not autodicord == ['nan'] and not error.empty:
            if placement not in error.columns:
                logging.warning(
                    '{} not in error report.  Use  Full Placement Name'
                    'or mpPlacement Name.  Dictionary was not automatically'
                    ' populated'.format(placement))
                return True
            logging.info('Populating {}'.format(self.filename))
            error = self.split_error_df(err, autodicord, placement)
            error = self.auto_combine(error, rc_auto)
            error = self.auto_split(error)
            error = error.loc[~error[dctc.FPN].isin(self.data_dict[dctc.FPN])]
            self.data_dict = self.data_dict.append(error, sort=True)
            self.data_dict = self.data_dict[dctc.COLS]
            err.dic = self
            err.reset()

    def auto_combine(self, error, rc_auto):
        sorted_missing = self.sort_relation_cols(
            error.columns, rc_auto, return_missing=True,
            return_bad_values=True)
        if sorted_missing['bad_values']:
            logging.warning(
                'The following auto dictionary items could not be sorted into '
                'a relation column: {} They will not be added to {}'.format(
                    sorted_missing['bad_values'], self.filename))
            error.drop(columns=sorted_missing['bad_values'], inplace=True)
        error = self.translate_relation_cols(error, rc_auto)
        comb_cols = [x for x in error.columns if self.comb_key in x]
        final_cols = set(x.split(self.comb_key)[0] for x in comb_cols)
        for col in final_cols:
            ind = [int(x.split(self.comb_key)[1]) for x in comb_cols if
                   x.split(self.comb_key)[0] == col]
            ind = [x for x in range(max(ind) + 1) if x not in ind]
            if ind:
                if col in sorted_missing:
                    ind = [x for x in sorted_missing[col]['missing'] if
                           int(x.split(self.comb_key)[1]) in ind]
                else:
                    delimit_str = [x for x in comb_cols if
                                   x.split(self.comb_key)[0] == col][0]
                    delimit_str = delimit_str.split(self.comb_key)[2]
                    ind = [self.comb_key.join([col, str(x), delimit_str])
                           for x in ind]
                comb_cols += ind
                for i in ind:
                    error[i] = 0
        comb_cols = sorted(comb_cols,
                           key=lambda x: int(x.split(self.comb_key)[1]))
        for col in comb_cols:
            final_col = col.split(self.comb_key)[0]
            delimit_str = col.split(self.comb_key)[2]
            if final_col not in error.columns:
                error[final_col] = error[col].astype('U')
            else:
                error[final_col] = (error[final_col].astype('U') +
                                    str(delimit_str) +
                                    error[col].astype('U'))
            error.drop([col], axis=1, inplace=True)
        return error

    def sort_relation_cols(self, columns, rc_auto, keep_bad_delim=True,
                           return_missing=False, return_bad_values=False):
        miss = 'missing'
        bad_value = 'bad_values'
        default_delim = '_'
        rc_cols, rc_delimit = rc_auto
        component_dict = {}
        valid_values = []
        all_rel_cols = []
        for rc_key in rc_cols:
            start_idx = 0
            component_dict[rc_key] = {comp: [] for comp in rc_cols[rc_key]}
            key_cols = [col for col in columns
                        if col.split(self.comb_key)[0] == rc_key]
            all_rel_cols.extend(key_cols)
            for idx, comp in enumerate(rc_cols[rc_key]):
                lead_delim = None
                trail_delim = None
                if 0 < idx <= len(rc_delimit[rc_key]):
                    lead_delim = rc_delimit[rc_key][idx - 1]
                if idx < len(rc_delimit[rc_key]):
                    trail_delim = rc_delimit[rc_key][idx]
                rel_cols = [col for col in columns
                            if col.split(self.comb_key)[0] == comp
                            if col not in key_cols]
                all_rel_cols.extend(rel_cols)
                if rel_cols:
                    first_index = 0
                    check_cols = rel_cols
                    bad_delim = keep_bad_delim
                else:
                    first_index = start_idx
                    check_cols = key_cols
                    bad_delim = False
                first_col = self.get_first_comp_col(first_index,
                                                    check_cols,
                                                    lead_delim,
                                                    bad_delim)
                seq_cols = self.get_sequential_comp_cols(first_index+1,
                                                         check_cols,
                                                         trail_delim)
                if not first_col:
                    if return_missing and check_cols:
                        if miss not in component_dict[rc_key]:
                            component_dict[rc_key][miss] = []
                        if not lead_delim:
                            lead_delim = default_delim
                        missing_col = self.comb_key.join(
                            [rc_key, str(start_idx), lead_delim])
                        component_dict[rc_key][miss].append(missing_col)
                    start_idx += 1
                component_dict[rc_key][comp].extend(first_col)
                component_dict[rc_key][comp].extend(seq_cols)
                start_idx += len(component_dict[rc_key][comp])

            extra_cols = [col for col in key_cols if
                          len(col.split(self.comb_key)) == 3]
            extra_cols = [col for col in extra_cols if
                          int(col.split(self.comb_key)[1]) >= start_idx]
            if return_missing:
                if miss not in component_dict[rc_key]:
                    component_dict[rc_key][miss] = []
                if extra_cols:
                    extra_ind = [int(col.split(self.comb_key)[1]) for col in
                                 extra_cols]
                    missing_ind = [
                        str(x) for x in range(start_idx, max(extra_ind)) if x
                        not in extra_ind]
                    missing_col = [
                        self.comb_key.join([rc_key, i, default_delim])
                        for i in missing_ind]
                    component_dict[rc_key][miss].extend(missing_col)
            if return_bad_values:
                valid_values.extend(extra_cols)
        if return_bad_values:
            valid_values.extend([x for col in component_dict for sublist in
                                 component_dict[col].values() for x in
                                 sublist])
            component_dict[bad_value] = [x for x in all_rel_cols if x not in
                                         valid_values]
        return component_dict

    def get_first_comp_col(self, start_idx, columns, delim=None,
                           keep_bad_delim=True):
        if not columns:
            return []
        col_name = columns[0].split(self.comb_key)[0]
        if col_name in columns:
            first_col = [col_name]
            return first_col
        if delim and not keep_bad_delim:
            first_col = [col for col in columns if col.split(self.comb_key)[1:]
                         == [str(start_idx), delim]]
        else:
            first_col = [col for col in columns if col.split(self.comb_key)[1]
                         == str(start_idx)]
        if len(first_col) > 1:
            first_col = [first_col[0]]
        return first_col

    def get_sequential_comp_cols(self, start_idx, columns, delim):
        if not columns:
            return []
        col_name = columns[0].split(self.comb_key)[0]
        if col_name in columns:
            columns.remove(col_name)
        sequential_cols = []
        idx = start_idx
        skipped_index = False
        while not skipped_index:
            seq_col = [col for col in columns
                       if col.split(self.comb_key)[1] == str(idx)
                       and col.split(self.comb_key)[2] != delim]
            if not seq_col:
                skipped_index = True
            else:
                sequential_cols.append(seq_col[0])
            idx += 1
        return sequential_cols

    def get_relation_translations(self, columns, rc_auto, fix_bad_delim=True):
        if columns.empty:
            return {}
        rc_delimit = rc_auto[1]
        sorted_columns = self.sort_relation_cols(
            columns, rc_auto, keep_bad_delim=fix_bad_delim)
        translation_dict = {}
        for rc_key in sorted_columns:
            abs_index = 0
            for idx, comp in enumerate(sorted_columns[rc_key]):
                if not sorted_columns[rc_key][comp]:
                    abs_index += 1
                    continue
                col_name = (sorted_columns[rc_key][comp][0]
                            .split(self.comb_key)[0])
                lead_delim = None
                first_index = abs_index
                if 0 < idx <= len(rc_delimit[rc_key]):
                    lead_delim = rc_delimit[rc_key][idx-1]
                if col_name == comp:
                    first_index = 0
                first_col = self.get_first_comp_col(
                    first_index, sorted_columns[rc_key][comp],
                    lead_delim, fix_bad_delim)
                if not first_col:
                    abs_index += 1
                for col_idx, col in enumerate(sorted_columns[rc_key][comp]):
                    if not first_col:
                        col_idx += 1
                    if len(col.split(self.comb_key)) != 3:
                        new_delim = '_'
                        if lead_delim:
                            new_delim = lead_delim
                    else:
                        new_delim = col.split(self.comb_key)[2]
                    if col_name == comp:
                        new_name = rc_key
                        new_index = str(abs_index)
                        if fix_bad_delim and col_idx < 1 and lead_delim:
                            if new_delim != lead_delim:
                                new_delim = lead_delim
                                logging.warning(
                                    '{} has incorrect delimiter to be the '
                                    'first component of {}. Treating '
                                    'delimiter as {}.'
                                    .format(col, comp, lead_delim))
                    else:
                        new_name = comp
                        new_index = str(col_idx)
                    trans_name = self.comb_key.join(
                        [new_name, new_index, new_delim])
                    translation_dict[col] = trans_name
                    abs_index += 1
        return translation_dict

    def translate_relation_cols(self, df, rc_auto, to_component=False,
                                fix_bad_delim=True):
        if df.columns.empty:
            return df
        rc_keys = rc_auto[0].keys()
        rc_comps = [col for sublist in rc_auto[0].values() for col in sublist]
        translation_dict = self.get_relation_translations(
            df.columns, rc_auto, fix_bad_delim=fix_bad_delim)
        if to_component:
            translation_dict = {k: v for k, v in translation_dict.items()
                                if k.split(self.comb_key)[0] in rc_keys}
        else:
            translation_dict = {k: v for k, v in translation_dict.items()
                                if k.split(self.comb_key)[0] in rc_comps}
        tdf = df.rename(columns=translation_dict)
        return tdf

    @staticmethod
    def auto_split(error=pd.DataFrame()):
        split_key = '::'
        split_cols = [x for x in error.columns if '::' in x]
        for col in split_cols:
            params = col.split(split_key)
            delimit_list = params[1::2]
            new_col_list = params[::2]
            for idx, c in enumerate(delimit_list):
                cur_del = delimit_list[idx]
                cur_col = new_col_list[idx]
                error[col] = error[col].astype('U')
                error[cur_col] = error[col].str.split(cur_del).str[0]
                error[col] = error[col].str.split(cur_del).str[1:]
                error[col] = error[col].apply(lambda x: cur_del.join(x))
            error[new_col_list[-1]] = error[col]
            error.drop([col], axis=1, inplace=True)
        return error

    def apply_functions(self):
        self.apply_constants()
        self.apply_translation()
        self.apply_relation()
        self.apply_translation()
        self.clean()
        self.write()

    def apply_relation(self):
        rc = RelationalConfig()
        rc.read(dctc.filename_rel_config)
        self.data_dict = rc.loop(self.data_dict)

    def apply_constants(self):
        dcc = DictConstantConfig(self.filename)
        dcc.read(dctc.filename_con_config)
        self.data_dict = dcc.apply_constants_to_dict(self.data_dict)

    def apply_translation(self):
        tc = DictTranslationConfig()
        tc.read(dctc.filename_tran_config)
        self.data_dict = tc.apply_translation_to_dict(self.data_dict)

    def write(self, df=None):
        logging.debug('Writing {}'.format(self.filename))
        if df is None:
            df = self.data_dict
        try:
            df.to_csv(self.dict_path_filename, index=False, encoding='utf-8')
        except IOError:
            logging.warning('{} could not be opened.  This dictionary'
                            'was not saved.'.format(self.filename))

    def clean(self):
        self.data_dict = utl.data_to_type(self.data_dict, dctc.floatcol,
                                          dctc.datecol, dctc.strcol)
        if dctc.FPN in self.data_dict.columns:
            self.data_dict = self.data_dict.drop_duplicates(dctc.FPN)
        self.data_dict = self.data_dict.reset_index(drop=True)


class RelationalConfig(object):
    def __init__(self):
        self.csv_path = utl.config_path
        utl.dir_check(utl.config_path)
        self.df = pd.DataFrame()
        self.rc = None
        self.relational_params = None
        self.key_list = []

    def write(self, df, configfile):
        logging.debug('Writing {}'.format(configfile))
        try:
            df.to_csv(os.path.join(self.csv_path, configfile), index=False,
                      encoding='utf-8')
        except IOError:
            logging.warning('{} could not be opened.  This dictionary'
                            'was not saved.'.format(configfile))

    def read(self, configfile):
        self.df = utl.import_read_csv(configfile, self.csv_path)
        if self.df.empty:
            logging.debug('No Relational Dictionary config')
            return None
        self.key_list = self.df[dctc.RK].tolist()
        self.rc = self.df.set_index(dctc.RK).to_dict()
        self.rc[dctc.DEP] = ({key: list(str(value).split('|')) for key, value
                              in self.rc[dctc.DEP].items()})

    def get_relation_params(self, relational_key):
        relational_params = {}
        for param in self.rc:
            value = self.rc[param][relational_key]
            relational_params[param] = value
        return relational_params

    def get_auto_cols(self):
        rc_auto = {self.rc[dctc.KEY][k]: v.split('::')[::2]
                   for k, v in self.rc[dctc.AUTO].items() if str(v) != 'nan'}
        return rc_auto

    def get_auto_delims(self):
        rc_delimit = {self.rc[dctc.KEY][k]: v.split('::')[1::2]
                      for k, v in self.rc[dctc.AUTO].items()
                      if str(v) != 'nan'}
        return rc_delimit

    def get_auto_tuple(self):
        auto_tuple = (self.get_auto_cols(), self.get_auto_delims())
        return auto_tuple

    def get_auto_cols_list(self):
        rc_auto = self.get_auto_cols()
        cols_list = []
        for key in rc_auto:
            cols_list.extend(rc_auto[key])
        return cols_list

    def loop(self, data_dict):
        for key in self.key_list:
            self.relational_params = self.get_relation_params(key)
            dr = DictRelational(**self.relational_params)
            data_dict = dr.apply_to_dict(data_dict)
        return data_dict


class DictRelational(object):
    def __init__(self, **kwargs):
        self.csv_path = os.path.join(csv_path, 'Relational/')
        utl.dir_check(self.csv_path)
        self.df = pd.DataFrame()
        self.params = kwargs
        self.filename = self.params[dctc.FN]
        self.full_file_path = os.path.join(self.csv_path, self.filename)
        self.key = self.params[dctc.KEY]
        self.dependents = self.params[dctc.DEP]
        self.columns = [self.key] + self.dependents

    def read(self):
        if not os.path.isfile(self.full_file_path):
            logging.info('Creating {}'.format(self.filename))
            df = pd.DataFrame(columns=self.columns, index=None)
            df.to_csv(self.full_file_path, index=False, encoding='utf-8')
        self.df = utl.import_read_csv(self.filename, self.csv_path,
                                      empty_df=True)
        if self.df.empty:
            self.df = pd.DataFrame(columns=self.columns)
        self.df = utl.data_to_type(self.df, str_col=[self.key])

    def add_key_values(self, data_dict):
        keys_list = pd.DataFrame(data_dict[self.key]).drop_duplicates()
        keys_list.dropna(subset=[self.key], inplace=True)
        keys_list = self.get_new_values(keys_list)
        keys_list = self.auto_split(keys_list)
        self.df = utl.data_to_type(self.df, str_col=keys_list.columns)
        self.df = self.df.merge(keys_list, how='outer').reset_index(drop=True)
        self.df.dropna(subset=[self.key], inplace=True)
        self.write(self.df)

    def get_new_values(self, keys_list):
        keys_list = utl.data_to_type(keys_list, str_col=keys_list.columns)
        keys_list = keys_list.merge(pd.DataFrame(self.df[self.key]),
                                    on=self.key, how='left', indicator=True)
        keys_list = keys_list[keys_list['_merge'] == 'left_only']
        keys_list = pd.DataFrame(keys_list[self.key])
        return keys_list

    def auto_split(self, keys_list):
        tdf = keys_list
        if 'Auto' in self.params and str(self.params['Auto']) != 'nan':
            tdf = tdf.rename(columns={self.key: self.params['Auto']})
            tdf = Dict().auto_split(tdf)
            if keys_list.empty:
                tdf[self.key] = None
            else:
                tdf[self.key] = keys_list
        return tdf

    def write(self, df):
        logging.debug('Writing {}'.format(self.filename))
        if df is None:
            df = self.df
        try:
            df.to_csv(self.full_file_path, index=False, encoding='utf-8')
        except IOError:
            logging.warning('{} could not be opened.  This dictionary '
                            'was not saved.'.format(self.filename))

    def apply_to_dict(self, data_dict):
        if self.key not in data_dict.columns:
            return data_dict
        self.read()
        self.add_key_values(data_dict)
        data_dict = utl.data_to_type(data_dict, str_col=[self.key])
        data_dict = data_dict.merge(self.df, on=self.key, how='left')
        for col in self.dependents:
            col_x = col + '_x'
            col_y = col + '_y'
            if col_y in data_dict.columns:
                data_dict[col] = data_dict[col_y]
                data_dict = data_dict.drop([col_x, col_y], axis=1)
        self.rename_y_columns(data_dict)
        data_dict = self.reorder_columns(data_dict)
        return data_dict

    @staticmethod
    def rename_y_columns(df):
        for x in df.columns:
            if x[-2:] == '_y':
                df.rename(columns={x: x[:-2]}, inplace=True)

    @staticmethod
    def reorder_columns(data_dict):
        if dctc.PNC in data_dict.columns:
            first_cols = [dctc.FPN, dctc.PNC]
            back_cols = [x for x in data_dict.columns if x not in first_cols]
            cols = first_cols + back_cols
        else:
            cols = [x for x in dctc.COLS if x in data_dict.columns]
        data_dict = data_dict[cols]
        return data_dict


class DictConstantConfig(object):
    def __init__(self, parent_dict):
        self.parent_dict = parent_dict
        self.csv_path = utl.config_path
        utl.dir_check(utl.config_path)
        self.df = pd.DataFrame()
        self.dict_col_names = None
        self.dict_col_values = None
        self.dict_constants = None

    def read_raw_df(self, configfile):
        try:
            self.df = utl.import_read_csv(configfile, self.csv_path)
        except IOError:
            logging.debug('No Constant Dictionary config')
            return None
        self.check_for_dict_col(configfile)

    def read(self, configfile):
        self.read_raw_df(configfile)
        self.filter_df()
        self.dict_col_names = self.df[dctc.DICT_COL_NAME].tolist()
        self.dict_constants = self.df.set_index(dctc.DICT_COL_NAME).to_dict()

    def check_for_dict_col(self, configfile):
        if dctc.DICT_COL_DICTNAME not in self.df.columns:
            self.df[dctc.DICT_COL_DICTNAME] = np.nan
            self.write(self.df, configfile)

    def write(self, df, configfile):
        logging.debug('Writing {}'.format(configfile))
        try:
            df.to_csv(os.path.join(self.csv_path, configfile), index=False,
                      encoding='utf-8')
        except IOError:
            logging.warning('{} could not be opened.  This dictionary'
                            'was not saved.'.format(configfile))

    def filter_df(self):
        self.df[dctc.DICT_COL_DICTNAME] = (self.df[dctc.DICT_COL_DICTNAME]
                                           .fillna(self.parent_dict))
        self.df = self.df[self.df[dctc.DICT_COL_DICTNAME] == self.parent_dict]
        self.df = self.df.drop(dctc.DICT_COL_DICTNAME, axis=1)

    def get(self):
        return self.dict_constants

    def apply_constants_to_dict(self, data_dict):
        if self.dict_col_names is None:
            return data_dict
        for col in self.dict_col_names:
            constant_value = self.dict_constants[dctc.DICT_COL_VALUE][col]
            data_dict[col] = constant_value
        return data_dict


class DictTranslationConfig(object):
    def __init__(self):
        self.csv_path = os.path.join(csv_path, 'Translational/')
        utl.dir_check(self.csv_path)
        self.df = pd.DataFrame()

    def read(self, configfile):
        try:
            self.df = utl.import_read_csv(configfile, self.csv_path)
        except IOError:
            logging.debug('No Translational Dictionary config')
            return None
        self.clean_df()

    def clean_df(self):
        col = dctc.DICT_COL_VALUE
        self.df[col] = np.where(self.df[col].isna(), self.df[col].astype('U'),
                                self.df[col])

    def write(self, df, configfile):
        logging.debug('Writing {}'.format(configfile))
        try:
            df.to_csv(os.path.join(self.csv_path, configfile), index=False,
                      encoding='utf-8')
        except IOError:
            logging.warning('{} could not be opened.  This dictionary '
                            'was not saved.'.format(configfile))

    def get(self):
        return self.df

    def apply_translation_to_dict(self, data_dict):
        if self.df.empty:
            return data_dict
        for col in self.df[dctc.DICT_COL_NAME].unique():
            if col not in data_dict.columns:
                continue
            tdf = self.df[self.df[dctc.DICT_COL_NAME] == col]
            if dctc.DICT_COL_FNC in tdf.columns:
                data_dict = self.strip_dict(tdf, col, data_dict)
                data_dict = self.select_translation(tdf, col, data_dict)
                data_dict = self.select_translation(tdf, col, data_dict, 'Set')
                data_dict = self.select_translation(tdf, col, data_dict,
                                                    'Append')
                tdf = tdf[tdf[dctc.DICT_COL_FNC].isnull()]
            data_dict = self.apply_translation(tdf, col, data_dict)
        return data_dict

    @staticmethod
    def select_translation(tdf, col, data_dict, fnc_type='Select'):
        if dctc.DICT_COL_SEL not in tdf.columns:
            return data_dict
        tdf = tdf.copy()
        tdf = utl.data_to_type(tdf, str_col=[dctc.DICT_COL_FNC])
        select_rows = tdf[dctc.DICT_COL_FNC].str.contains(fnc_type, na=False)
        tdf = tdf[select_rows].copy()
        tdf[dctc.DICT_COL_FNC] = tdf[dctc.DICT_COL_FNC].str.split('::').str[1]
        sel = tdf[[dctc.DICT_COL_FNC, dctc.DICT_COL_SEL, dctc.DICT_COL_VALUE,
                   dctc.DICT_COL_NVALUE]].to_dict(orient='index')
        for s in sel:
            col2 = sel[s][dctc.DICT_COL_FNC]
            col2_q = sel[s][dctc.DICT_COL_SEL]
            val = sel[s][dctc.DICT_COL_VALUE]
            nval = sel[s][dctc.DICT_COL_NVALUE]
            if col2 not in data_dict.columns:
                continue
            if fnc_type == 'Select':
                data_dict.loc[(data_dict[col2].astype('U') == col2_q) &
                              (data_dict[col] == val), col] = nval
            if fnc_type == 'Set':
                data_dict.loc[data_dict[col2].astype('U') == col2_q, col] = nval
            if fnc_type == 'Append':
                mask = ((data_dict[col2].astype('U') == col2_q) &
                        (data_dict[col].str[-len(nval):] != nval))
                data_dict.loc[mask, col] = (data_dict.loc[mask, col] + nval)
        return data_dict

    @staticmethod
    def strip_dict(tdf, col, data_dict):
        tdf = tdf.copy()
        tdf = utl.data_to_type(tdf, str_col=[dctc.DICT_COL_FNC])
        tdf = tdf[tdf[dctc.DICT_COL_FNC] == 'Strip']
        data_dict = utl.data_to_type(data_dict, str_col=[col])
        for val in tdf[dctc.DICT_COL_VALUE].unique():
            data_dict[col] = data_dict[col].str.replace(val, '')
        return data_dict

    @staticmethod
    def apply_translation(tdf, col, data_dict):
        tdf = tdf[[dctc.DICT_COL_VALUE, dctc.DICT_COL_NVALUE]]
        replace_dict = dict(zip(tdf[dctc.DICT_COL_VALUE],
                                tdf[dctc.DICT_COL_NVALUE]))
        data_dict[col] = data_dict[col].astype('U').replace(replace_dict)
        return data_dict


def dict_update():
    for filename in os.listdir(csv_path):
        logging.info('Attempting to update {}'.format(filename))
        if filename[-4:] != '.csv':
            continue
        if 'plannet' in filename:
            cols = dctc.PCOLS
        else:
            cols = dctc.COLS
        ndic = pd.DataFrame(columns=cols, index=None)
        try:
            dic = Dict(filename)
        except Exception as e:
            logging.warning('Could not load dict continuing: {}'.format(e))
            continue
        if dctc.FPN not in dic.data_dict.columns:
            continue
        odic = dic.get()
        df = ndic.append(odic, sort=True)
        if 'pncFull Placement Name' in df.columns:
            df[dctc.FPN] = df['pncFull Placement Name']
            df = df[cols]
        df = df[cols]
        dic.write(df)
