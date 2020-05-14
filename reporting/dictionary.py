import sys
import os
import logging
import numpy as np
import pandas as pd
import reporting.utils as utl
import reporting.dictcolumns as dctc

csvpath = utl.dict_path


class Dict(object):
    def __init__(self, filename=None):
        utl.dir_check(csvpath)
        if str(filename) == 'nan':
            logging.error('No dictionary file provided.  Aborting.')
            sys.exit(0)
        self.filename = filename
        self.dict_path = csvpath
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

    def auto_functions(self, err, autodicord, placement):
        self.auto(err, autodicord, placement)
        self.apply_functions()

    @staticmethod
    def split_error_df(err, autodicord, placement, include_index=False):
        error = err.get()
        error.columns = [dctc.FPN, dctc.PN]
        for i, value in enumerate(autodicord):
            if include_index:
                col_name = '{}-{}'.format(i, value)
            else:
                col_name = value
            error[col_name] = error[placement].str.split('_').str[i]
        return error

    def auto(self, err, autodicord, placement):
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
            error = self.auto_combine(error)
            error = self.auto_split(error)
            error = error.loc[~error[dctc.FPN].isin(self.data_dict[dctc.FPN])]
            self.data_dict = self.data_dict.append(error, sort=True)
            self.data_dict = self.data_dict[dctc.COLS]
            err.dic = self
            err.reset()

    @staticmethod
    def auto_combine(error=pd.DataFrame()):
        comb_key = ':::'
        comb_cols = [x for x in error.columns if comb_key in x]
        for col in sorted(comb_cols, key=lambda x: int(x.split(comb_key)[1])):
            final_col = col.split(comb_key)[0]
            delimit_str = col.split(comb_key)[2]
            if final_col not in error.columns:
                error[final_col] = error[col].astype('U')
            else:
                error[final_col] = (error[final_col].astype('U') +
                                    str(delimit_str) +
                                    error[col].astype('U'))
            error.drop([col], axis=1, inplace=True)
        return error

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
        self.csvpath = utl.config_path
        utl.dir_check(utl.config_path)
        self.df = pd.DataFrame()
        self.rc = None
        self.relational_params = None
        self.key_list = []

    def write(self, df, configfile):
        logging.debug('Writing {}'.format(configfile))
        try:
            df.to_csv(os.path.join(self.csvpath, configfile), index=False,
                      encoding='utf-8')
        except IOError:
            logging.warning('{} could not be opened.  This dictionary'
                            'was not saved.'.format(configfile))

    def read(self, configfile):
        self.df = utl.import_read_csv(configfile, self.csvpath)
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

    def loop(self, data_dict):
        for key in self.key_list:
            self.relational_params = self.get_relation_params(key)
            dr = DictRelational(**self.relational_params)
            data_dict = dr.apply_to_dict(data_dict)
        return data_dict


class DictRelational(object):
    def __init__(self, **kwargs):
        self.csvpath = os.path.join(csvpath, 'Relational/')
        utl.dir_check(self.csvpath)
        self.df = pd.DataFrame()
        self.params = kwargs
        self.filename = self.params[dctc.FN]
        self.full_file_path = os.path.join(self.csvpath, self.filename)
        self.key = self.params[dctc.KEY]
        self.dependents = self.params[dctc.DEP]
        self.columns = [self.key] + self.dependents

    def read(self):
        if not os.path.isfile(self.full_file_path):
            logging.info('Creating {}'.format(self.filename))
            df = pd.DataFrame(columns=self.columns, index=None)
            df.to_csv(self.full_file_path, index=False, encoding='utf-8')
        self.df = utl.import_read_csv(self.filename, self.csvpath)
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
        self.csvpath = utl.config_path
        utl.dir_check(utl.config_path)
        self.df = pd.DataFrame()
        self.dict_col_names = None
        self.dict_col_values = None
        self.dict_constants = None

    def read_raw_df(self, configfile):
        try:
            self.df = utl.import_read_csv(configfile, self.csvpath)
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
            df.to_csv(os.path.join(self.csvpath, configfile), index=False,
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
        self.csvpath = os.path.join(csvpath, 'Translational/')
        utl.dir_check(self.csvpath)
        self.df = pd.DataFrame()

    def read(self, configfile):
        try:
            self.df = utl.import_read_csv(configfile, self.csvpath)
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
            df.to_csv(os.path.join(self.csvpath, configfile), index=False,
                      encoding='utf-8')
        except IOError:
            logging.warning('{} could not be opened.  This dictionary '
                            'was not saved.'.format(configfile))

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
                data_dict.loc[(data_dict[col2] == col2_q) &
                              (data_dict[col] == val), col] = nval
            if fnc_type == 'Set':
                data_dict.loc[data_dict[col2] == col2_q, col] = nval
            if fnc_type == 'Append':
                mask = ((data_dict[col2] == col2_q) &
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
    for filename in os.listdir(csvpath):
        logging.info('Attempting to update {}'.format(filename))
        if filename[-4:] != '.csv':
            continue
        if 'plannet' in filename:
            cols = dctc.PCOLS
        else:
            cols = dctc.COLS
        ndic = pd.DataFrame(columns=cols, index=None)
        dic = Dict(filename)
        if dctc.FPN not in dic.data_dict.columns:
            continue
        odic = dic.get()
        df = ndic.append(odic, sort=True)
        if 'pncFull Placement Name' in df.columns:
            df[dctc.FPN] = df['pncFull Placement Name']
            df = df[cols]
        df = df[cols]
        dic.write(df)
