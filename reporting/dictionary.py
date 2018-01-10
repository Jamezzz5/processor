import sys
import os.path
import logging
import pandas as pd
import reporting.utils as utl
import reporting.dictcolumns as dctc

csvpath = 'Dictionaries/'


class Dict(object):
    def __init__(self, filename):
        utl.dir_check(csvpath)
        if str(filename) == 'nan':
            logging.error('No dictionary file provided.  Aborting.')
            sys.exit(0)
        self.filename = filename
        self.dict_path = csvpath
        self.dict_path_filename = self.dict_path + self.filename
        self.data_dict = pd.DataFrame()
        self.read()

    def read(self):
        if not os.path.isfile(self.dict_path_filename):
            logging.info('Creating ' + self.filename)
            if self.filename == dctc.PFN:
                data_dict = pd.DataFrame(columns=dctc.PCOLS, index=None)
            else:
                data_dict = pd.DataFrame(columns=dctc.COLS, index=None)
            data_dict.to_csv(self.dict_path_filename, index=False)
        self.data_dict = utl.import_read_csv(self.dict_path, self.filename)
        self.clean()
        self.data_dict = self.data_dict.drop_duplicates()

    def get(self):
        return self.data_dict

    def merge(self, df, colname):
        logging.info('Merging ' + self.filename)
        df = df.merge(self.data_dict, on=colname, how='left')
        return df

    def auto(self, err, autodicord, placement):
        error = err.get()
        if not autodicord == ['nan'] and not error.empty:
            if placement not in error.columns:
                logging.warning(
                    str(placement) + ' not in error report.  Use ' +
                    'Full Placement Name or mpPlacement Name.  ' +
                    'Dictionary was not automatically populated')
                return True
            logging.info('Populating ' + self.filename)
            for i, value in enumerate(autodicord):
                error[value] = error[placement].str.split('_').str[i]
            error = self.auto_combine(error)
            error = self.auto_split(error)
            error = error.loc[~error[dctc.FPN].isin(self.data_dict[dctc.FPN])]
            self.data_dict = self.data_dict.append(error)
            self.data_dict = self.data_dict[dctc.COLS]
            self.clean()
            self.write()
            err.dic = self
            err.reset()

    @staticmethod
    def auto_combine(error=pd.DataFrame()):
        comb_key = ':::'
        comb_cols = [x for x in error.columns if comb_key in x]
        for col in sorted(comb_cols):
            final_col = col.split(comb_key)[0]
            delimit_str = col.split(comb_key)[2]
            if final_col not in error.columns:
                error[final_col] = error[col].astype(str)
            else:
                error[final_col] = (error[final_col] + str(delimit_str) +
                                    error[col].astype(str))
            error.drop([col], axis=1, inplace=True)
        return error

    @staticmethod
    def auto_split(error=pd.DataFrame()):
        split_key = '::'
        split_cols = [x for x in error.columns if '::' in x]
        for col in split_cols:
            params = col.split(split_key)
            delimit_list = params[1::2]
            delimit_list = ', '.join(delimit_list)
            new_col_list = params[::2]
            error[col] = error[col].astype(str)
            error[col] = error[col].apply(lambda x: x + delimit_list
                                          if delimit_list not in x else x)
            df = pd.DataFrame(error[col].str.split(delimit_list, 1).tolist(),
                              columns=new_col_list, index=error.index)
            new_col_list.append(col)
            drop_cols = [x for x in new_col_list if x in error.columns]
            error.drop(drop_cols, axis=1, inplace=True)
            error = pd.concat([error, df], axis=1)
        return error

    def apply_relation(self):
        rc = RelationalConfig()
        rc.read(dctc.filename_rel_config)
        self.data_dict = rc.loop(self.data_dict)
        self.clean()
        self.write(self.data_dict)

    def apply_constants(self):
        dcc = DictConstantConfig()
        dcc.read(dctc.filename_con_config)
        self.data_dict = dcc.apply_constants_to_dict(self.data_dict)
        self.clean()
        self.write(self.data_dict)

    def apply_translation(self):
        tc = DictTranslationConfig()
        tc.read(dctc.filename_tran_config)
        self.data_dict = tc.apply_translation_to_dict(self.data_dict)
        self.clean()
        self.write(self.data_dict)

    def write(self, df=None):
        logging.debug('Writing ' + self.filename)
        if df is None:
            df = self.data_dict
        try:
            df.to_csv(self.dict_path_filename, index=False)
        except IOError:
            logging.warning(self.filename + ' could not be opened.  ' +
                            'This dictionary was not saved.')

    def clean(self):
        self.data_dict = utl.data_to_type(self.data_dict, dctc.floatcol,
                                          dctc.datecol, dctc.strcol)
        self.data_dict = self.data_dict.drop_duplicates(dctc.FPN)
        self.data_dict = self.data_dict.reset_index(drop=True)


class RelationalConfig(object):
    def __init__(self):
        self.csvpath = 'Config/'
        utl.dir_check('Config/')
        self.df = pd.DataFrame()
        self.rc = None
        self.relational_params = None
        self.key_list = []

    def read(self, configfile):
        filename = self.csvpath + configfile
        try:
            self.df = pd.read_csv(filename)
        except IOError:
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
        self.csvpath = 'Dictionaries/Relational/'
        utl.dir_check(self.csvpath)
        self.df = pd.DataFrame()
        self.params = kwargs
        self.filename = self.params[dctc.FN]
        self.full_file_path = self.csvpath + self.filename
        self.key = self.params[dctc.KEY]
        self.dependents = self.params[dctc.DEP]
        self.columns = [self.key] + self.dependents

    def read(self):
        if not os.path.isfile(self.full_file_path):
            logging.info('Creating ' + self.filename)
            df = pd.DataFrame(columns=self.columns, index=None)
            df.to_csv(self.full_file_path, index=False)
        self.df = pd.read_csv(self.full_file_path)

    def add_key_values(self, data_dict):
        keys_list = pd.DataFrame(data_dict[self.key]).drop_duplicates()
        keys_list.dropna(subset=[self.key], inplace=True)
        self.df = self.df.merge(keys_list, how='outer').reset_index(drop=True)
        self.df.dropna(subset=[self.key], inplace=True)
        self.write(self.df)

    def write(self, df):
        logging.debug('Writing ' + self.filename)
        if df is None:
            df = self.df
        try:
            df.to_csv(self.full_file_path, index=False)
        except IOError:
            logging.warning(self.filename + ' could not be opened.  ' +
                            'This dictionary was not saved.')

    def apply_to_dict(self, data_dict):
        if self.key not in data_dict.columns:
            return data_dict
        self.read()
        self.add_key_values(data_dict)
        self.df.dropna()
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
    def __init__(self):
        self.csvpath = 'Config/'
        utl.dir_check('Config/')
        self.df = pd.DataFrame()
        self.dict_col_names = None
        self.dict_col_values = None
        self.dict_constants = None

    def read(self, configfile):
        filename = self.csvpath + configfile
        try:
            self.df = pd.read_csv(filename)
        except IOError:
            logging.debug('No Constant Dictionary config')
            return None
        self.dict_col_names = self.df[dctc.DICT_COL_NAME].tolist()
        self.dict_constants = self.df.set_index(dctc.DICT_COL_NAME).to_dict()

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
        self.csvpath = 'Dictionaries/Translational/'
        utl.dir_check('Dictionaries/Translational/')
        self.df = pd.DataFrame()

    def read(self, configfile):
        filename = self.csvpath + configfile
        try:
            self.df = pd.read_csv(filename)
        except IOError:
            logging.debug('No Translational Dictionary config')
            return None

    def apply_translation_to_dict(self, data_dict):
        if self.df.empty:
            return data_dict
        for col in self.df[dctc.DICT_COL_NAME].unique():
            if col not in data_dict.columns:
                continue
            tdf = self.df[self.df[dctc.DICT_COL_NAME] == col]
            tdf = tdf[[dctc.DICT_COL_VALUE, dctc.DICT_COL_NVALUE]]
            replace_dict = dict(zip(tdf[dctc.DICT_COL_VALUE],
                                    tdf[dctc.DICT_COL_NVALUE]))
            data_dict[col] = data_dict[col].astype(str).replace(replace_dict)
        return data_dict


def dict_update():
    for filename in os.listdir(csvpath):
        if filename[-4:] != '.csv':
            continue
        if 'plannet' in filename:
            cols = dctc.PCOLS
        else:
            cols = dctc.COLS
        ndic = pd.DataFrame(columns=cols, index=None)
        dic = Dict(filename)
        odic = dic.get()
        df = ndic.append(odic)
        if 'pncFull Placement Name' in df.columns:
            df[dctc.FPN] = df['pncFull Placement Name']
            df = df[cols]
        df = df[cols]
        dic.write(df)
