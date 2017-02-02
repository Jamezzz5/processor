import sys
import os.path
import logging
import pandas as pd
import cleaning as cln
import dictcolumns as dctc

csvpath = 'Dictionaries/'


class Dict(object):
    def __init__(self, filename):
        cln.dircheck(csvpath)
        if str(filename) == 'nan':
            logging.error('No dictionary file provided.  Aborting.')
            sys.exit(0)
        self.filename = filename
        self.dictfile = csvpath + self.filename
        self.read()

    def read(self):
        if not os.path.isfile(self.dictfile):
            logging.info('Creating ' + self.filename)
            if self.filename == dctc.PFN:
                data_dict = pd.DataFrame(columns=dctc.PCOLS, index=None)
            else:
                data_dict = pd.DataFrame(columns=dctc.COLS, index=None)
            data_dict.to_csv(self.dictfile, index=False)
        self.data_dict = pd.read_csv(self.dictfile)
        self.clean()

    def get(self):
        return self.data_dict

    def merge(self, df, colname):
        logging.info('Merging ' + self.filename)
        df = df.merge(self.data_dict, on=colname, how='left')
        return df

    def auto(self, err, autodicord, placement):
        error = err.get()
        if not autodicord == ['nan'] and not error.empty:
            logging.info('Populating ' + self.filename)
            i = 0
            for value in autodicord:
                error[value] = error[dctc.PN].str.split('_').str[i]
                i = i + 1
            error = error.ix[~error[dctc.FPN].isin(self.data_dict[dctc.FPN])]
            self.data_dict = self.data_dict.append(error)
            self.data_dict = self.data_dict[dctc.COLS]
            self.write()
            err.dic = self
            err.reset()
            self.clean()

    def write(self, df=None):
        logging.info('Writing ' + self.filename)
        if df is None:
            df = self.data_dict
        try:
            df.to_csv(self.dictfile, index=False)
        except IOError:
            logging.warn(self.filename + ' could not be opened.  ' +
                         'This dictionary was not saved.')

    def clean(self):
        self.data_dict = cln.data_to_type(self.data_dict, dctc.floatcol,
                                          dctc.datecol, dctc.strcol)


def dict_update():
    for filename in os.listdir(csvpath):
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
