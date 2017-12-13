import logging
import os.path
import sys
import pandas as pd
import reporting.utils as utl
import reporting.dictcolumns as dctc

csvpath = 'ERROR REPORTS/'


class ErrorReport(object):
    def __init__(self, df, dic, pn, filename):
        utl.dir_check(csvpath)
        if str(filename) == 'nan':
            logging.error('No error report file provided.  Aborting.')
            sys.exit(0)
        self.df = df
        self.dic = dic
        self.pn = pn
        self.filename = filename
        self.data_err = None
        self.dictionary = None
        self.reset()

    def reset(self):
        self.dictionary = self.dic.get()
        self.data_err = self.create()
        self.write(self.filename)

    def create(self):
        if dctc.FPN not in self.df:
            logging.warning('Full Placement Name not in ' + self.filename +
                            '.  Delete that dictionary and try rebuilding.')
        data_err = pd.merge(self.df, self.dictionary, on=dctc.FPN,
                            how='left', indicator=True)
        data_err = data_err[data_err['_merge'] == 'left_only']
        if self.pn is None:
            data_err = data_err[[dctc.FPN]].drop_duplicates()
            data_err.columns = [dctc.FPN]
        else:
            data_err = data_err[[dctc.FPN, self.pn]].drop_duplicates()
            data_err.columns = [dctc.FPN, dctc.PN]
        return data_err

    def get(self):
        return self.data_err

    def write(self, filename):
        errfile = csvpath + filename
        if self.data_err.empty:
            try:
                os.remove(errfile)
                logging.info('All placements defined!  ' + filename +
                             ' was deleted.')
            except OSError:
                logging.info('All placements defined!')
        else:
            try:
                self.data_err.to_csv(errfile, index=False)
                logging.warning('Not all placements defined.  ' + filename +
                                ' was generated')
            except IOError:
                logging.warning(filename + 'cannot be opened.' +
                                '  It was not updated.')
