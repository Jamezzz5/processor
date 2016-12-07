import logging
import os.path
import pandas as pd
import dictionary as dct

log = logging.getLogger()

csvpath = 'ERROR REPORTS/'


class ErrorReport(object):
    def __init__(self, df, dic, pn, filename):
        self.df = df
        self.dic = dic
        self.pn = pn
        self.filename = filename
        self.reset()

    def reset(self):
        self.dictionary = self.dic.get()
        self.data_err = self.create()
        self.write(self.filename)

    def create(self):
        data_err = pd.merge(self.df, self.dictionary, on=dct.FPN,
                            how='left', indicator=True)
        data_err = data_err[data_err['_merge'] == 'left_only']
        data_err = data_err[[dct.FPN, self.pn]].drop_duplicates()
        data_err.columns = [dct.FPN, dct.PN]
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
                logging.info('All placements defined!  ')
                next
        else:
            try:
                self.data_err.to_csv(errfile, index=False)
                logging.warn('Not all placements defined.  ' + filename +
                             ' was generated')
            except IOError:
                logging.warn(filename + 'cannot be opened.' +
                             '  It was not updated.')
