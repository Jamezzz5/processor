import logging
import os.path
import sys
import pandas as pd
import processor.reporting.utils as utl
import processor.reporting.dictcolumns as dctc

csv_path = utl.error_path


class ErrorReport(object):
    def __init__(self, df, dic, pn, filename, merge_col=dctc.FPN):
        utl.dir_check(csv_path)
        if str(filename) == 'nan':
            logging.error('No error report file provided.  Aborting.')
            sys.exit(0)
        self.df = df
        self.dic = dic
        self.pn = pn
        self.filename = filename
        self.merge_col = merge_col
        self.merge_df = None
        self.data_err = None
        self.dictionary = None
        self.reset()

    def reset(self):
        if isinstance(self.dic, pd.DataFrame):
            self.dictionary = self.dic
        else:
            self.dictionary = self.dic.get()
        self.data_err = self.create()
        self.write(self.filename)

    def create(self):
        if isinstance(self.merge_col, list):
            self.merge_df = pd.merge(self.df, self.dictionary,
                                     left_on=self.merge_col[0],
                                     right_on=self.merge_col[1],
                                     how='left', indicator=True)
            self.merge_col = self.merge_col[0]
        else:
            if self.merge_col not in self.df:
                logging.warning('Full Placement Name not in {}. Delete that '
                                'dictionary and try '
                                'rebuilding.'.format(self.filename))
            cols_to_merge = [x for x in [self.merge_col, self.pn]
                             if x in self.df.columns]
            self.merge_df = pd.merge(
                self.df[cols_to_merge], self.dictionary,
                on=self.merge_col, how='left', indicator=True)
        data_err = self.merge_df[self.merge_df['_merge'] == 'left_only']
        if self.pn is None:
            data_err = self.drop_error_df_duplicates(data_err,
                                                     [self.merge_col])
        else:
            data_err.loc[:, dctc.PN] = data_err.loc[:, self.pn]
            data_err = self.drop_error_df_duplicates(data_err,
                                                     [self.merge_col, dctc.PN])
        return data_err

    @staticmethod
    def drop_error_df_duplicates(data_err, merge_col):
        data_err = data_err[merge_col].drop_duplicates()
        data_err.columns = [merge_col]
        return data_err

    def get(self):
        return self.data_err

    def write(self, filename):
        error_file = os.path.join(csv_path, filename)
        if self.data_err.empty:
            try:
                os.remove(error_file)
                logging.info('All placements defined!  '
                             '{} was deleted.'.format(filename))
            except OSError:
                logging.info('All placements defined!')
        else:
            try:
                self.data_err.to_csv(error_file, index=False, encoding='utf-8')
                logging.warning('Not all placements defined.  {}'
                                ' was generated'.format(filename))
            except IOError:
                logging.warning('{} cannot be opened.'
                                '  It was not updated.'.format(filename))
