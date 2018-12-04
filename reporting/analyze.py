import logging
import pandas as pd
import reporting.utils as utl
import reporting.vmcolumns as vmc
import reporting.vendormatrix as vm
import reporting.dictcolumns as dctc


class Analyze(object):
    def __init__(self, df=pd.DataFrame(), file=None, matrix=None):
        self.df = df
        self.file = file
        self.matrix = matrix
        if self.df.empty and self.file:
            self.load_df_from_file()

    def load_df_from_file(self):
        self.df = utl.import_read_csv(self.file)

    def check_delivery(self, df):
        plan_names = self.matrix.vendor_set(vm.plan_key)[vmc.fullplacename]
        df = df.groupby(plan_names).apply(lambda x: 0 if x[dctc.PNC].sum() == 0
                                          else x[vmc.cost].sum() /
                                          x[dctc.PNC].sum())
        f_df = df[df > 1]
        if f_df.empty:
            logging.info('Nothing has delivered in full.')
        else:
            del_p = f_df.apply(lambda x: "{0:.2f}%".format(x*100))
            logging.info('The following have delivered in full: \n'
                         '{}'.format(del_p))
            o_df = f_df[f_df > 1.5]
            if not o_df.empty:
                del_p = o_df.apply(lambda x: "{0:.2f}%".format(x * 100))
                logging.info(
                    'The following have over-delivered: \n'
                    '{}'.format(del_p))

    def check_plan_error(self, df):
        plan_names = self.matrix.vendor_set(vm.plan_key)[vmc.fullplacename]
        er = self.matrix.vendor_set(vm.plan_key)[vmc.filenameerror]
        edf = utl.import_read_csv(er, utl.error_path)
        if edf.empty:
            logging.info('No Planned error.')
            return True
        edf[plan_names] = pd.DataFrame(
            edf[vmc.fullplacename].str.split('_').values.tolist(),
            columns=plan_names)
        for col in plan_names:
            df = df[df[col].isin(edf[col].values)]
        df = df[plan_names + [vmc.vendorkey]].drop_duplicates()
        for col in df.columns:
            df[col] = "'" + df[col] + "'"
        logging.info('Undefined placements have the following keys: \n'
                     '{}'.format(df))

    def do_all_analysis(self):
        self.check_delivery(self.df)
        self.check_plan_error(self.df)
