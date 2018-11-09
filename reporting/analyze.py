import logging
import reporting.utils as utl
import reporting.vmcolumns as vmc
import reporting.vendormatrix as vm
import reporting.dictcolumns as dctc


class Analyze(object):
    def __init__(self, df=None, file=None, matrix=None):
        self.df = df
        self.file = file
        self.matrix = matrix
        if not self.df and self.file:
            self.load_df_from_file()

    def load_df_from_file(self):
        self.df = utl.import_read_csv(self.file)

    def check_delivery(self):
        plan_names = self.matrix.vendor_set(vm.plan_key)[vmc.fullplacename]
        delivery = self.df.groupby(plan_names).apply(
            lambda x: 0 if x[dctc.PNC].sum() == 0
            else x[vmc.cost].sum() / x[dctc.PNC].sum())
        f_delivery = delivery[delivery > 1]
        if f_delivery.empty:
            logging.info('Nothing has delivered in full.')
        else:
            del_p = f_delivery.apply(lambda x: "{0:.2f}%".format(x*100))
            logging.info('The following have delivered in full: \n'
                         '{}'.format(del_p))
            o_delivery = f_delivery[f_delivery > 1.5]
            if not o_delivery.empty:
                del_p = o_delivery.apply(lambda x: "{0:.2f}%".format(x * 100))
                logging.info(
                    'The following have over-delivered: \n'
                    '{}'.format(del_p))

    def do_all_analysis(self):
        self.check_delivery()
