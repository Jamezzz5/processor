import os
import re
import shutil
import logging
import operator
import tarfile
import pandas as pd
import seaborn as sns
import datetime as dt
import reporting.utils as utl
import reporting.vmcolumns as vmc
import reporting.vendormatrix as vm
import reporting.dictcolumns as dctc


class Analyze(object):
    def __init__(self, df=pd.DataFrame(), file_name=None, matrix=None):
        self.df = df
        self.file_name = file_name
        self.matrix = matrix
        self.vc = ValueCalc()
        if self.df.empty and self.file_name:
            self.load_df_from_file()

    def load_df_from_file(self):
        self.df = utl.import_read_csv(self.file_name)

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
        df = df[df[dctc.PFPN].isin(edf[vmc.fullplacename].values)][
            plan_names + [vmc.vendorkey]].drop_duplicates()
        df = vm.full_placement_creation(df, None, dctc.FPN, plan_names)
        df = df[df[dctc.FPN].isin(edf[dctc.FPN].values)]
        df = utl.col_removal(df, None, [dctc.FPN])
        for col in df.columns:
            df[col] = "'" + df[col] + "'"
        df_dict = '\n'.join(['{}{}'.format(k, v)
                             for k, v in df.to_dict(orient='index').items()])
        logging.info('Undefined placements have the following keys: \n'
                     '{}'.format(df_dict))

    def backup_files(self):
        bu = os.path.join(utl.backup_path, dt.date.today().strftime('%Y%m%d'))
        logging.info('Backing up all files to {}'.format(bu))
        for path in [utl.backup_path, bu]:
            utl.dir_check(path)
        file_dicts = {'raw.gzip': self.df}
        for file_name, df in file_dicts.items():
            file_name = os.path.join(bu, file_name)
            df.to_csv(file_name, compression='gzip')
        for file_path in [utl.config_path, utl.dict_path, utl.raw_path]:
            file_name = '{}.tar.gz'.format(file_path.replace('/', ''))
            file_name = os.path.join(bu, file_name)
            tar = tarfile.open(file_name, "w:gz")
            tar.add(file_path, arcname=file_path.replace('/', ''))
            tar.close()
        for file_name in ['logfile.log']:
            new_file_name = os.path.join(bu, file_name)
            shutil.copy(file_name, new_file_name)
        logging.info('Successfully backed up files to {}'.format(bu))

    @staticmethod
    def make_heat_map(df, cost_cols=None, percent_cols=None):
        fig, axs = sns.plt.subplots(ncols=len(df.columns),
                                    gridspec_kw={'hspace': 0, 'wspace': 0})
        for idx, col in enumerate(df.columns):
            text_format = ",.0f"
            sns.heatmap(df[[col]], annot=True, fmt=text_format, linewidths=.5,
                        cbar=False, cmap="Blues", ax=axs[idx])
            if col in cost_cols:
                for t in axs[idx].texts:
                    t.set_text('$' + t.get_text())
            if idx != 0:
                axs[idx].set_ylabel('')
                axs[idx].get_yaxis().set_ticks([])
            else:
                labels = [val[:30] for val in reversed(list(df.index))]
                axs[idx].set_yticklabels(labels=labels)
            axs[idx].xaxis.tick_top()
        sns.plt.show()
        sns.plt.close()

    def generate_table(self, group, metrics, sort=None):
        base_metrics = [x for x in metrics if x not in self.vc.metric_names]
        calc_metrics = [x for x in metrics if x not in base_metrics]
        df = self.df.groupby(group)[base_metrics].sum()
        df = self.vc.calculate_all_metrics(calc_metrics, df)
        cost_cols = [x for x in metrics if metrics[x]]
        if sort:
            df = df.sort_values(sort, ascending=False)
        self.make_heat_map(df, cost_cols)

    def do_all_analysis(self):
        self.backup_files()
        self.check_delivery(self.df)
        self.check_plan_error(self.df)


class ValueCalc(object):
    file_name = os.path.join(utl.config_path, 'aly_grouped_metrics.csv')
    metric_name = 'Metric Name'
    formula = 'Formula'
    operations = {'+': operator.add, '-': operator.sub, '/': operator.truediv,
                  '*': operator.mul, '%': operator.mod, '^': operator.xor}

    def __init__(self):
        self.calculations = self.get_grouped_metrics()
        self.metric_names = [self.calculations[x][self.metric_name]
                             for x in self.calculations]
        self.parse_formulas()

    @staticmethod
    def get_default_metrics():
        metric_names = ['CTR', 'CPC', 'CPA', 'CPLP', 'CPBC', 'View to 100']
        formula = ['Clicks/Impressions', 'Net Cost Final/Clicks',
                   'Net Cost Final/Conv1_CPA', 'Net Cost Final/Landing Page',
                   'Net Cost Final/Button Click', 'Video Views 100/Video Views']
        df = pd.DataFrame({'Metric Name': metric_names, 'Formula': formula})
        return df

    def get_grouped_metrics(self):
        if os.path.isfile(self.file_name):
            df = pd.read_csv(self.file_name)
        else:
            df = self.get_default_metrics()
        calculations = df.to_dict(orient='index')
        return calculations

    def parse_formulas(self):
        for gm in self.calculations:
            formula = self.calculations[gm][self.formula]
            reg_operators = '([' + ''.join(self.operations.keys()) + '])'
            formula = re.split(reg_operators, formula)
            self.calculations[gm][self.formula] = formula

    def get_metric_formula(self, metric_name):
        f = [self.calculations[x][self.formula] for x in self.calculations if
             self.calculations[x][self.metric_name] == metric_name][0]
        return f

    def calculate_all_metrics(self, metric_names, df=None):
        for metric_name in metric_names:
            df = self.calculate_metric(metric_name, df)
        return df

    def calculate_metric(self, metric_name, df=None):
        col = metric_name
        formula = self.get_metric_formula(metric_name)
        current_op = None
        for item in formula:
            if current_op:
                df[col] = self.operations[current_op](df[col], df[item])
                current_op = None
            elif item in self.operations:
                current_op = item
            else:
                df[col] = df[item]
        return df
