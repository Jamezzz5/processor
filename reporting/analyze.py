import os
import re
import json
import shutil
import logging
import operator
import tarfile
import numpy as np
import pandas as pd
import seaborn as sns
import datetime as dt
import reporting.calc as cal
import reporting.utils as utl
import reporting.vmcolumns as vmc
import reporting.expcolumns as exc
import reporting.vendormatrix as vm
import reporting.dictcolumns as dctc


class Analyze(object):
    date_col = 'date'
    delivery_col = 'delivery'
    under_delivery_col = 'under-delivery'
    full_delivery_col = 'full-delivery'
    over_delivery_col = 'over-delivery'
    unknown_col = 'unknown'
    delivery_comp_col = 'delivery_completion'
    raw_file_update_col = 'raw_file_update'
    topline_col = 'topline_metrics'
    lw_topline_col = 'last_week_topline_metrics'
    tw_topline_col = 'two_week_topline_merics'
    kpi_col = 'kpi_col'
    raw_columns = 'raw_file_columns'
    vk_metrics = 'vendor_key_metrics'
    vendor_metrics = 'vendor_metrics'
    missing_metrics = 'missing_metrics'
    flagged_metrics = 'flagged_metrics'
    placement_col = 'placement_col'
    max_api_length = 'max_api_length'
    double_counting_all = 'double_counting_all'
    double_counting_partial = 'double_counting_partial'
    missing_flat_costs = 'missing_flat_costs'
    missing_flat_clicks = 'missing_flat_clicks'
    analysis_dict_file_name = 'analysis_dict.json'
    analysis_dict_key_col = 'key'
    analysis_dict_data_col = 'data'
    analysis_dict_msg_col = 'message'
    analysis_dict_date_col = 'date'
    analysis_dict_param_col = 'parameter'
    analysis_dict_param_2_col = 'parameter_2'
    analysis_dict_filter_col = 'filter_col'
    analysis_dict_filter_val = 'filter_val'
    analysis_dict_split_col = 'split_col'
    analysis_dict_small_param_2 = 'Smallest'
    analysis_dict_large_param_2 = 'Largest'
    analysis_dict_only_param_2 = 'Only'

    def __init__(self, df=pd.DataFrame(), file_name=None, matrix=None):
        self.analysis_dict = []
        self.df = df
        self.file_name = file_name
        self.matrix = matrix
        self.vc = ValueCalc()
        if self.df.empty and self.file_name:
            self.load_df_from_file()

    def get_base_analysis_dict_format(self):
        analysis_dict_format = {
            self.analysis_dict_key_col: '',
            self.analysis_dict_data_col: {},
            self.analysis_dict_msg_col: '',
            self.analysis_dict_param_col: '',
            self.analysis_dict_param_2_col: '',
            self.analysis_dict_split_col: '',
            self.analysis_dict_filter_col: '',
            self.analysis_dict_filter_val: ''
        }
        return analysis_dict_format

    def load_df_from_file(self):
        self.df = utl.import_read_csv(self.file_name)

    def add_to_analysis_dict(self, key_col, message='', data='',
                             param='', param2='', split='',
                             filter_col='', filter_val=''):
        base_dict = self.get_base_analysis_dict_format()
        base_dict[self.analysis_dict_key_col] = str(key_col)
        base_dict[self.analysis_dict_msg_col] = str(message)
        base_dict[self.analysis_dict_param_col] = str(param)
        base_dict[self.analysis_dict_param_2_col] = str(param2)
        base_dict[self.analysis_dict_split_col] = str(split)
        base_dict[self.analysis_dict_filter_col] = str(filter_col)
        base_dict[self.analysis_dict_filter_val] = str(filter_val)
        base_dict[self.analysis_dict_data_col] = data
        self.analysis_dict.append(base_dict)

    def check_delivery(self, df):
        plan_names = self.matrix.vendor_set(vm.plan_key)[vmc.fullplacename]
        df = df.groupby(plan_names).apply(lambda x: 0 if x[dctc.PNC].sum() == 0
                                          else x[vmc.cost].sum() /
                                          x[dctc.PNC].sum())
        f_df = df[df > 1]
        if f_df.empty:
            delivery_msg = 'Nothing has delivered in full.'
            logging.info(delivery_msg)
            self.add_to_analysis_dict(key_col=self.delivery_col,
                                      param=self.under_delivery_col,
                                      message=delivery_msg)
        else:
            del_p = f_df.apply(lambda x: "{0:.2f}%".format(x * 100))
            delivery_msg = 'The following have delivered in full: '
            logging.info('{}\n{}'.format(delivery_msg, del_p))
            data = del_p.reset_index().rename(columns={0: 'Delivery'})
            self.add_to_analysis_dict(key_col=self.delivery_col,
                                      param=self.full_delivery_col,
                                      message=delivery_msg,
                                      data=data.to_dict())
            o_df = f_df[f_df > 1.5]
            if not o_df.empty:
                del_p = o_df.apply(lambda x: "{0:.2f}%".format(x * 100))
                delivery_msg = 'The following have over-delivered:'
                logging.info('{}\n{}'.format(delivery_msg, del_p))
                data = del_p.reset_index().rename(columns={0: 'Delivery'})
                self.add_to_analysis_dict(key_col=self.delivery_col,
                                          param=self.over_delivery_col,
                                          message=delivery_msg,
                                          data=data.to_dict())

    @staticmethod
    def get_rolling_mean_df(df, value_col, group_cols):
        pdf = pd.pivot_table(df, index=vmc.date, columns=group_cols,
                             values=value_col, aggfunc=np.sum)
        if len(pdf.columns) > 10000:
            logging.warning('Maximum 10K combos for calculation, data set '
                            'has {}'.format(len(pdf.columns)))
            return pd.DataFrame()
        df = pdf.unstack().reset_index().rename(columns={0: value_col})
        for x in [3, 7, 30]:
            ndf = pdf.rolling(
                window=x, min_periods=1).mean().unstack().reset_index().rename(
                columns={0: '{} rolling {}'.format(value_col, x)})
            df = df.merge(ndf, on=group_cols + [vmc.date])
        return df

    def project_delivery_completion(self, df):
        plan_names = self.matrix.vendor_set(vm.plan_key)[vmc.fullplacename]
        average_df = self.get_rolling_mean_df(
            df=df, value_col=vmc.cost, group_cols=plan_names)
        if average_df.empty:
            delivery_msg = ('Average df empty, maybe too large.  '
                            'Could not project delivery completion.')
            logging.warning(delivery_msg)
            self.add_to_analysis_dict(key_col=self.delivery_comp_col,
                                      message=delivery_msg)
            return False
        last_date = dt.datetime.strftime(
            dt.datetime.today() - dt.timedelta(days=1), '%Y-%m-%d')
        average_df = average_df[average_df[vmc.date] == last_date]
        average_df = average_df.drop(columns=[vmc.cost])
        df = df.groupby(plan_names)[vmc.cost, dctc.PNC].sum()
        df = df[df[dctc.PNC] - df[vmc.cost] > 0]
        df = df.reset_index()
        if df.empty:
            delivery_msg = ('Dataframe empty, likely no planned net costs.  '
                            'Could not project delivery completion.')
            logging.warning(delivery_msg)
            self.add_to_analysis_dict(key_col=self.delivery_comp_col,
                                      message=delivery_msg)
            return False
        df = df.merge(average_df, on=plan_names)
        df['days'] = (df[dctc.PNC] - df[vmc.cost]) / df[
            '{} rolling {}'.format(vmc.cost, 3)]
        df['days'] = df['days'].replace([np.inf, -np.inf], np.nan).fillna(10000)
        df['days'] = np.where(df['days'] > 10000, 10000, df['days'])
        df['completed_date'] = pd.to_datetime(df[vmc.date]) + pd.to_timedelta(
            np.ceil(df['days']).astype(int), unit='D')
        no_date_map = ((df['completed_date'] >
                        dt.datetime.today() + dt.timedelta(days=365)) |
                       (df['completed_date'] <
                        dt.datetime.today() - dt.timedelta(days=365)))
        df['completed_date'] = df['completed_date'].dt.strftime('%Y-%m-%d')
        df.loc[no_date_map, 'completed_date'] = 'Greater than 1 Year'
        df = df[plan_names + ['completed_date']]
        delivery_msg = 'Projected delivery completion dates are as follows:'
        logging.info('{}\n{}'.format(delivery_msg, df.to_string()))
        self.add_to_analysis_dict(key_col=self.delivery_comp_col,
                                  message=delivery_msg,
                                  data=df.to_dict())

    def check_raw_file_update_time(self):
        data_sources = self.matrix.get_all_data_sources()
        df = pd.DataFrame()
        for source in data_sources:
            file_name = source.p[vmc.filename]
            if os.path.exists(file_name):
                t = os.path.getmtime(file_name)
                last_update = dt.datetime.fromtimestamp(t)
                if last_update.date() == dt.datetime.today().date():
                    update_tier = 'Today'
                elif last_update.date() > (
                            dt.datetime.today() - dt.timedelta(days=7)).date():
                    update_tier = 'Within A Week'
                else:
                    update_tier = 'Greater Than One Week'
            else:
                last_update = 'Does Not Exist'
                update_tier = 'Never'
            data_dict = {'source': [source.key], 'update_time': [last_update],
                         'update_tier': [update_tier]}
            df = df.append(pd.DataFrame(data_dict),
                           ignore_index=True, sort=False)
        df['update_time'] = df['update_time'].astype('U')
        update_msg = 'Raw File update times and tiers are as follows:'
        logging.info('{}\n{}'.format(update_msg, df.to_string()))
        self.add_to_analysis_dict(key_col=self.raw_file_update_col,
                                  message=update_msg, data=df.to_dict())

    def check_plan_error(self, df):
        plan_names = self.matrix.vendor_set(vm.plan_key)[vmc.fullplacename]
        er = self.matrix.vendor_set(vm.plan_key)[vmc.filenameerror]
        edf = utl.import_read_csv(er, utl.error_path)
        if edf.empty:
            plan_error_msg = ('No Planned error - all {} '
                              'combinations are defined.'.format(plan_names))
            logging.info(plan_error_msg)
            self.add_to_analysis_dict(key_col=self.unknown_col,
                                      message=plan_error_msg)
            return True
        df = df[df[dctc.PFPN].isin(edf[vmc.fullplacename].values)][
            plan_names + [vmc.vendorkey]].drop_duplicates()
        df = vm.full_placement_creation(df, None, dctc.FPN, plan_names)
        df = df[df[dctc.FPN].isin(edf[dctc.FPN].values)]
        df = utl.col_removal(df, None, [dctc.FPN])
        for col in df.columns:
            df[col] = "'" + df[col] + "'"
        df = df.dropna()
        df_dict = '\n'.join(['{}{}'.format(k, v)
                             for k, v in df.to_dict(orient='index').items()])
        undefined_msg = 'Undefined placements have the following keys:'
        logging.info('{}\n{}'.format(undefined_msg, df_dict))
        self.add_to_analysis_dict(key_col=self.unknown_col,
                                  message=undefined_msg,
                                  data=df.to_dict())

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
        df = self.generate_df_table(group, metrics, sort)
        cost_cols = [x for x in metrics if metrics[x]]
        self.make_heat_map(df, cost_cols)

    def generate_df_table(self, group, metrics, sort=None, data_filter=None):
        base_metrics = [x for x in metrics if x not in self.vc.metric_names]
        calc_metrics = [x for x in metrics if x not in base_metrics]
        df = self.df.copy()
        if data_filter:
            filter_col = data_filter[0]
            filter_val = data_filter[1]
            if filter_col in df.columns:
                df = df[df[filter_col].isin(filter_val)]
            else:
                logging.warning('{} not in df columns'.format(filter_col))
                columns = group + metrics + [filter_col]
                return pd.DataFrame({x: [] for x in columns})
        for group_col in group:
            if group_col not in df.columns:
                logging.warning('{} not in df columns'.format(group))
                columns = group + metrics
                return pd.DataFrame({x: [] for x in columns})
        df = df.groupby(group)[base_metrics].sum()
        df = self.vc.calculate_all_metrics(calc_metrics, df)
        if sort:
            df = df.sort_values(sort, ascending=False)
        return df

    @staticmethod
    def give_df_default_format(df, columns=None):
        df = utl.give_df_default_format(df, columns)
        return df

    def get_table_without_format(self, data_filter=None, group=dctc.CAM):
        group = [group]
        metrics = []
        potential_metrics = [[cal.TOTAL_COST], [cal.NCF],
                             [vmc.impressions, 'CTR'], [vmc.clicks, 'CPC'],
                             [vmc.landingpage, 'CPLP'], [vmc.btnclick, 'CPBC'],
                             [vmc.purchase, 'CPP']]
        for metric in potential_metrics:
            if metric[0] in self.df.columns:
                metrics += metric
        df = self.generate_df_table(group=group, metrics=metrics,
                                    data_filter=data_filter)
        return df

    def generate_topline_metrics(self, data_filter=None, group=dctc.CAM):
        df = self.get_table_without_format(data_filter, group)
        df = self.give_df_default_format(df)
        df = df.transpose()
        log_info_text = ('Topline metrics are as follows: \n{}'
                         ''.format(df.to_string()))
        if data_filter:
            log_info_text = data_filter[2] + log_info_text
        logging.info(log_info_text)
        return df

    def calculate_kpi_trend(self, kpi, group, metrics):
        df = self.get_df_based_on_kpi(kpi, group, metrics, split=vmc.date)
        if len(df) < 2:
            logging.warning('Less than two datapoints for KPI {}'.format(kpi))
            return False
        df = df.sort_values(vmc.date).reset_index(drop=True).reset_index()
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
        fit = np.polyfit(df['index'], df[kpi], deg=1)
        if fit[0] > 0:
            trend = 'increasing'
        else:
            trend = 'decreasing'
        msg = ('The KPI {} is {} at a rate of {:,.3f} units per day'
               ' when given linear fit').format(kpi, trend, abs(fit[0]))
        logging.info(msg)
        df['fit'] = fit[0] * df['index'] + fit[1]
        df[vmc.date] = df[vmc.date].dt.strftime('%Y-%m-%d')
        self.add_to_analysis_dict(
            key_col=self.kpi_col, message=msg, data=df.to_dict(),
            param=kpi, param2='Trend', split=vmc.date)

    def explain_lowest_kpi_for_vendor(self, kpi, group, metrics, filter_col):
        min_val = self.find_in_analysis_dict(
            self.kpi_col, param=kpi, param_2=self.analysis_dict_small_param_2,
            split_col=dctc.VEN)
        if len(min_val) == 0:
            min_val = self.find_in_analysis_dict(
                self.kpi_col, param=kpi,
                param_2=self.analysis_dict_only_param_2, split_col=dctc.VEN)
            if len(min_val) == 0:
                return False
        min_val = min_val[0][self.analysis_dict_data_col][dctc.VEN].values()
        for val in min_val:
            for split in [dctc.CRE, dctc.TAR, dctc.PKD, dctc.PLD, dctc.ENV]:
                self.evaluate_smallest_largest_kpi(
                    kpi, group, metrics, split, filter_col, val, number=1)

    def get_df_based_on_kpi(self, kpi, group, metrics, split=None,
                            filter_col=None, filter_val=None, sort=None):
        if split:
            group = group + [split]
        if filter_col:
            group = group + [filter_col]
        if not sort:
            sort = kpi
        df = self.generate_df_table(group=group, metrics=metrics, sort=sort)
        df = df.reset_index().replace([np.inf, -np.inf], np.nan).fillna(0)
        df = df.loc[(df[dctc.KPI] == kpi) & (df[kpi].notnull()) & (df[kpi] > 0)]
        if filter_col:
            df = df.loc[(df[filter_col] == filter_val)]
        return df

    def evaluate_df_kpi_smallest_largest(self, df, kpi, split, filter_col,
                                         filter_val, small_large='Smallest'):
        format_df = self.give_df_default_format(df, columns=[kpi])
        if split == vmc.date:
            df[split] = df[split].dt.strftime('%Y-%m-%d')
        split_values = ', '.join(str(x) for x in df[split].values)
        msg = '{} value(s) for KPI {} broken out by {} are {}'.format(
            small_large, kpi, split, split_values)
        if filter_col:
            msg = '{} when filtered by the {} {}'.format(
                msg, filter_col, filter_val)
        log_info_text = ('{}\n{}'.format(msg, format_df.to_string()))
        logging.info(log_info_text)
        self.add_to_analysis_dict(
            key_col=self.kpi_col, message=msg, data=df.to_dict(),
            param=kpi, param2=small_large, split=split,
            filter_col=filter_col, filter_val=filter_val)

    def evaluate_smallest_largest_kpi(self, kpi, group, metrics, split=None,
                                      filter_col=None, filter_val=None,
                                      number=3):
        df = self.get_df_based_on_kpi(kpi, group, metrics, split, filter_col,
                                      filter_val)
        if df.empty:
            msg = ('Value(s) for KPI {} broken out by {} could '
                   'not be calculated'.format(kpi, split))
            if filter_col:
                msg = '{} when filtered by the {} {}'.format(
                    msg, filter_col, filter_val)
            logging.warning(msg)
            return False
        if len(df) < 2:
            df_list = [[df, self.analysis_dict_only_param_2]]
        else:
            smallest_df = df.nsmallest(n=number, columns=[kpi])
            largest_df = df.nlargest(n=number, columns=[kpi])
            df_list = [[smallest_df, self.analysis_dict_small_param_2],
                       [largest_df, self.analysis_dict_large_param_2]]
        for df in df_list:
            self.evaluate_df_kpi_smallest_largest(df[0], kpi, split, filter_col,
                                                  filter_val, df[1])

    def evaluate_on_kpi(self, kpi):
        kpi_formula = [
            self.vc.calculations[x] for x in self.vc.calculations
            if self.vc.calculations[x][self.vc.metric_name] == kpi]
        if kpi_formula:
            kpi_cols = kpi_formula[0][self.vc.formula][::2]
            metrics = kpi_cols + [kpi]
            missing_cols = [x for x in kpi_cols if x not in self.df.columns]
            if missing_cols:
                msg = 'Missing columns could not evaluate {}'.format(kpi)
                logging.warning(msg)
                self.add_to_analysis_dict(key_col=self.kpi_col,
                                          message=msg, param=kpi)
                return False
        elif kpi not in self.df.columns:
            msg = 'Unknown KPI: {}'.format(kpi)
            logging.warning(msg)
            self.add_to_analysis_dict(key_col=self.kpi_col,
                                      message=msg, param=kpi)
            return False
        else:
            metrics = [kpi]
        group = [dctc.CAM, dctc.KPI]
        self.evaluate_smallest_largest_kpi(kpi, group, metrics, split=dctc.VEN)
        self.explain_lowest_kpi_for_vendor(
            kpi=kpi, group=group, metrics=metrics, filter_col=dctc.VEN)
        self.evaluate_smallest_largest_kpi(kpi, group, metrics, split=vmc.date)
        self.calculate_kpi_trend(kpi, group, metrics)

    def evaluate_on_kpis(self):
        for kpi in self.df[dctc.KPI].unique():
            self.evaluate_on_kpi(kpi)

    def generate_topline_and_weekly_metrics(self, group=dctc.CAM):
        df = self.generate_topline_metrics(group=group)
        last_week_filter = [
            dt.datetime.strftime(
                (dt.datetime.today() - dt.timedelta(days=x)), '%Y-%m-%d')
            for x in range(1, 8)]
        tdf = self.generate_topline_metrics(
            data_filter=[vmc.date, last_week_filter, 'Last Weeks '],
            group=group)
        two_week_filter = [
            dt.datetime.strftime(
                (dt.datetime.today() - dt.timedelta(days=x)), '%Y-%m-%d')
            for x in range(8, 15)]
        twdf = self.generate_topline_metrics(
            data_filter=[vmc.date, two_week_filter, '2 Weeks Ago '],
            group=group)
        for val in [(self.topline_col, df), (self.lw_topline_col, tdf),
                    (self.tw_topline_col, twdf)]:
            msg = '{} as follows:'.format(val[0].replace('_', ' '))
            self.add_to_analysis_dict(key_col=self.topline_col,
                                      message=msg,
                                      data=val[1].to_dict(),
                                      param=val[0])
        return df, tdf, twdf

    def get_column_names_from_raw_files(self):
        data_sources = self.matrix.get_all_data_sources()
        df = pd.DataFrame()
        for source in data_sources:
            file_name = source.p[vmc.filename]
            first_row = source.p[vmc.firstrow]
            missing_cols = []
            if os.path.exists(file_name):
                tdf = utl.import_read_csv(file_name, nrows=first_row + 5)
                tdf = utl.first_last_adj(tdf, first_row, 0)
                cols = list(tdf.columns)
                active_metrics = source.get_active_metrics()
                for k, v in active_metrics.items():
                    for c in v:
                        if c not in cols:
                            missing_cols.append({k: c})
            else:
                cols = []
            data_dict = {vmc.vendorkey: [source.key], self.raw_columns: [cols],
                         'missing': [missing_cols]}
            df = df.append(pd.DataFrame(data_dict),
                           ignore_index=True, sort=False)
        update_msg = 'Columns and missing columns by key as follows:'
        logging.info('{}\n{}'.format(update_msg, df.to_string()))
        self.add_to_analysis_dict(key_col=self.raw_columns,
                                  message=update_msg, data=df.to_dict())

    def get_metrics_by_vendor_key(self):
        data_sources = self.matrix.get_all_data_sources()
        df = self.df.copy()
        metrics = []
        for source in data_sources:
            metrics.extend(source.get_active_metrics())
        metrics = list(set(metrics))
        metrics = [x for x in metrics if x in df.columns]
        agg_map = {x: [np.min, np.max] if (x == vmc.date) else np.sum
                   for x in metrics}
        df = df.groupby([vmc.vendorkey]).agg(agg_map)
        df.columns = [' - '.join(col).strip() for col in df.columns]
        df.columns = [x[:-6] if x[-6:] == ' - sum' else x for x in df.columns]
        df = df.reset_index()
        for col in [' - amin', ' - amax']:
            df[vmc.date + col] = df[vmc.date + col].astype('U')
        update_msg = 'Metrics by vendor key are as follows:'
        logging.info('{}\n{}'.format(update_msg, df.to_string()))
        self.add_to_analysis_dict(key_col=self.vk_metrics,
                                  message=update_msg, data=df.to_dict())

    def find_missing_metrics(self):
        df = self.get_table_without_format(group=dctc.VEN)
        format_df = self.give_df_default_format(df.copy())
        df = df.T
        update_msg = 'Metrics by vendor are as follows:'
        logging.info('{}\n{}'.format(update_msg, format_df.to_string()))
        self.add_to_analysis_dict(key_col=self.vendor_metrics,
                                  message=update_msg,
                                  data=format_df.T.to_dict())
        mdf = pd.DataFrame()
        for col in df.columns:
            missing_metrics = df[df[col] == 0][col].index.to_list()
            if missing_metrics:
                miss_dict = {dctc.VEN: col,
                             self.missing_metrics: missing_metrics}
                mdf = mdf.append(pd.DataFrame(miss_dict),
                                 ignore_index=True, sort=False)
        if mdf.empty:
            missing_msg = 'No vendors have missing metrics.'
            logging.info('{}'.format(missing_msg))
        else:
            missing_msg = 'The following vendors have missing metrics:'
            logging.info('{}\n{}'.format(missing_msg, mdf.to_string()))
        self.add_to_analysis_dict(key_col=self.missing_metrics,
                                  message=missing_msg, data=mdf.to_dict())

    def flag_errant_metrics(self):
        df = self.get_table_without_format(group=dctc.VEN)
        all_threshold = 'All'
        threshold_col = 'threshold'
        thresholds = {'CTR': {'Google SEM': 0.2, all_threshold: 0.06}}
        for metric_name, threshold_dict in thresholds.items():
            edf = df.copy()
            edf[threshold_col] = edf.index.map(threshold_dict).fillna(
                threshold_dict[all_threshold])
            edf = edf[edf['CTR'] > edf[threshold_col]]
            if not edf.empty:
                edf = edf[[metric_name, threshold_col]]
                edf = edf.replace([np.inf, -np.inf], np.nan).fillna(0)
                flagged_msg = ('The following vendors have unusually high {}s'
                               '.'.format(metric_name))
                logging.info('{}\n{}'.format(
                    flagged_msg, edf.to_string()))
                self.add_to_analysis_dict(
                    key_col=self.flagged_metrics, param=metric_name,
                    message=flagged_msg, data=edf.to_dict())

    def check_api_date_length(self):
        vk_list = []
        data_sources = self.matrix.get_all_data_sources()
        max_date_dict = {
            vmc.api_amz_key: 60, vmc.api_szk_key: 60, vmc.api_db_key: 60,
            vmc.api_tik_key: 30, vmc.api_ttd_key: 80, vmc.api_sc_key: 30}
        data_sources = [x for x in data_sources if 'API_' in x.key]
        for ds in data_sources:
            if 'API_' in ds.key:
                key = ds.key.split('_')[1]
                if key in max_date_dict.keys():
                    max_date = max_date_dict[key]
                    date_range = (ds.p[vmc.enddate] - ds.p[vmc.startdate]).days
                    if date_range > (max_date - 3):
                        vk_list.append(ds.key)
        mdf = pd.DataFrame({vmc.vendorkey:  vk_list})
        mdf[self.max_api_length] = ''
        if vk_list:
            msg = 'The following APIs are within 3 days of their max length:'
            logging.info('{}\n{}'.format(msg, vk_list))
            mdf[self.max_api_length] = mdf[vmc.vendorkey].str.split(
                '_').str[1].replace(max_date_dict)
        else:
            msg = 'No APIs within 3 days of max length.'
            logging.info('{}'.format(msg))
        self.add_to_analysis_dict(key_col=self.max_api_length,
                                  message=msg, data=mdf.to_dict())

    @staticmethod
    def processor_clean_functions(df, cd, cds_name, clean_functions):
        success = True
        for text, clean_func in clean_functions.items():
            if not success:
                msg = 'A previous step failed to process.'
                cd[text][cds_name] = (False, msg)
                continue
            try:
                df = clean_func(df)
                msg = 'Successfully able to {}'.format(text)
                cd[text][cds_name] = (True, msg)
            except Exception as e:
                msg = 'Could not {} with error: {}'.format(text, e)
                cd[text][cds_name] = (False, msg)
                success = False
        return df, cd, success

    @staticmethod
    def compare_start_end_date_raw(df, cd, cds_name, cds, vk='vk'):
        df = df.copy()
        df[vmc.vendorkey] = vk
        date_col_name = cds.p[vmc.date][0]
        if str(date_col_name) == 'nan' or date_col_name not in df.columns:
            msg = 'Date not specified or not column names.'
            msg = (False, msg)
        else:
            df[vmc.date] = df[cds.p[vmc.date][0]]
            df = utl.data_to_type(df=df, date_col=vmc.datadatecol)
            df = df[[vmc.vendorkey, vmc.date]].groupby([vmc.vendorkey]).agg(
                {vmc.date: [np.min, np.max]})
            df.columns = [' - '.join(col).strip() for col in df.columns]
            tdf = df.reset_index()
            max_date = tdf['{} - amax'.format(vmc.date)][0].date()
            min_date = tdf['{} - amin'.format(vmc.date)][0].date()
            sd = cds.p[vmc.startdate].date()
            ed = cds.p[vmc.enddate].date()
            if max_date < sd:
                msg = ('Last day in raw file {} is less than start date {}.\n'
                       'Result will be blank.  Change start date.'.format(
                         max_date, sd))
                msg = (False, msg)
            elif min_date > ed:
                msg = ('First day in raw file {} is less than end date {}.\n'
                       'Result will be blank.  Change end date.'.format(
                         min_date, ed))
                msg = (False, msg)
            else:
                msg = ('Some or all data in raw file with date range {} - {} '
                       'falls between start and end dates {} - {}'.format(
                         sd, ed, min_date, max_date))
                msg = (True, msg)
        cd[vmc.startdate][cds_name] = msg
        return cd

    def check_raw_file_against_plan_net(self, df, cd, cds_name):
        plan_df = self.matrix.vendor_get(vm.plan_key)
        if plan_df.empty:
            msg = (False, 'Plan net is empty could not check.')
        else:
            plan_names = self.matrix.vendor_set(vm.plan_key)[vmc.fullplacename]
            df = vm.full_placement_creation(df, None, dctc.FPN, plan_names)
            missing = [x for x in df[dctc.FPN].unique()
                       if x not in plan_df[dctc.FPN].unique()]
            if not missing:
                msg = (True, 'All values defined in plan net.')
            else:
                missing = ', '.join(missing)
                msg = (False, 'The following values were not in the plan net '
                              'dictionary: {}'.format(missing))
        cd[vm.plan_key][cds_name] = msg
        return cd

    @staticmethod
    def write_raw_file_dict(vk, cd):
        utl.dir_check(utl.tmp_file_suffix)
        file_name = '{}.json'.format(vk)
        file_name = os.path.join(utl.tmp_file_suffix, file_name)
        with open(file_name, 'w') as fp:
            json.dump(cd, fp)

    @staticmethod
    def check_combine_col_totals(cd, df, cds_name, c_cols):
        for col in c_cols:
            if col in df.columns:
                total = df[col].sum()
                if total <= 0:
                    msg = (False, 'Sum of column {} was {}'.format(col, total))
                else:
                    msg = (True, int(total))
                if cds_name == 'New':
                    old_total = cd[col]['Old'][1]
                    if (not isinstance(old_total, str) and
                            not isinstance(total, str) and old_total > total):
                        msg = (
                            False, 'Old file total {} was greater than new '
                                   'file total {} for col {}'.format(
                                      old_total, total, col))
                cd[col][cds_name] = msg
        return cd

    @staticmethod
    def get_base_raw_file_dict(ds):
        cd = {'file_load': {},
              vmc.fullplacename: {},
              vmc.placement: {},
              vmc.date: {},
              'empty': {},
              vmc.startdate: {}}
        c_cols = [x for x in vmc.datafloatcol if ds.p[x] != ['nan']]
        clean_functions = {
            'get and merge dictionary': ds.get_and_merge_dictionary,
            'combine data': ds.combine_data,
            'remove cols and make calculations':
                ds.remove_cols_and_make_calculations}
        for x in c_cols + list(clean_functions.keys()) + [vm.plan_key]:
            cd[x] = {}
        return cd, clean_functions, c_cols

    def compare_raw_files(self, vk):
        ds = self.matrix.get_data_source(vk)
        tds = self.matrix.get_data_source(vk)
        file_type = os.path.splitext(ds.p[vmc.filename_true])[1]
        tmp_file = ds.p[vmc.filename_true].replace(
            file_type, '{}{}'.format(utl.tmp_file_suffix, file_type))
        tds.p[vmc.filename_true] = tmp_file
        tds.p[vmc.filename] = tmp_file
        cd, clean_functions, c_cols = self.get_base_raw_file_dict(ds)
        for cds_name, cds in {'Old': ds, 'New': tds}.items():
            try:
                df = cds.get_raw_df()
            except Exception as e:
                logging.warning('Unknown exception: {}'.format(e))
                if cds_name == 'New':
                    msg = 'Please open the file in excel, select all columns, '
                    'select General in the Number format dropdown, '
                    'save as a csv and retry.'
                else:
                    msg = ('The old file may not exist.  '
                           'Please save the new file.')
                cd['file_load'][cds_name] = (
                    False,
                    '{} file could not be loaded.  {}'.format(cds_name, msg))
                continue
            cd['file_load'][cds_name] = (True, 'File was successfully read.')
            for col in [vmc.fullplacename, vmc.placement, vmc.date] + c_cols:
                cols_to_check = ds.p[col]
                if col == vmc.placement:
                    cols_to_check = [ds.p[col]]
                missing_cols = [x for x in cols_to_check
                                if x.replace('::', '') not in df.columns]
                if missing_cols:
                    msg = (False,
                           'Columns specified in the {} are not in the'
                           ' new file those columns are: '
                           '{}'.format(col, ','.join(missing_cols)))
                else:
                    msg = (True, '{} columns are in the raw file.'.format(col))
                cd[col][cds_name] = msg
            if df is None or df.empty:
                msg = '{} file is empty skipping checks.'.format(cds_name)
                cd['empty'][cds_name] = (False, msg)
                continue
            total_mb = int(round(df.memory_usage(index=True).sum() / 1000000))
            msg = '{} file has {} rows and is {}MB.'.format(
                cds_name, len(df.index), total_mb)
            cd['empty'][cds_name] = (True, msg)
            cd = self.compare_start_end_date_raw(df, cd, cds_name, cds, vk)
            df, cd, success = self.processor_clean_functions(
                df, cd, cds_name, clean_functions)
            if not success:
                for col in [vm.plan_key]:
                    msg = ('Could not fully process files so no '
                           'additional checks could be made.')
                    cd[col][cds_name] = (False, msg)
            cd = self.check_combine_col_totals(cd, df, cds_name, c_cols)
            cd = self.check_raw_file_against_plan_net(df, cd, cds_name)
            cds.df = df
        self.write_raw_file_dict(vk, cd)

    def find_placement_name_column(self):
        data_sources = self.matrix.get_all_data_sources()
        df = pd.DataFrame()
        for source in data_sources:
            file_name = source.p[vmc.filename]
            first_row = source.p[vmc.firstrow]
            p_col = source.p[vmc.placement]
            if os.path.exists(file_name):
                tdf = utl.import_read_csv(file_name, nrows=first_row + 3)
                tdf = utl.first_last_adj(tdf, first_row, 0)
                if tdf.empty:
                    continue
                tdf = tdf.applymap(lambda x: str(x).count('_'))\
                    .apply(lambda x: sum(x))
                r_col = tdf.idxmax(axis=1)
                if r_col > p_col:
                    data_dict = {vmc.vendorkey: [source.key],
                                 'Current Placement Col:': p_col,
                                 'Suggested:': r_col}
                    df = df.append(pd.DataFrame(data_dict),
                                   ignore_index=True, sort=False)
        update_msg = 'The following data sources have more breakouts in ' \
                     'another column. Consider changing placement name source:'
        logging.info('{}\n{}'.format(update_msg, df.to_string()))
        self.add_to_analysis_dict(key_col=self.placement_col,
                                  message=update_msg, data=df.to_dict())

    def find_metric_double_counting(self):
        rdf = pd.DataFrame()
        groups = [dctc.VEN, vmc.vendorkey, dctc.PN]
        metrics = [cal.NCF, vmc.impressions, vmc.clicks, vmc.views,
                   vmc.views25, vmc.views50, vmc.views75, vmc.views100]
        metrics = [metric for metric in metrics if metric in self.df.columns]
        df = self.generate_df_table(groups, metrics, sort=None,
                                    data_filter=None)
        df.reset_index(inplace=True)
        sdf = df.groupby([dctc.VEN, vmc.vendorkey]).size()
        sdf = sdf.reset_index().rename(columns={0: 'Total Num Placements'})
        sdf = sdf.groupby(dctc.VEN).max().reset_index()
        df = df[df.duplicated(subset=dctc.PN, keep=False)]
        if df.empty:
            return None
        for metric in metrics:
            tdf = df[df[metric] > 0]
            tdf = tdf[tdf.duplicated(subset=dctc.PN, keep=False)]
            if not tdf.empty:
                tdf = tdf.groupby([dctc.VEN, vmc.vendorkey]).size()
                tdf = tdf.reset_index().rename(columns={0: 'Num Duplicates'})
                tdf['Metric'] = metric
                rdf = pd.concat([rdf, tdf], ignore_index=True)
        if rdf.empty:
            return None
        rdf = sdf[[dctc.VEN, 'Total Num Placements']].merge(
            rdf, how='inner', on=dctc.VEN)
        rdf = rdf.groupby([dctc.VEN, 'Metric', 'Total Num Placements',
                           'Num Duplicates'])[vmc.vendorkey].apply(
            lambda x: ','.join(x)).reset_index()
        rdf = rdf.groupby([dctc.VEN, 'Metric', vmc.vendorkey,
                           'Num Duplicates']).max().reset_index()
        adf = rdf[rdf['Total Num Placements'] == rdf['Num Duplicates']]
        pdf = rdf[rdf['Total Num Placements'] > rdf['Num Duplicates']]
        if not adf.empty:
            msg = ('The following vendors are double counting metrics on all '
                   'placements from the following sources:')
            logging.info('{}\n{}'.format(msg, adf.to_string()))
            self.add_to_analysis_dict(
                key_col=self.double_counting_all,
                message=msg, data=adf.to_dict())
        if not pdf.empty:
            msg = ('The following vendors are double counting metrics on some '
                   'placements from the following sources. Check only '
                   'untracked placements are being uploaded in rawfiles:')
            logging.info('{}\n{}'.format(msg, pdf.to_string()))
            self.add_to_analysis_dict(
                key_col=self.double_counting_partial,
                message=msg, data=pdf.to_dict())

    def find_missing_flat_spend(self):
        groups = [dctc.VEN, dctc.PKD, dctc.PD, dctc.BM, vmc.date]
        metrics = [cal.NCF, vmc.clicks]
        metrics = [metric for metric in metrics if metric in self.df.columns]
        df = self.generate_df_table(groups, metrics, sort=None,
                                    data_filter=None)
        df.reset_index(inplace=True)
        df = df[(df[dctc.BM] == 'Flat') | (df[dctc.BM] == 'FLAT')]
        if df.empty:
            return None
        tdf = df[df[vmc.clicks] > 0]
        tdf = tdf.groupby([dctc.VEN, dctc.PKD, dctc.PD, dctc.BM]).min()
        tdf.reset_index(inplace=True)
        df = df.groupby([dctc.VEN, dctc.PKD, dctc.PD, dctc.BM]).sum()
        df.reset_index(inplace=True)
        df[dctc.PD] = pd.to_datetime(df[dctc.PD], format='%Y-%m-%d %H:%M:%S')
        df = df[(df[cal.NCF] == 0) & (df[dctc.PD] <= dt.datetime.today())]
        df = df.merge(tdf.drop_duplicates(),
                      on=[dctc.VEN, dctc.PKD, dctc.PD, dctc.BM, cal.NCF],
                      how='left', indicator=True)
        df = df.drop(columns=['Clicks_y'])
        df = df.rename(columns={'Date': 'First Click Date',
                                'Clicks_x': 'Clicks'})
        cdf = df[df['_merge'] == 'both']
        ndf = df[df['_merge'] == 'left_only']
        if not cdf.empty:
            cdf = cdf.iloc[:, :-1]
            msg = ('The following flat packages have passed their placement '
                   'date and have no net cost:')
            logging.info('{}\n{}'.format(msg, cdf.to_string()))
            self.add_to_analysis_dict(
                key_col=self.missing_flat_costs,
                message=msg, data=cdf.to_dict())
        if not ndf.empty:
            ndf = ndf[[dctc.VEN, dctc.PKD, dctc.PD]]
            msg = ('The following flat packages have passed their placement '
                   'date and have no net cost nor associated clicks:')
            logging.info('{}\n{}'.format(msg, ndf.to_string()))
            self.add_to_analysis_dict(
                key_col=self.missing_flat_clicks,
                message=msg, data=ndf.to_dict())
        return True

    def find_in_analysis_dict(self, key, param=None, param_2=None,
                              split_col=None, filter_col=None, filter_val=None):
        item = [x for x in self.analysis_dict
                if x[self.analysis_dict_key_col] == key]
        if param:
            item = [x for x in item if x[self.analysis_dict_param_col] == param]
        if param_2:
            item = [x for x in item if
                    x[self.analysis_dict_param_2_col] == param_2]
        if split_col:
            item = [x for x in item if
                    x[self.analysis_dict_split_col] == split_col]
        if filter_col:
            item = [x for x in item if
                    x[self.analysis_dict_filter_col] == filter_col]
        if filter_val:
            item = [x for x in item if
                    x[self.analysis_dict_filter_val] == filter_val]
        return item

    def write_analysis_dict(self):
        with open(self.analysis_dict_file_name, 'w') as fp:
            json.dump(self.analysis_dict, fp)

    def do_all_analysis(self):
        self.backup_files()
        self.check_delivery(self.df)
        self.check_plan_error(self.df)
        self.project_delivery_completion(self.df)
        self.check_raw_file_update_time()
        self.generate_topline_and_weekly_metrics()
        self.evaluate_on_kpis()
        self.get_column_names_from_raw_files()
        self.get_metrics_by_vendor_key()
        self.find_missing_metrics()
        self.flag_errant_metrics()
        self.find_placement_name_column()
        self.find_metric_double_counting()
        self.find_missing_flat_spend()
        self.check_api_date_length()
        self.write_analysis_dict()


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
        metric_names = ['CTR', 'CPC', 'CPA', 'CPLP', 'CPBC', 'View to 100',
                        'CPCV', 'CPLPV', 'CPP', 'CPM']
        formula = ['Clicks/Impressions', 'Net Cost Final/Clicks',
                   'Net Cost Final/Conv1_CPA', 'Net Cost Final/Landing Page',
                   'Net Cost Final/Button Click', 'Video Views 100/Video Views',
                   'Net Cost Final/Video Views', 'Net Cost Final/Landing Page',
                   'Net Cost Final/Purchase', 'Net Cost Final/Impressions']
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

    def calculate_all_metrics(self, metric_names, df=None, db_translate=None):
        if db_translate:
            tdf = pd.read_csv(os.path.join('config', 'db_df_translation.csv'))
            db_translate = dict(
                zip(tdf[exc.translation_df], tdf[exc.translation_db]))
        for metric_name in metric_names:
            df = self.calculate_metric(metric_name, df,
                                       db_translate=db_translate)
        return df

    def calculate_metric(self, metric_name, df=None, db_translate=None):
        col = metric_name
        formula = self.get_metric_formula(metric_name)
        current_op = None
        if db_translate:
            formula = [db_translate[x] if x in formula[::2] else x
                       for x in formula]
        for item in formula:
            if item.lower() == 'impressions' and 'Clicks' not in formula:
                df[item] = df[item] / 1000
            if current_op:
                if col in df and item in df:
                    df[col] = self.operations[current_op](df[col], df[item])
                    current_op = None
                else:
                    logging.warning('{} missing could not calc.'.format(item))
                    return df
            elif item in self.operations:
                current_op = item
            else:
                df[col] = df[item]
        return df
