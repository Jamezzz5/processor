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
import reporting.dictionary as dct
import reporting.vendormatrix as vm
import reporting.dictcolumns as dctc


class Analyze(object):
    date_col = 'date'
    database_cache = 'database_cache'
    delivery_col = 'delivery'
    under_delivery_col = 'under-delivery'
    full_delivery_col = 'full-delivery'
    over_delivery_col = 'over-delivery'
    unknown_col = 'unknown'
    delivery_comp_col = 'delivery_completion'
    daily_delivery_col = 'daily_delivery'
    over_daily_pace = 'over_daily_pace'
    under_daily_pace = 'under_daily_pace'
    adserving_alert = 'adserving_alert'
    daily_pacing_alert = 'daily_pacing'
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
    missing_flat = 'missing_flat'
    missing_serving = 'missing_serving'
    missing_ad_rate = 'missing_ad_rate'
    package_cap = 'package_cap'
    package_vendor = 'package_vendor'
    package_vendor_good = 'package_vendor_good'
    package_vendor_bad = 'package_vendor_bad'
    cap_name = 'cap_name'
    change_auto_order = 'change_auto_order'
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
    fixes_to_run = False

    def __init__(self, df=pd.DataFrame(), file_name=None, matrix=None):
        self.analysis_dict = []
        self.df = df
        self.file_name = file_name
        self.matrix = matrix
        self.vc = ValueCalc()
        self.class_list = [
            CheckColumnNames, FindPlacementNameCol, CheckAutoDictOrder,
            CheckApiDateLength, CheckFlatSpends, CheckDoubleCounting,
            GetPacingAnalysis, GetDailyDelivery, GetServingAlerts,
            GetDailyPacingAlerts, CheckPackageCapping]
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
        plan_names = self.matrix.vendor_set(vm.plan_key)
        if not plan_names:
            logging.warning('VM does not have plan key')
            return False
        plan_names = plan_names[vmc.fullplacename]
        miss_cols = [x for x in plan_names if x not in df.columns]
        if miss_cols:
            logging.warning('Df does not have cols {}'.format(miss_cols))
            return False
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
    def get_start_end_dates(df, plan_names):
        """
        Gets start and end dates at the level of the planned net full placement
        name. Dates taken from mediaplan where available, else from
        vendormatrix based on which vendorkey has more spend.

        :param df: full output df
        :param plan_names: planned net full placement columns
        :returns: two dfs w/ start and end dates for each unique breakout
        """
        matrix = vm.VendorMatrix().vm_df
        matrix = matrix[[vmc.vendorkey, vmc.startdate, vmc.enddate]]
        matrix = matrix.rename(columns={vmc.startdate: dctc.SD,
                                        vmc.enddate: dctc.ED})
        matrix = utl.data_to_type(matrix, date_col=[dctc.SD, dctc.ED],
                                  fill_empty=False)
        matrix[[dctc.SD, dctc.ED]] = matrix[[dctc.SD, dctc.ED]].fillna(pd.NaT)
        matrix[[dctc.SD, dctc.ED]] = matrix[
            [dctc.SD, dctc.ED]].replace([""], pd.NaT)
        vm_dates = df[plan_names + [vmc.vendorkey, vmc.cost]]
        vm_dates = vm_dates.merge(matrix, how='left', on=vmc.vendorkey)
        vm_dates = vm_dates.groupby(
            plan_names + [vmc.vendorkey]).agg(
            {vmc.cost: 'sum', dctc.SD: 'min', dctc.ED: 'max'}).reset_index()
        vm_dates = vm_dates[vm_dates[vmc.cost] > 0]
        vm_dates = vm_dates.groupby(plan_names).agg(
            {dctc.SD: 'min', dctc.ED: 'max'}).reset_index()
        if dctc.SD in df.columns and dctc.ED in df.columns:
            start_end_dates = df[plan_names + [dctc.SD, dctc.ED]]
            start_end_dates = start_end_dates.groupby(plan_names).agg(
                {dctc.SD: 'min', dctc.ED: 'max'})
            start_end_dates = start_end_dates.reset_index()
        else:
            start_end_dates = vm_dates[plan_names + [dctc.SD, dctc.ED]]
        start_end_dates = start_end_dates[start_end_dates.apply(
            lambda x: (x[dctc.SD] != x[dctc.ED]
                       and not pd.isnull(x[dctc.SD])
                       and not pd.isnull(x[dctc.ED])), axis=1)]
        vm_dates[plan_names] = vm_dates[plan_names].astype(object)
        vm_dates = vm_dates.merge(
            start_end_dates, how='left', on=plan_names, indicator=True)
        vm_dates = vm_dates[vm_dates['_merge'] == 'left_only']
        vm_dates = vm_dates.drop(
            columns=['_merge', 'mpStart Date_y', 'mpEnd Date_y'])
        vm_dates = vm_dates.rename(columns={'mpStart Date_x': dctc.SD,
                                            'mpEnd Date_x': dctc.ED})
        start_end_dates = pd.concat([start_end_dates, vm_dates])
        start_end_dates = utl.data_to_type(start_end_dates,
                                           date_col=[dctc.SD, dctc.ED],
                                           fill_empty=False)
        start_dates = start_end_dates[plan_names + [dctc.SD]]
        end_dates = start_end_dates[plan_names + [dctc.ED]]
        return start_dates, end_dates

    def check_raw_file_update_time(self):
        data_sources = self.matrix.get_all_data_sources()
        df = pd.DataFrame()
        for source in data_sources:
            if vmc.filename not in source.p:
                continue
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
        if df.empty:
            return False
        df['update_time'] = df['update_time'].astype('U')
        update_msg = 'Raw File update times and tiers are as follows:'
        logging.info('{}\n{}'.format(update_msg, df.to_string()))
        self.add_to_analysis_dict(key_col=self.raw_file_update_col,
                                  message=update_msg, data=df.to_dict())

    def get_plan_names(self):
        plan_names = self.matrix.vendor_set(vm.plan_key)
        if not plan_names:
            logging.warning('VM does not have plan key')
            plan_names = None
        else:
            plan_names = plan_names[vmc.fullplacename]
        return plan_names

    def check_plan_error(self, df):
        plan_names = self.get_plan_names()
        if not plan_names:
            return False
        er = self.matrix.vendor_set(vm.plan_key)[vmc.filenameerror]
        edf = utl.import_read_csv(er, utl.error_path)
        if edf.empty:
            plan_error_msg = ('No Planned error - all {} '
                              'combinations are defined.'.format(plan_names))
            logging.info(plan_error_msg)
            self.add_to_analysis_dict(key_col=self.unknown_col,
                                      message=plan_error_msg)
            return True
        if dctc.PFPN not in df.columns:
            logging.warning('Df does not have column: {}'.format(dctc.PFPN))
            return False
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
        undefined_msg = 'Missing planned spends have the following keys:'
        logging.info('{}\n{}'.format(undefined_msg, df_dict))
        self.add_to_analysis_dict(key_col=self.unknown_col,
                                  message=undefined_msg,
                                  data=df.to_dict())

    def backup_files(self):
        bu = os.path.join(utl.backup_path, dt.date.today().strftime('%Y%m%d'))
        logging.info('Backing up all files to {}'.format(bu))
        dir_to_backup = [utl.config_path, utl.dict_path, utl.raw_path]
        for path in [utl.backup_path, bu] + dir_to_backup:
            utl.dir_check(path)
        file_dicts = {'raw.gzip': self.df}
        for file_name, df in file_dicts.items():
            file_name = os.path.join(bu, file_name)
            df.to_csv(file_name, compression='gzip')
        for file_path in dir_to_backup:
            file_name = '{}.tar.gz'.format(file_path.replace('/', ''))
            file_name = os.path.join(bu, file_name)
            tar = tarfile.open(file_name, "w:gz")
            tar.add(file_path, arcname=file_path.replace('/', ''))
            tar.close()
        for file_name in ['logfile.log']:
            if os.path.exists(file_name):
                new_file_name = os.path.join(bu, file_name)
                shutil.copy(file_name, new_file_name)
        logging.info('Successfully backed up files to {}'.format(bu))

    # noinspection PyUnresolvedReferences
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

    def generate_df_table(self, group, metrics, sort=None, data_filter=None,
                          df=pd.DataFrame()):
        base_metrics = [x for x in metrics if x not in self.vc.metric_names]
        calc_metrics = [x for x in metrics if x not in base_metrics]
        if df.empty:
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
        if dctc.KPI in self.df.columns:
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

    def get_metrics_by_vendor_key(self):
        data_sources = self.matrix.get_all_data_sources()
        df = self.df.copy()
        if df.empty:
            logging.warning('Dataframe empty could not get metrics.')
            return False
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
        return True

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
        if df.empty:
            logging.warning('Dataframe empty, could not determine flags.')
            return False
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
        return True

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
                    if 'Old' not in cd[col]:
                        old_total = 0
                    else:
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

    @staticmethod
    def check_sheet_names(tds, sheet_names):
        missing_sheets = []
        xl = pd.read_excel(tds.p[vmc.filename], None)
        sheet_lists = list(xl.keys())
        for sheet_name in sheet_names:
            if sheet_name not in sheet_lists:
                missing_sheets.append(sheet_name)
        return missing_sheets

    def compare_raw_files(self, vk):
        ds = self.matrix.get_data_source(vk)
        tds = self.matrix.get_data_source(vk)
        file_type = os.path.splitext(ds.p[vmc.filename_true])[1]
        tmp_file = ds.p[vmc.filename_true].replace(
            file_type, '{}{}'.format(utl.tmp_file_suffix, file_type))
        tds.p[vmc.filename_true] = tmp_file
        tds.p[vmc.filename] = tmp_file
        missing_sheets = []
        if ':::' in ds.p[vmc.filename]:
            sheet_names = ds.p[vmc.filename].split(':::')[1:]
            sheet_info = ':::' + ':::'.join(sheet_names)
            missing_sheets = self.check_sheet_names(tds, sheet_names)
            tds.p[vmc.filename] += sheet_info
        cd, clean_functions, c_cols = self.get_base_raw_file_dict(ds)
        for cds_name, cds in {'Old': ds, 'New': tds}.items():
            try:
                df = cds.get_raw_df()
            except Exception as e:
                logging.warning('Unknown exception: {}'.format(e))
                if cds_name == 'New':
                    if missing_sheets:
                        missing_sheets = ', '.join(missing_sheets).upper()
                        msg = ('Xlsx file is missing the following sheets: '
                               '{}. Rename sheets if naming is wrong. Else, '
                               'check w/ vendor to get all needed sheets.'
                               ).format(missing_sheets)
                    else:
                        msg = 'Please open the file in excel, select all '
                        'columns, select General in the Number format '
                        'dropdown, save as a csv and retry.'
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

    def find_missing_serving(self):
        groups = [vmc.vendorkey, dctc.SRV, dctc.AM, dctc.PN]
        metrics = []
        serving_vals = ['1x1 Click & Imp', '1x1 Click Only', 'In-Banner',
                        'In-Stream Video', 'No Tracking', 'Rich Media',
                        'Standard', 'VAST', 'VPAID']
        df = self.generate_df_table(groups, metrics, sort=None,
                                    data_filter=None)
        df = df.reset_index()
        if df.empty:
            logging.warning('Dataframe empty, '
                            'could not determine missing serving.')
            return False
        df = df[(df[vmc.vendorkey].str.contains(vmc.api_dc_key)) |
                (df[vmc.vendorkey].str.contains(vmc.api_szk_key))]
        df = df[(df[dctc.AM] == 'nan') | (df[dctc.AM] == 0) |
                (df[dctc.AM].isnull())]
        df = df[~df[dctc.SRV].isin(serving_vals)]
        df = df.astype({dctc.SRV: str, dctc.AM: str})
        if not df.empty:
            msg = ('The following placements are under an adserver w/o '
                   'a recognized serving model. Add via Edit Processor Files'
                   'Translate or in platform:')
            logging.info('{}\n{}'.format(msg, df.to_string()))
        else:
            msg = ('All placements under an adserver have an associated '
                   'serving model.')
            logging.info('{}'.format(msg))
        self.add_to_analysis_dict(key_col=self.missing_serving,
                                  message=msg, data=df.to_dict())
        return True

    def find_missing_ad_rate(self):
        groups = [vmc.vendorkey, dctc.SRV, dctc.AM, dctc.AR]
        metrics = []
        df = self.generate_df_table(groups, metrics, sort=None,
                                    data_filter=None)
        df = df.reset_index()
        if df.empty:
            logging.warning(
                'Dataframe empty, could not determine missing ad rate.')
            return False
        df = df[((df[vmc.vendorkey].str.contains(vmc.api_dc_key)) |
                (df[vmc.vendorkey].str.contains(vmc.api_szk_key)))
                & (df[dctc.SRV] != 'No Tracking')]
        df = df[(df[dctc.AR] == 0) | (df[dctc.AR].isnull()) |
                (df[dctc.AR] == 'nan')]
        df = df.astype({dctc.SRV: str, dctc.AM: str, dctc.AR: str})
        df = df.drop(columns=vmc.vendorkey)
        if not df.empty:
            msg = ('The following Adserving Models are missing associated '
                   'rates. Add via Edit Processor Files -> Edit Relation '
                   'Dictionaries -> Relation - Serving:')
            logging.info('{}\n{}'.format(msg, df.to_string()))
        else:
            msg = ('All placements w/ Adserving Models have associated '
                   'adserving rates.')
            logging.info('{}'.format(msg))
        self.add_to_analysis_dict(key_col=self.missing_ad_rate,
                                  message=msg, data=df.to_dict())
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
        self.check_raw_file_update_time()
        self.generate_topline_and_weekly_metrics()
        self.evaluate_on_kpis()
        self.get_metrics_by_vendor_key()
        self.find_missing_metrics()
        self.flag_errant_metrics()
        self.find_missing_serving()
        self.find_missing_ad_rate()
        for analysis_class in self.class_list:
            analysis_class(self).do_analysis()
        self.write_analysis_dict()

    def do_analysis_and_fix_processor(self, pre_run=False):
        for analysis_class in self.class_list:
            if analysis_class.fix:
                if (pre_run and analysis_class.pre_run) or not pre_run:
                    analysis_class(self).do_and_fix_analysis()
        return self.fixes_to_run


class AnalyzeBase(object):
    name = ''
    fix = False
    pre_run = False

    def __init__(self, analyze_class=None):
        self.aly = analyze_class
        self.matrix = self.aly.matrix

    def do_analysis(self):
        self.not_implemented_warning('do_analysis')

    def fix_analysis(self, aly_dict, write=True):
        self.not_implemented_warning('fix_analysis')
        return None

    def not_implemented_warning(self, func_name):
        logging.warning('{} function not implemented for: {}'.format(
            func_name, self.name))

    def do_and_fix_analysis(self):
        self.do_analysis()
        aly_dict = self.aly.find_in_analysis_dict(self.name)
        if (len(aly_dict) > 0 and 'data' in aly_dict[0]
                and len(aly_dict[0]['data']) > 0):
            self.aly.fixes_to_run = True
            self.fix_analysis(pd.DataFrame(aly_dict[0]['data']))

    def add_to_analysis_dict(self, df, msg):
        self.aly.add_to_analysis_dict(
            key_col=self.name, message=msg, data=df.to_dict())


class CheckAutoDictOrder(AnalyzeBase):
    name = Analyze.change_auto_order
    fix = True

    @staticmethod
    def get_vendor_list():
        tc = dct.DictTranslationConfig()
        tc.read(dctc.filename_tran_config)
        tdf = tc.df[tc.df[dctc.DICT_COL_NAME] == dctc.VEN]
        ven_list = []
        for col in [dctc.DICT_COL_VALUE, dctc.DICT_COL_NVALUE]:
            new_ven_list = tdf[col].unique().tolist()
            ven_list = list(set(ven_list + new_ven_list))
            ven_list = [x for x in ven_list if x not in ['nan', '0']]
        return ven_list

    def do_analysis_on_data_source(self, source, df, ven_list=None):
        if not ven_list:
            ven_list = self.get_vendor_list()
        tdf = source.get_raw_df()
        if dctc.FPN not in tdf.columns or tdf.empty:
            return df
        tdf = pd.DataFrame(tdf[dctc.FPN].str.split('_').to_list())
        ven_col_counts = [tdf[col].isin(ven_list).sum()
                          for col in tdf.columns]
        max_val = max(ven_col_counts)
        max_idx = min(
            [idx for idx, val in enumerate(ven_col_counts) if val == max_val])
        auto_dict_idx = (source.p[vmc.autodicord].index(dctc.VEN)
                         if dctc.VEN in source.p[vmc.autodicord] else None)
        if (auto_dict_idx and max_idx != auto_dict_idx and
                ven_col_counts[max_idx] > 0):
            diff = auto_dict_idx - max_idx
            if diff > 0:
                new_order = source.p[vmc.autodicord][diff:]
            else:
                new_order = (diff * -1) * [dctc.MIS] + source.p[vmc.autodicord]
            data_dict = {vmc.vendorkey: [source.key],
                         self.name: [new_order]}
            df = df.append(pd.DataFrame(data_dict),
                           ignore_index=True, sort=False)
        return df

    def do_analysis(self):
        data_sources = self.matrix.get_all_data_sources()
        df = pd.DataFrame()
        ven_list = self.get_vendor_list()
        for ds in data_sources:
            df = self.do_analysis_on_data_source(ds, df, ven_list)
        if df.empty:
            msg = 'No new proposed order.'
        else:
            msg = 'Proposed new order by key as follows:'
        logging.info('{}\n{}'.format(msg, df.to_string()))
        self.aly.add_to_analysis_dict(key_col=self.name,
                                      message=msg, data=df.to_dict())

    def fix_analysis_for_data_source(self, source_aly_dict, write=True):
        vk = source_aly_dict[vmc.vendorkey]
        new_order = '|'.join(source_aly_dict[self.name])
        logging.info('Changing order for {} to {}'.format(vk, new_order))
        data_source = self.aly.matrix.get_data_source(vk)
        try:
            os.remove(os.path.join(utl.dict_path,
                                   data_source.p[vmc.filenamedict]))
        except FileNotFoundError as e:
            logging.warning('File not found error: {}'.format(e))
        self.aly.matrix.vm_change_on_key(vk, vmc.autodicord, new_order)
        if write:
            self.aly.matrix.write()

    def fix_analysis(self, aly_dict, write=True):
        aly_dict = aly_dict.to_dict(orient='records')
        for x in aly_dict:
            self.fix_analysis_for_data_source(x, write=write)
        if write:
            self.aly.matrix.write()
        return self.aly.matrix.vm_df

    
class CheckPackageCapping(AnalyzeBase):
    name = Analyze.package_cap
    cap_name = Analyze.cap_name
    package_vendor_good = 'Package_Vendor_Good'
    package_vendor_bad = 'Package_Vendor_Bad'
    plan_net_temp = 'Planned Net Cost - TEMP'
    net_cost_capped = 'Net Cost (Capped)'
    pre_run = True
    fix = True

    def initialize_cap_file(self):
        """
        gets Cap Config file and the file to be capped
        df -> raw data appended with cap file (will later be grouped cleaner)
        temp_package_cap -> the column name to be capped on, stated in cap file
        c -> config file
        pdf -> cap file data
        cap_file -> MetricCap() object
        """
        df = self.aly.df
        df = cal.net_cost_calculation(df)
        c = None
        if cal.MetricCap().config:
            cap_file = cal.MetricCap()
            for cfg in cap_file.config:
                c = cap_file.config[cfg]
            if c and os.path.isfile(c[cal.MetricCap().file_name]):
                pdf = cap_file.get_cap_file(c)
                df = df.append(pdf)
                temp_package_cap = c[cap_file.proc_dim]
                return df, temp_package_cap, c, pdf, cap_file

    def check_package_cap(self, df, temp_package_cap):
        """
        Checks if a package used for capping has reached or exceeded its cap
        Prints to logfile

        Make sure cap file exists, set as pdf and append to our dataframe
        temp_package_cap -> column name we are capping on from raw file
        'plan_net_temp -> how much we cap,taken from raw file
        """
        df = df[[temp_package_cap, self.plan_net_temp, vmc.cost]]
        df = df.groupby([temp_package_cap])
        df = df.apply(lambda x:
                      0 if x[self.plan_net_temp].sum() == 0
                      else x[vmc.cost].sum() / x[self.plan_net_temp].sum())
        f_df = df[df >= 1]
        if f_df.empty:
            delivery_msg = 'No Packages have exceeded their cap'
            logging.info(delivery_msg)
            self.aly.add_to_analysis_dict(
                key_col=self.cap_name,
                param=self.aly.under_delivery_col,
                message=delivery_msg)
            return f_df
        else:
            del_p = f_df.apply(lambda x: "{0:.2f}%".format(x * 100))
            delivery_msg = 'The following packages have delivered in full: '
            logging.info('{}\n{}'.format(delivery_msg, del_p))
            data = del_p.reset_index().rename(columns={0: 'Cap'})
            self.aly.add_to_analysis_dict(
                key_col=self.cap_name,
                param=self.aly.full_delivery_col,
                message=delivery_msg,
                data=data.to_dict())
            o_df = f_df[f_df > 1.5]
            if not o_df.empty:
                del_p = o_df.apply(lambda x:
                                   "{0:.2f}%".format(x * 100))
                delivery_msg = 'The following packages have over-delivered:'
                logging.info('{}\n{}'.format(delivery_msg, del_p))
                data = del_p.reset_index().rename(columns={0: 'Cap'})
                self.aly.add_to_analysis_dict(
                    key_col=self.cap_name,
                    param=self.aly.over_delivery_col,
                    message=delivery_msg,
                    data=data.to_dict())
            return data

    def check_package_vendor(self, df, temp_package_cap, pdf):
        """
        Warns if the package cap file will affect multiple vendors
        creates dataframe grouped by cap and vendor
        counts unique members,
        if there are more vendors than there are caps, raise a warning
        return df of packages with multiple vendors associated
        """
        df = df[[dctc.VEN, vmc.vendorkey, dctc.PN, temp_package_cap,
                 self.plan_net_temp, vmc.cost]]
        df = df.groupby([temp_package_cap, dctc.VEN])
        df = df.size().reset_index(name='count')
        df = df[[temp_package_cap, dctc.VEN]]
        df = df[df[temp_package_cap].isin(pdf[temp_package_cap])]
        df = df[df.duplicated(subset=temp_package_cap, keep=False)]
        if not df.empty:
            delivery_msg = ('One or more of the packages you are capping on is '
                            'associated with multiple vendors')
            logging.warning('{}\n{}'.format(delivery_msg, df))
            self.aly.add_to_analysis_dict(key_col=self.name,
                                          param=self.aly.package_vendor_bad,
                                          message=delivery_msg,
                                          data=df.to_dict())
            return df
        else:
            delivery_msg = "All packages are capping on a single vendor"
            logging.info('{}\n{}'.format(delivery_msg, df))
            self.aly.add_to_analysis_dict(key_col=self.name,
                                          param=self.aly.package_vendor_good,
                                          message=delivery_msg,
                                          data=df.to_dict())
            return df

    def fix_package_vendor(self, temp_package_cap, c, pdf, cap_file,
                           write=None, aly_dict=None):
        """
        Takes in capped packages that are associated with more than one vendor
        Changes their names to be unique
        Translates all instances in dictionaries to match
        """
        df = aly_dict
        if not df.empty:
            t_df = pd.DataFrame({dctc.DICT_COL_NAME: [],
                                 dctc.DICT_COL_VALUE: [],
                                 dctc.DICT_COL_NVALUE: [],
                                 dctc.DICT_COL_FNC: [],
                                 dctc.DICT_COL_SEL: [], 'index': []})
            t_df[dctc.DICT_COL_SEL] = df[dctc.VEN]
            t_df[dctc.DICT_COL_NAME] = temp_package_cap
            t_df[dctc.DICT_COL_VALUE] = df[temp_package_cap]
            for temp_package_cap in df[[temp_package_cap]]:
                df[temp_package_cap] = df[temp_package_cap] + '-' + df[dctc.VEN]
                df[self.net_cost_capped] = pdf[self.plan_net_temp]
            df = df[[temp_package_cap, self.net_cost_capped]]
            df.replace(to_replace=np.NaN,
                       value=df.loc[0],
                       inplace=True)
            path = c[cap_file.file_name]
            df.to_csv(path, index=False, encoding='utf-8')
            t_df[dctc.DICT_COL_NVALUE] = df[temp_package_cap]
            t_df[dctc.DICT_COL_FNC] = 'Select::mpVendor'
            t_df = t_df[[dctc.DICT_COL_NAME, dctc.DICT_COL_VALUE,
                         dctc.DICT_COL_NVALUE, dctc.DICT_COL_FNC,
                         dctc.DICT_COL_SEL]]
            if write:
                tc = dct.DictTranslationConfig().csv_path
                translation = dct.DictTranslationConfig()
                trans_dict = pd.read_csv(tc + dctc.filename_tran_config)
                trans_dict = trans_dict.append(t_df)
                translation.write(trans_dict, dctc.filename_tran_config)
                fix_msg = 'Automatically changing capped package names:'
                logging.info('{}\n{}'.format(fix_msg, t_df))
            return t_df

    def do_analysis(self):
        try:
            df, temp_package_cap, c, pdf, cap_file = self.initialize_cap_file()
        except TypeError:
            logging.debug("cap config file is missing")
            return None
        except AttributeError:
            logging.debug("one of the files may be empty")
            return None
        self.check_package_cap(df, temp_package_cap)
        self.check_package_vendor(df, temp_package_cap, pdf)
    
    def fix_analysis(self, aly_dict, write=True):
        try:
            df, temp_package_cap, c, pdf, cap_file = self.initialize_cap_file()
        except TypeError:
            logging.debug("cap config file is missing")
            return None
        except AttributeError:
            logging.debug("one of the files may be empty")
            return None
        self.fix_package_vendor(temp_package_cap, c, pdf, cap_file,
                                write=write, aly_dict=aly_dict)
        

class FindPlacementNameCol(AnalyzeBase):
    name = Analyze.placement_col
    fix = True

    @staticmethod
    def do_analysis_on_data_source(source, df):
        if vmc.filename not in source.p:
            return pd.DataFrame()
        file_name = source.p[vmc.filename]
        first_row = source.p[vmc.firstrow]
        transforms = str(source.p[vmc.transform]).split(':::')
        transforms = [x for x in transforms if x.split('::')[0]
                      in ['FilterCol', 'MergeReplaceExclude']]
        p_col = source.p[vmc.placement]
        if os.path.exists(file_name):
            tdf = source.get_raw_df(nrows=first_row + 3)
            if tdf.empty and transforms:
                tdf = source.get_raw_df()
            tdf = tdf.drop([vmc.fullplacename], axis=1, errors='ignore')
            if tdf.empty:
                return df
            tdf = tdf.applymap(
                lambda x: str(x).count('_')).apply(lambda x: sum(x))
            max_col = tdf.idxmax(axis=1)
            max_exists = max_col in tdf
            p_exists = p_col in tdf
            no_p_check = (not p_exists and max_exists)
            p_check = (max_exists and p_exists and
                       tdf[max_col] >= (tdf[p_col] + 9)
                       and 75 <= tdf[max_col] <= 105)
            if no_p_check or p_check:
                data_dict = {vmc.vendorkey: [source.key],
                             'Current Placement Col': p_col,
                             'Suggested Col': max_col}
                df = df.append(pd.DataFrame(data_dict),
                               ignore_index=True, sort=False)
        return df

    def do_analysis(self):
        data_sources = self.matrix.get_all_data_sources()
        df = pd.DataFrame()
        for source in data_sources:
            df = self.do_analysis_on_data_source(source, df)
        if df.empty:
            msg = ('Placement Name columns look correct. '
                   'No columns w/ more breakouts.')
            logging.info('{}'.format(msg))
        else:
            msg = ('The following data sources have more breakouts in '
                   'another column. Consider changing placement name '
                   'source:')
            logging.info('{}\n{}'.format(msg, df.to_string()))
        self.aly.add_to_analysis_dict(key_col=self.name,
                                      message=msg, data=df.to_dict())

    def fix_analysis_for_data_source(self, source_aly_dict, write=True,
                                     col=vmc.placement):
        vk = source_aly_dict[vmc.vendorkey]
        new_col = source_aly_dict['Suggested Col']
        logging.info('Changing {} {} to {}'.format(vk, col, new_col))
        self.aly.matrix.vm_change_on_key(vk, col, new_col)
        if write:
            self.aly.matrix.write()

    def fix_analysis(self, aly_dict, write=True):
        aly_dict = aly_dict.to_dict(orient='records')
        for x in aly_dict:
            self.fix_analysis_for_data_source(x, False)
        if write:
            self.aly.matrix.write()
        return self.aly.matrix.vm_df


class CheckApiDateLength(AnalyzeBase):
    """Checks APIs for max date length and splits data sources if necessary."""
    name = Analyze.max_api_length
    fix = True
    pre_run = True

    def do_analysis(self):
        """
        Loops through all data sources checking and flagging through those that
        are too long.  Those sources are added to a df and the analysis dict
        """
        vk_list = []
        data_sources = self.matrix.get_all_data_sources()
        max_date_dict = {
            vmc.api_amz_key: 60, vmc.api_szk_key: 60, vmc.api_db_key: 60,
            vmc.api_tik_key: 30, vmc.api_ttd_key: 80, vmc.api_sc_key: 30,
            vmc.api_amd_key: 30}
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
        mdf[self.name] = ''
        if vk_list:
            msg = 'The following APIs are within 3 days of their max length:'
            logging.info('{}\n{}'.format(msg, vk_list))
            mdf[self.name] = mdf[vmc.vendorkey].str.split(
                '_').str[1].replace(max_date_dict)
        else:
            msg = 'No APIs within 3 days of max length.'
            logging.info('{}'.format(msg))
        self.add_to_analysis_dict(df=mdf, msg=msg)

    def fix_analysis(self, aly_dict, write=True):
        """
        Takes data sources that are too long and splits them based on date.

        :param aly_dict: a df containing items to fix
        :param write: boolean will write the vm as csv when true
        :returns: the vm as a df
        """
        tdf = aly_dict.to_dict(orient='records')
        df = self.aly.matrix.vm_df
        for x in tdf:
            vk = x[vmc.vendorkey]
            logging.info('Duplicating vendor key {}'.format(vk))
            max_date_length = x[self.name]
            ndf = df[df[vmc.vendorkey] == vk].reset_index(drop=True)
            ndf = utl.data_to_type(ndf, date_col=[vmc.startdate])
            new_sd = ndf[vmc.startdate][0] + dt.timedelta(
                days=max_date_length - 3)
            if new_sd.date() >= dt.datetime.today().date():
                new_sd = dt.datetime.today() - dt.timedelta(days=3)
            new_str_sd = new_sd.strftime('%Y-%m-%d')
            ndf.loc[0, vmc.startdate] = new_str_sd
            ndf.loc[0, vmc.enddate] = ''
            new_vk = '{}_{}'.format(vk, new_str_sd)
            ndf.loc[0, vmc.vendorkey] = new_vk
            file_type = os.path.splitext(ndf[vmc.filename][0])[1].lower()
            new_fn = '{}{}'.format(new_vk.replace('API_', '').lower(),
                                   file_type)
            ndf.loc[0, vmc.filename] = new_fn
            idx = df[df[vmc.vendorkey] == vk].index
            df.loc[idx, vmc.vendorkey] = df.loc[
                idx, vmc.vendorkey][idx[0]].replace('API_', '')
            old_ed = new_sd - dt.timedelta(days=1)
            df.loc[idx, vmc.enddate] = old_ed.strftime('%Y-%m-%d')
            df = df.append(ndf).reset_index(drop=True)
        self.aly.matrix.vm_df = df
        if write:
            self.aly.matrix.write()
        return self.aly.matrix.vm_df


class CheckColumnNames(AnalyzeBase):
    """Checks raw data for column names and reassigns if necessary."""
    name = Analyze.raw_columns
    fix = True
    pre_run = True

    def do_analysis(self):
        """
        Loops through all data sources adds column names and flags if
        missing active metrics.
        """
        data_sources = self.matrix.get_all_data_sources()
        df = pd.DataFrame()
        for source in data_sources:
            if vmc.firstrow not in source.p:
                continue
            first_row = source.p[vmc.firstrow]
            transforms = str(source.p[vmc.transform]).split(':::')
            transforms = [x for x in transforms if x.split('::')[0]
                          in ['FilterCol', 'MergeReplaceExclude']]
            missing_cols = []
            tdf = source.get_raw_df(nrows=first_row+5)
            if tdf.empty and transforms:
                tdf = source.get_raw_df()
            cols = list(tdf.columns)
            active_metrics = source.get_active_metrics()
            active_metrics[vmc.placement] = [source.p[vmc.placement]]
            for k, v in active_metrics.items():
                for c in v:
                    if c not in cols:
                        missing_cols.append({k: c})
            data_dict = {vmc.vendorkey: [source.key], self.name: [cols],
                         'missing': [missing_cols]}
            df = df.append(pd.DataFrame(data_dict),
                           ignore_index=True, sort=False)
        update_msg = 'Columns and missing columns by key as follows:'
        logging.info('{}\n{}'.format(update_msg, df.to_string()))
        self.add_to_analysis_dict(df=df, msg=update_msg)

    def fix_analysis(self, aly_dict, write=True):
        """
        Adjusts placement name and auto dict order of data sources when those
        values are missing.

        :param aly_dict: a df containing items to fix
        :param write: boolean will write the vm as csv when true
        :returns: the vm as a df
        """
        aly_dicts = aly_dict.to_dict(orient='records')
        df = self.aly.matrix.vm_df
        aly_dicts = [x for x in aly_dicts
                     if x['missing'] and x['raw_file_columns']]
        for aly_dict in aly_dicts:
            vk = aly_dict[vmc.vendorkey]
            source = self.matrix.get_data_source(vk)
            placement_missing = [x for x in aly_dict['missing'] if
                                 vmc.placement in x.keys()]
            if placement_missing:
                logging.info('Placement name missing for {}.  '
                             'Attempting to find.'.format(vk))
                fnc = FindPlacementNameCol(self.aly)
                tdf = fnc.do_analysis_on_data_source(source, pd.DataFrame())
                if not tdf.empty:
                    tdf = tdf.to_dict(orient='records')[0]
                    for col in [vmc.placement, vmc.fullplacename]:
                        fnc.fix_analysis_for_data_source(tdf, True, col)
                    self.matrix = vm.VendorMatrix(display_log=False)
                    source = self.matrix.get_data_source(vk)
                    cad = CheckAutoDictOrder(self.aly)
                    tdf = cad.do_analysis_on_data_source(source, pd.DataFrame())
                    if not tdf.empty:
                        tdf = tdf.to_dict(orient='records')[0]
                        cad.fix_analysis_for_data_source(tdf, True)
                        self.matrix = vm.VendorMatrix(display_log=False)
            date_missing = [x for x in aly_dict['missing'] if
                            vmc.date in x.keys()]
            if date_missing:
                logging.info('Date col missing for {}.  '
                             'Attempting to find.'.format(vk))
                tdf = source.get_raw_df()
                for col in tdf.columns:
                    try:
                        tdf[col] = utl.data_to_type(tdf[col].reset_index(),
                                                    date_col=[col])[col]
                    except:
                        tdf[col] = pd.NaT
                date_col = (tdf.isnull().sum() * 100 / len(tdf)).idxmin()
                logging.info('Changing {} date col to {} '.format(vk, date_col))
                self.aly.matrix.vm_change_on_key(vk, vmc.date, date_col)
                self.aly.matrix.write()
                self.matrix = vm.VendorMatrix(display_log=False)
        self.aly.matrix.vm_df = df
        if write:
            self.aly.matrix.write()
        return self.aly.matrix.vm_df


class CheckFlatSpends(AnalyzeBase):
    """Checks for past flat packages reassigns placement date if necessary."""
    name = Analyze.missing_flat
    first_click_col = 'First Click Date'
    error_col = 'Error'
    missing_clicks_error = 'No Clicks'
    placement_date_error = 'Incorrect Placement Date'
    missing_rate_error = 'Missing Buy Rate'
    fix = True
    pre_run = True

    def merge_first_click_date(self, df, tdf, groups):
        df = df.merge(tdf.drop_duplicates(),
                      on=groups,
                      how='left', indicator=True)
        df = df.drop(columns=['Clicks_y'])
        df = df.rename(columns={vmc.date: self.first_click_col,
                                'Clicks_x': vmc.clicks})
        df = df.astype({vmc.clicks: str})
        df[dctc.PD] = df[dctc.PD].dt.strftime('%Y-%m-%d %H:%M:%S')
        df[self.first_click_col] = df[
            self.first_click_col].dt.strftime('%Y-%m-%d %H:%M:%S')
        return df

    def find_missing_flat_spend(self, df):
        """
        Checks for flat packages w/ no attributed cost past placement date.
        Sorts into missing clicks, no buy model, or wrong placement date.
        """
        pn_groups = [dctc.VEN, dctc.COU, dctc.PN, dctc.PKD, dctc.PD, dctc.BM,
                     dctc.BR, vmc.date]
        metrics = [cal.NCF, vmc.clicks]
        metrics = [metric for metric in metrics if metric in df.columns]
        df = self.aly.generate_df_table(pn_groups, metrics, sort=None,
                                        data_filter=None, df=df)
        df.reset_index(inplace=True)
        if dctc.BM in df.columns:
            df = df[(df[dctc.BM] == cal.BM_FLAT) |
                    (df[dctc.BM] == cal.BM_FLAT2)]
        if not df.empty:
            pk_groups = [dctc.VEN, dctc.COU, dctc.PKD]
            tdf = df.groupby(pk_groups).sum()
            tdf.reset_index(inplace=True)
            tdf = tdf[tdf[cal.NCF] == 0]
            df = df.merge(tdf[pk_groups], how='right')
            if not df.empty:
                pn_groups.remove(vmc.date)
                tdf = df[df[vmc.clicks] > 0]
                tdf = tdf.groupby(pn_groups).min()
                tdf.reset_index(inplace=True)
                if cal.NCF not in tdf:
                    return pd.DataFrame()
                tdf = tdf.drop(columns=[cal.NCF])
                tdf = utl.data_to_type(tdf, date_col=[dctc.PD, vmc.date])
                df = df.groupby(pn_groups).sum()
                df.reset_index(inplace=True)
                df = utl.data_to_type(df, date_col=[dctc.PD])
                df = self.merge_first_click_date(df, tdf, pn_groups)
                df = utl.data_to_type(df, date_col=[dctc.PD])
                rdf = df[df[dctc.BR] == 0]
                if not rdf.empty:
                    rdf = rdf.drop(columns='_merge')
                rdf[self.error_col] = self.missing_rate_error
                df = df[df[dctc.PD] <= dt.datetime.today()]
                if not df.empty:
                    cdf = df[df['_merge'] == 'both']
                    cdf = cdf.iloc[:, :-1]
                    cdf = cdf[cdf[self.first_click_col] != cdf[dctc.PD]]
                    cdf[self.error_col] = self.placement_date_error
                    ndf = df[df['_merge'] == 'left_only']
                    ndf = ndf.drop(columns=['_merge'])
                    ndf[self.error_col] = self.missing_clicks_error
                    df = cdf.append(rdf, sort=False)
                    df = df.append(ndf, sort=False)
                    df = df.reset_index(drop=True)
                    df = df.dropna(how='all')
                    df = df.fillna('')
        df = utl.data_to_type(df, str_col=[dctc.PD, self.first_click_col])
        return df

    def do_analysis(self):
        df = self.aly.df
        rdf = self.find_missing_flat_spend(df)
        if rdf.empty:
            msg = ('All flat packages with clicks past their placement date '
                   'have associated net cost.')
            logging.info('{}'.format(msg))
        else:
            msg = ('The following flat packages are not calculating net cost '
                   'for the following reasons:')
            logging.info('{}\n{}'.format(msg, rdf.to_string()))
        self.add_to_analysis_dict(df=rdf, msg=msg)

    def fix_analysis(self, aly_dict, write=True):
        """
        Translates flat packages w/ missing spends placement date to first w/
        clicks.

        :param aly_dict: a df containing items to fix
        :param write: boolean will write the translational_dict as csv when true
        :returns: the lines added to translational_dict
        """
        if (aly_dict.empty or self.placement_date_error
                not in aly_dict[self.error_col].values):
            return pd.DataFrame()
        translation = dct.DictTranslationConfig()
        translation.read(dctc.filename_tran_config)
        translation_df = translation.get()
        aly_dicts = aly_dict.to_dict(orient='records')
        tdf = pd.DataFrame(columns=translation_df.columns)
        for aly_dict in aly_dicts:
            if aly_dict[self.error_col] == self.placement_date_error:
                old_val = aly_dict[dctc.PD].strip('00:00:00').strip()
                new_val = aly_dict[
                    self.first_click_col].strip('00:00:00').strip()
                try:
                    trans = [[dctc.PD, old_val, new_val,
                              'Select::' + dctc.PN,
                              aly_dict[dctc.PN], 0]]
                    row = pd.DataFrame(trans, columns=translation_df.columns)
                    tdf = tdf.append(row, ignore_index=True, sort=False)
                except AssertionError:
                    trans = [[dctc.PD, old_val, new_val,
                              'Select::' + dctc.PN,
                              aly_dict[dctc.PN]]]
                    row = pd.DataFrame(trans, columns=translation_df.columns)
                    tdf = tdf.append(row, ignore_index=True, sort=False)
        translation_df = translation_df.append(
            tdf, ignore_index=True, sort=False)
        if write:
            translation.write(translation_df, dctc.filename_tran_config)
        return tdf


class CheckDoubleCounting(AnalyzeBase):
    """
    Checks for double counting datasources.
    If double counting all placements, removes metric from one of the
    datasources.
    """
    name = Analyze.double_counting_all
    error_col = 'Error'
    double_counting_all = 'All'
    double_counting_partial = 'Partial'
    tmp_col = 'temp'
    metric_col = 'Metric'
    total_placement_count = 'Total Num Placements'
    num_duplicates = 'Num Duplicates'
    fix = True
    pre_run = True

    def count_unique_placements(self, df, col):
        df = df.groupby([dctc.VEN, vmc.vendorkey, dctc.PN]).size()
        df = df.reset_index().rename(columns={0: self.tmp_col})
        df = df.groupby([dctc.VEN, vmc.vendorkey]).size()
        df = df.reset_index().rename(columns={0: col})
        return df

    def find_metric_double_counting(self, df):
        rdf = pd.DataFrame()
        groups = [dctc.VEN, vmc.vendorkey, dctc.PN, vmc.date]
        metrics = [cal.NCF, vmc.impressions, vmc.clicks, vmc.video_plays,
                   vmc.views, vmc.views25, vmc.views50, vmc.views75,
                   vmc.views100]
        metrics = [metric for metric in metrics if metric in df.columns]
        df = self.aly.generate_df_table(groups, metrics, sort=None,
                                        data_filter=None, df=df)
        df.reset_index(inplace=True)
        sdf = self.count_unique_placements(df, self.total_placement_count)
        sdf = sdf.groupby(dctc.VEN).max().reset_index()
        df = df[df.duplicated(subset=[dctc.VEN, dctc.PN, vmc.date], keep=False)]
        if not df.empty:
            for metric in metrics:
                tdf = df[df[metric] > 0]
                tdf = tdf[tdf.duplicated(
                    subset=[dctc.PN, vmc.date], keep=False)]
                if not tdf.empty:
                    tdf = self.count_unique_placements(tdf, self.num_duplicates)
                    tdf[self.metric_col] = metric
                    rdf = pd.concat([rdf, tdf], ignore_index=True)
        if not rdf.empty:
            rdf = sdf[[dctc.VEN, self.total_placement_count]].merge(
                rdf, how='inner', on=dctc.VEN)
            rdf = rdf.groupby([dctc.VEN, self.metric_col,
                               self.total_placement_count,
                               self.num_duplicates])[vmc.vendorkey].apply(
                lambda x: ','.join(x)).reset_index()
            rdf = rdf.groupby([dctc.VEN, self.metric_col, vmc.vendorkey,
                               self.num_duplicates]).max().reset_index()
            rdf[self.error_col] = np.where(
                rdf[self.total_placement_count] == rdf[self.num_duplicates],
                self.double_counting_all, self.double_counting_partial)
        return rdf

    def do_analysis(self):
        df = self.aly.df
        rdf = self.find_metric_double_counting(df)
        if rdf.empty:
            msg = ('No datasources are double counting placements for any '
                   'metric.')
            logging.info('{}'.format(msg))
        else:
            msg = ('The following datasources are double counting the following'
                   ' metrics on all or some placements:')
            logging.info('{}\n{}'.format(msg, rdf.to_string()))
        self.add_to_analysis_dict(df=rdf, msg=msg)

    @staticmethod
    def remove_metric(vm_df, vk, metric):
        if metric == cal.NCF:
            metric = vmc.cost
        idx = vm_df[vm_df[vmc.vendorkey] == vk].index
        vm_df.loc[idx, metric] = ''
        logging.info('Removing {} from {}.'.format(metric, vk))
        return vm_df

    @staticmethod
    def update_rule(vm_df, vk, metric, vendor, idx, query_str, metric_str):
        if metric == cal.NCF:
            metric = vmc.cost
        if vendor not in str(vm_df.loc[idx, query_str].values):
            vm_df.loc[idx, query_str] = (
                    vm_df.loc[idx, query_str][idx[0]] + ',' + vendor)
        if not (metric in str(vm_df.loc[idx, metric_str].values)):
            vm_df.loc[idx, metric_str] = (
                    vm_df.loc[idx, metric_str][idx[0]] +
                    '|' + metric)
        logging.info('Adding rule for {} to remove {} {}.'.format(
            vk, vendor, metric))
        return vm_df

    @staticmethod
    def add_rule(vm_df, vk, rule_num, idx, metric, vendor):
        if metric == cal.NCF:
            metric = vmc.cost
        metric_str = "_".join([utl.RULE_PREF, str(rule_num), utl.RULE_METRIC])
        query_str = "_".join([utl.RULE_PREF, str(rule_num), utl.RULE_QUERY])
        factor_str = "_".join([utl.RULE_PREF, str(rule_num), utl.RULE_FACTOR])
        vm_df.loc[idx, factor_str] = 0.0
        vm_df.loc[idx, metric_str] = ('POST' + '::' + metric)
        vm_df.loc[idx, query_str] = (dctc.VEN + '::' + vendor)
        logging.info('Adding rule for {} to remove ''{} {}.'.format(vk, vendor,
                                                                    metric))
        return vm_df

    def fix_all(self, aly_dict):
        aly_dict = aly_dict.sort_values(by=[dctc.VEN, self.metric_col])
        metric_buckets = {
            'ctr_metrics': [vmc.impressions, vmc.clicks],
            'vtr_metrics': [
                vmc.views25, vmc.views50, vmc.views75, vmc.views100],
            'video_play_metrics': [vmc.video_plays, vmc.views],
            'net_cost_metrics': [cal.NCF]
        }
        vm_df = self.aly.matrix.vm_df
        logging.info('Attempting to remove double counting.')
        for index, row in aly_dict.iterrows():
            vks = row[vmc.vendorkey].split(',')
            raw_vks = [x for x in vks if vmc.api_raw_key in x
                       or vmc.api_gs_key in x]
            serve_vks = [x for x in vks if vmc.api_szk_key in x
                         or vmc.api_dc_key in x]
            first_empty = None
            added = False
            bucket = [k for k, v in metric_buckets.items()
                      if row[self.metric_col] in v]
            if not bucket:
                bucket = row[self.metric_col]
            else:
                bucket = bucket[0]
            for vk in raw_vks:
                if len(vks) > 1:
                    vm_df = self.remove_metric(vm_df, vk, row[self.metric_col])
                    vks.remove(vk)
            for vk in serve_vks:
                if len(vks) > 1:
                    idx = vm_df[vm_df[vmc.vendorkey] == vk].index
                    for i in range(1, 7):
                        metric_str = "_".join(
                            [utl.RULE_PREF, str(i), utl.RULE_METRIC])
                        query_str = "_".join(
                            [utl.RULE_PREF, str(i), utl.RULE_QUERY])
                        if ([x for x in metric_buckets[bucket]
                             if x in str(vm_df.loc[idx, metric_str].values)]):
                            vm_df = self.update_rule(
                                vm_df, vk, row[self.metric_col],
                                row[dctc.VEN], idx, query_str, metric_str)
                            added = True
                            break
                        if not vm_df.loc[idx, query_str].any():
                            if not first_empty:
                                first_empty = i
                            continue
                    if not added:
                        if first_empty:
                            self.add_rule(vm_df, vk, first_empty, idx,
                                          row[self.metric_col], row[dctc.VEN])
                        else:
                            logging.warning('No empty rules for {}. Could not '
                                            'auto-fix double counting.'
                                            .format(vk))
                vks.remove(vk)
        self.aly.matrix.vm_df = vm_df
        return vm_df

    def fix_analysis(self, aly_dict, write=True):
        """
        Removes duplicate metrics if all placements duplicated.
        Prioritizes removal from rawfiles first, adservers otherwise.

        :param aly_dict: a df containing items to fix
        :param write: boolean will write the vendormatrix as csv when true
        :returns: the vendormatrix as a df
        """
        if aly_dict.empty:
            return pd.DataFrame()
        self.fix_all(
            aly_dict[aly_dict[self.error_col] == self.double_counting_all])
        if write:
            self.aly.matrix.write()
        return self.aly.matrix.vm_df


class GetPacingAnalysis(AnalyzeBase):
    name = Analyze.delivery_comp_col
    fix = False
    pre_run = False
    delivery_col = 'Delivery'
    proj_completion_col = 'Projected Full Delivery'
    pacing_goal_col = '% Through Campaign'

    @staticmethod
    def get_rolling_mean_df(df, value_col, group_cols):
        """
        Gets rolling means to project delivery from

        :param df: a df containing dates and desired values/groups
        :param value_col: values to calculate rolling means of
        :param group_cols: column breakouts to base rolling means on
        :returns: df w/ groups cols, value_cols, and 3,7,30 day rolling means
        """
        if df.empty:
            logging.warning('Dataframe empty, could not get rolling mean.')
            return df
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

    def project_delivery_completion(self, df, average_df, plan_names,
                                    final_cols):
        """
        Use rolling means to project delivery completion date.

        :param df: df where planned costs greater than net
        :param average_df: return df from get_rolling_mean_df
        :param plan_names: planned net full placement columns
        :param final_cols: desired columns in final df
        :returns: original df w/ added projected completion column
        """
        df = df.merge(average_df, how='left', on=plan_names)
        df['days'] = (df[dctc.PNC] - df[vmc.cost]) / df[
            '{} rolling {}'.format(vmc.cost, 3)]
        df['days'] = df['days'].replace(
            [np.inf, -np.inf], np.nan).fillna(10000)
        df['days'] = np.where(df['days'] > 10000, 10000, df['days'])
        df[self.proj_completion_col] = pd.to_datetime(
            df[vmc.date]) + pd.to_timedelta(
            np.ceil(df['days']).astype(int), unit='D')
        no_date_map = ((df[self.proj_completion_col] >
                        dt.datetime.today() + dt.timedelta(days=365)) |
                       (df[self.proj_completion_col] <
                        dt.datetime.today() - dt.timedelta(days=365)))
        df[self.proj_completion_col] = df[
            self.proj_completion_col].dt.strftime('%Y-%m-%d')
        df.loc[
            no_date_map, self.proj_completion_col] = 'Greater than 1 Year'
        df[self.proj_completion_col] = df[
            self.proj_completion_col].replace(
            [np.inf, -np.inf, np.datetime64('NaT'), 'NaT'], np.nan
        ).fillna('Greater than 1 Year')
        df = df[final_cols]
        return df

    def get_actual_delivery(self, df):
        """
        Calculate delivery metrics

        :param df: df w/ topline planned and actual spend metrics
        :returns: original df w/ delivery and pacing metrics
        """
        df[self.delivery_col] = (df[vmc.cost] / df[dctc.PNC] * 100).round(2)
        df[self.delivery_col] = df[self.delivery_col].replace(
            [np.inf, -np.inf], np.nan).fillna(0)
        df[self.delivery_col] = df[self.delivery_col].astype(str) + '%'
        df[self.pacing_goal_col] = ((pd.Timestamp.today(None) - df[dctc.SD]
                                     ) / (df[dctc.ED] - df[dctc.SD])
                                    * 100).round(2)
        df[self.pacing_goal_col] = np.where(
            df[self.pacing_goal_col] > 100, 100, df[self.pacing_goal_col])
        df[self.pacing_goal_col] = df[self.pacing_goal_col].replace(
            [np.inf, -np.inf], np.nan).fillna(0)
        df[self.pacing_goal_col] = df[self.pacing_goal_col].astype(str) + '%'
        return df

    def get_pacing_analysis(self, df):
        """
        Calculate topline level pacing data for use in pacing table and alerts.

        :param df: full output df
        """
        if df.empty:
            logging.warning('Dataframe empty could not get pacing analysis.')
            return pd.DataFrame()
        plan_names = self.matrix.vendor_set(vm.plan_key)[vmc.fullplacename]
        average_df = self.get_rolling_mean_df(
            df=df, value_col=vmc.cost, group_cols=plan_names)
        if average_df.empty:
            msg = ('Average df empty, maybe too large.  '
                   'Could not project delivery completion.')
            logging.warning(msg)
            self.aly.add_to_analysis_dict(key_col=self.aly.delivery_comp_col,
                                          message=msg)
            return pd.DataFrame()
        last_date = dt.datetime.strftime(
            dt.datetime.today() - dt.timedelta(days=1), '%Y-%m-%d')
        average_df = average_df[average_df[vmc.date] == last_date]
        average_df = average_df.drop(columns=[vmc.cost])
        start_dates, end_dates = self.aly.get_start_end_dates(
            df, plan_names)
        df = df.groupby(plan_names)[vmc.cost, dctc.PNC, vmc.AD_COST].sum()
        df = df.reset_index()
        df = df[(df[vmc.cost] > 0) | (df[dctc.PNC] > 0)]
        tdf = df[df[dctc.PNC] > df[vmc.cost]]
        df = df[df[dctc.PNC] <= df[vmc.cost]]
        final_cols = (plan_names + [dctc.PNC] + [vmc.cost] + [vmc.AD_COST] +
                      [self.proj_completion_col])
        if not tdf.empty:
            tdf = self.project_delivery_completion(
                tdf, average_df, plan_names, final_cols)
        if not df.empty:
            df[self.proj_completion_col] = [
                'No Planned' if x == 0 else 'Delivered' for x in df[dctc.PNC]]
            df = df[final_cols]
            if not tdf.empty:
                tdf = tdf.merge(
                    df, how='outer', on=final_cols)
            else:
                tdf = df
        over_delv = self.aly.find_in_analysis_dict(
            self.delivery_col, param=self.aly.over_delivery_col)
        if over_delv:
            df = pd.DataFrame(over_delv[0]['data'])
            tdf = tdf.merge(
                df[plan_names], on=plan_names, how='left', indicator=True)
            tdf[self.proj_completion_col] = [
                'Over Delivered' if tdf['_merge'][x] == 'both'
                else tdf[self.proj_completion_col][x] for x in tdf.index]
            tdf = tdf.drop(columns=['_merge'])
        tdf = tdf.merge(start_dates, how='left', on=plan_names)
        tdf = tdf.merge(end_dates, how='left', on=plan_names)
        tdf = self.get_actual_delivery(tdf)
        final_cols = (plan_names + [dctc.SD] + [dctc.ED] + [vmc.cost] +
                      [dctc.PNC] + [self.delivery_col] +
                      [self.proj_completion_col] + [self.pacing_goal_col] +
                      [vmc.AD_COST])
        final_cols = [x for x in final_cols if x in tdf.columns]
        tdf = tdf[final_cols]
        tdf = tdf.replace([np.inf, -np.inf], np.nan).fillna(0)
        tdf = utl.data_to_type(
            tdf, float_col=[vmc.cost, dctc.PNC, vmc.AD_COST])
        for col in [dctc.PNC, vmc.cost, vmc.AD_COST]:
            tdf[col] = '$' + tdf[col].round(2).astype(str)
        for col in [dctc.SD, dctc.ED]:
            tdf[col] = [str(0) if x == 0
                        else str(pd.to_datetime(x).date()) for x in tdf[col]]
        return tdf

    def do_analysis(self):
        df = self.aly.df
        df = self.get_pacing_analysis(df)
        if df.empty:
            msg = 'Could not calculate pacing data.'
            logging.info('{}'.format(msg))
        else:
            msg = ('Projected delivery completion and current pacing '
                   'is as follows:')
            logging.info('{}\n{}'.format(msg, df.to_string()))
        self.aly.add_to_analysis_dict(key_col=self.aly.delivery_comp_col,
                                      message=msg, data=df.to_dict())


class GetDailyDelivery(AnalyzeBase):
    name = Analyze.placement_col
    fix = False
    pre_run = False
    num_days = 'Num Days'
    daily_spend_goal = 'Daily Spend Goal'
    day_pacing = 'Day Pacing'

    def get_daily_delivery(self, df):
        """
        Get daily delivery data for each unique planned net level breakout

        :param df: full output df
        """
        daily_dfs = []
        if df.empty:
            logging.warning('Dataframe empty cannot get daily delivery')
            return daily_dfs
        plan_names = self.matrix.vendor_set(vm.plan_key)[vmc.fullplacename]
        start_dates, end_dates = self.aly.get_start_end_dates(df, plan_names)
        pdf_cols = plan_names + [dctc.PNC, dctc.UNC]
        pdf = self.matrix.vendor_get(vm.plan_key)
        pdf = pdf[pdf_cols]
        groups = plan_names + [vmc.date]
        metrics = [cal.NCF]
        df = df.groupby(groups)[metrics].sum().reset_index()
        df = utl.data_to_type(df, date_col=[vmc.date])
        unique_breakouts = df.groupby(plan_names).first().reset_index()
        unique_breakouts = unique_breakouts[plan_names]
        sort_ascending = [True for _ in plan_names]
        sort_ascending.append(False)
        for index, row in unique_breakouts.iterrows():
            tdf = df
            for x in plan_names:
                tdf = tdf[tdf[x] == row[x]]
            tdf = tdf.merge(start_dates, how='left', on=plan_names)
            tdf = tdf.merge(end_dates, how='left', on=plan_names)
            tdf = tdf.merge(pdf, how='left', on=plan_names)
            tdf = utl.data_to_type(tdf, float_col=[dctc.PNC])
            tdf[self.num_days] = (tdf[dctc.ED] - tdf[dctc.SD]).dt.days
            tdf = tdf.replace([np.inf, -np.inf], np.nan).fillna(0)
            if tdf[self.num_days][0] == 0 or tdf[dctc.PNC][0] == 0:
                tdf[self.daily_spend_goal] = 0
                tdf[self.day_pacing] = '0%'
            else:
                daily_spend_goal = (tdf[dctc.PNC][0] / tdf[self.num_days][0])
                stop_date = (tdf[dctc.SD][0] +
                             dt.timedelta(days=int(tdf[self.num_days][0])))
                tdf[self.daily_spend_goal] = [daily_spend_goal if
                                              (tdf[dctc.SD][0] <= x <= stop_date
                                               ) else 0 for x in tdf[vmc.date]]
                tdf[self.day_pacing] = (
                        ((tdf[cal.NCF] / tdf[self.daily_spend_goal]) - 1) * 100)
                tdf[self.day_pacing] = tdf[self.day_pacing].replace(
                    [np.inf, -np.inf], np.nan).fillna(0.0)
                tdf[self.day_pacing] = (
                        tdf[self.day_pacing].round(2).astype(str) + '%')
            tdf = tdf.sort_values(
                plan_names + [vmc.date], ascending=sort_ascending)
            tdf[[dctc.SD, dctc.ED, vmc.date]] = tdf[
                [dctc.SD, dctc.ED, vmc.date]].astype(str)
            tdf = tdf.reset_index(drop=True)
            daily_dfs.append(tdf.to_dict())
        return daily_dfs

    def do_analysis(self):
        df = self.aly.df
        dfs = self.get_daily_delivery(df)
        msg = 'Daily delivery is as follows:'
        self.aly.add_to_analysis_dict(key_col=self.aly.daily_delivery_col,
                                      message=msg, data=dfs)


class GetServingAlerts(AnalyzeBase):
    name = Analyze.placement_col
    fix = False
    pre_run = False
    adserving_ratio = 'Adserving %'
    prog_vendors = ['DV360', 'dv360', 'DV 360', 'Verizon', 'VERIZON']

    def get_serving_alerts(self):
        """
        Check for adserving overages -- over 6% of net cost (> 2 stddevs)

        """
        pacing_analysis = self.aly.find_in_analysis_dict(
            self.aly.delivery_comp_col)[0]
        df = pd.DataFrame(pacing_analysis['data'])
        plan_names = self.aly.get_plan_names()
        if not plan_names:
            return pd.DataFrame()
        final_cols = plan_names + [vmc.cost, vmc.AD_COST, self.adserving_ratio]
        if not df.empty:
            df = utl.data_to_type(df, float_col=[vmc.cost, vmc.AD_COST])
            df[self.adserving_ratio] = df.apply(
                lambda row: 0 if row[vmc.cost] == 0
                else (row[vmc.AD_COST] / row[vmc.cost]) * 100, axis=1)
            df = df[(df[self.adserving_ratio] > 9) |
                    ((df[self.adserving_ratio] > 6) &
                     ~(df[dctc.VEN].isin(self.prog_vendors)))]
            if not df.empty:
                df[[vmc.cost, vmc.AD_COST]] = (
                        '$' + df[[vmc.cost, vmc.AD_COST]].round(2).astype(str))
                df[self.adserving_ratio] = (
                        df[self.adserving_ratio].round(2).astype(str) + "%")
                df = df[final_cols]
        return df

    def do_analysis(self):
        df = self.get_serving_alerts()
        if df.empty:
            msg = 'No significant adserving overages.'
            logging.info('{}\n{}'.format(msg, df))
        else:
            msg = 'Adserving cost significantly OVER for the following: '
            logging.info('{}\n{}'.format(msg, df))
        self.aly.add_to_analysis_dict(key_col=self.aly.adserving_alert,
                                      message=msg, data=df.to_dict())


class GetDailyPacingAlerts(AnalyzeBase):
    name = Analyze.placement_col
    fix = False
    pre_run = False
    day_pacing = 'Day Pacing'

    def get_daily_pacing_alerts(self):
        """
        Check daily pacing issues -- +/- 20% of daily pacing goal

        """
        dfs_dict = self.aly.find_in_analysis_dict(
            self.aly.daily_delivery_col)[0]['data']
        if not dfs_dict:
            logging.warning('Dataframes empty could not get alerts')
            return pd.DataFrame(), pd.DataFrame()
        yesterday = dt.datetime.strftime(
            dt.datetime.today() - dt.timedelta(days=1), '%Y-%m-%d')
        over_df = pd.DataFrame(columns=pd.DataFrame(dfs_dict[0]).columns)
        under_df = pd.DataFrame(columns=pd.DataFrame(dfs_dict[0]).columns)
        for data in dfs_dict:
            df = pd.DataFrame(data)
            if not df.empty:
                df = df[df[vmc.date] == yesterday]
                if not df.empty:
                    val = df[self.day_pacing].iloc[0]
                    val = float(val.replace("%", ""))
                    if val >= 20:
                        over_df = pd.concat([over_df, df], ignore_index=True)
                    if val <= -20:
                        df[self.day_pacing].iloc[0] = (
                            df[self.day_pacing].iloc[0].replace("-", ""))
                        under_df = pd.concat([under_df, df], ignore_index=True)
        return over_df, under_df

    def do_analysis(self):
        over_df, under_df = self.get_daily_pacing_alerts()
        if over_df.empty:
            msg = 'No significant daily pacing overages.'
            logging.info('{}\n{}'.format(msg, over_df))
        else:
            msg = ('Yesterday\'s spend for the following exceeded '
                   'daily pacing goal by:')
            logging.info('{}\n{}'.format(msg, over_df))
        self.aly.add_to_analysis_dict(
            key_col=self.aly.daily_pacing_alert, message=msg,
            param=self.aly.over_daily_pace, data=over_df.to_dict())
        if under_df.empty:
            msg = 'No significant daily under pacing.'
            logging.info('{}\n{}'.format(msg, under_df))
        else:
            msg = ('Yesterday\'s spend for the following under paced the '
                   'daily goal by:')
            logging.info('{}\n{}'.format(msg, under_df))
        self.aly.add_to_analysis_dict(
            key_col=self.aly.daily_pacing_alert, message=msg,
            param=self.aly.under_daily_pace, data=under_df.to_dict())


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
                        'CPCV', 'CPLPV', 'CPP', 'CPM', 'VCR', 'CPV']
        formula = ['Clicks/Impressions', 'Net Cost Final/Clicks',
                   'Net Cost Final/Conv1_CPA', 'Net Cost Final/Landing Page',
                   'Net Cost Final/Button Click', 'Video Views 100/Video Views',
                   'Net Cost Final/Video Views', 'Net Cost Final/Landing Page',
                   'Net Cost Final/Purchase', 'Net Cost Final/Impressions',
                   'Video Views 100/Video Views', 'Net Cost Final/Video Views']
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
                if item not in df.columns:
                    df[item] = 0
                df[col] = df[item]
        return df
