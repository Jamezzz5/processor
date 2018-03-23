import os
import logging
import pandas as pd
import reporting.fbapi as fbapi
import reporting.awapi as awapi
import reporting.twapi as twapi
import reporting.ttdapi as ttdapi
import reporting.gaapi as gaapi
import reporting.nbapi as nbapi
import reporting.scapi as scapi
import reporting.ftp as ftp
import reporting.awss3 as awss3
import reporting.afapi as afapi
import reporting.export as export
import reporting.vmcolumns as vmc
import reporting.utils as utl


class ImportHandler(object):
    def __init__(self, args, matrix):
        self.args = args
        self.matrix = matrix

    def output(self, api_merge, api_df, filename, first_row, last_row, vk):
        utl.dir_check(utl.raw_path)
        if str(api_merge) != 'nan':
            api_merge_file = utl.raw_path + str(api_merge)
            if os.path.isfile(api_merge_file):
                try:
                    df = pd.read_csv(api_merge_file, parse_dates=True)
                except IOError:
                    logging.warning(api_merge + ' could not be opened.  ' +
                                    'API data was not merged.')
                    api_df.to_csv(api_merge_file)
                    return None
                df = utl.first_last_adj(df, first_row, last_row)
                df = self.create_all_col(df)
                api_df = utl.first_last_adj(api_df, first_row, last_row)
                api_df = self.create_all_col(api_df)
                api_df = api_df[~api_df['ALL'].isin(df['ALL'])]
                df = df.append(api_df, ignore_index=True)
                df.drop('ALL', axis=1, inplace=True)
                df.to_csv(api_merge_file, index=False)
                if first_row != 0:
                    self.matrix.vm_change(vk, vmc.firstrow, 0)
                if last_row != 0:
                    self.matrix.vm_change(vk, vmc.lastrow, 0)
            else:
                logging.warning(api_merge + ' not found.  Creating file.')
                df = pd.DataFrame()
                df = df.append(api_df, ignore_index=True)
                df.to_csv(api_merge_file, index=False)
        else:
            try:
                api_df.to_csv(utl.raw_path + filename, index=False)
            except UnicodeEncodeError:
                api_df.to_csv(utl.raw_path + filename, index=False,
                              encoding='utf-8')
            except IOError:
                logging.warning(utl.raw_path + filename + ' could not be ' +
                                'opened.  API data was not saved.')

    @staticmethod
    def create_all_col(df):
        df['ALL'] = ''
        for col in df.columns:
            df['ALL'] = df['ALL'] + df[col].astype(str)
        return df

    def arg_check(self, arg_check):
        if self.args == arg_check or self.args == 'all':
            return True
        else:
            return False

    @staticmethod
    def date_check(date):
        if date.date() is pd.NaT:
            return True
        return False

    def api_calls(self, key_list, api_class):
        for vk in key_list:
            params = self.matrix.vendor_set(vk)
            api_class.input_config(params[vmc.apifile])
            start_check = self.date_check(params[vmc.startdate])
            end_check = self.date_check(params[vmc.enddate])
            if params[vmc.apifields] == ['nan']:
                params[vmc.apifields] = None
            if start_check:
                params[vmc.startdate] = None
            if end_check:
                params[vmc.enddate] = None
            df = api_class.get_data(sd=params[vmc.startdate],
                                    ed=params[vmc.enddate],
                                    fields=params[vmc.apifields])
            self.output(params[vmc.apimerge], df, params[vmc.filename],
                        params[vmc.firstrow], params[vmc.lastrow], vk)

    def api_loop(self):
        if self.arg_check('fb'):
            self.api_calls(self.matrix.api_fb_key, fbapi.FbApi())
        if self.arg_check('aw'):
            self.api_calls(self.matrix.api_aw_key, awapi.AwApi())
        if self.arg_check('tw'):
            self.api_calls(self.matrix.api_tw_key, twapi.TwApi())
        if self.arg_check('ttd'):
            self.api_calls(self.matrix.api_ttd_key, ttdapi.TtdApi())
        if self.arg_check('ga'):
            self.api_calls(self.matrix.api_ga_key, gaapi.GaApi())
        if self.arg_check('nb'):
            self.api_calls(self.matrix.api_nb_key, nbapi.NbApi())
        if self.arg_check('af'):
            self.api_calls(self.matrix.api_af_key, afapi.AfApi())
        if self.arg_check('sc'):
            self.api_calls(self.matrix.api_sc_key, scapi.ScApi())

    def ftp_load(self, ftp_key, ftp_class):
        for vk in ftp_key:
            params = self.matrix.vendor_set(vk)
            ftp_class.input_config(params[vmc.apifile])
            ftp_class.header = params[vmc.firstrow]
            df = ftp_class.get_data()
            self.output(params[vmc.apimerge], df, params[vmc.filename],
                        params[vmc.firstrow], params[vmc.lastrow], vk)

    def ftp_loop(self):
        if self.arg_check('sz'):
            self.ftp_load(self.matrix.ftp_sz_key, ftp.FTP())

    def db_load(self, db_key, db_class):
        for vk in db_key:
            params = self.matrix.vendor_set(vk)
            db_class.input_config(params[vmc.apifile])
            df = db_class.get_data(filename=params[vmc.apifields][0])
            self.output(params[vmc.apimerge], df, params[vmc.filename],
                        params[vmc.firstrow], params[vmc.lastrow], vk)

    def db_loop(self):
        if self.arg_check('dna'):
            self.db_load(self.matrix.db_dna_key, export.DB())

    def s3_load(self, s3_key, s3_class):
        for vk in s3_key:
            params = self.matrix.vendor_set(vk)
            s3_class.input_config(params[vmc.apifile])
            start_check = self.date_check(params[vmc.startdate])
            end_check = self.date_check(params[vmc.enddate])
            if start_check:
                params[vmc.startdate] = None
            if end_check:
                params[vmc.enddate] = None
            df = s3_class.get_data(sd=params[vmc.startdate],
                                   ed=params[vmc.enddate])
            self.output(params[vmc.apimerge], df, params[vmc.filename],
                        params[vmc.firstrow], params[vmc.lastrow], vk)

    def s3_loop(self):
        if self.arg_check('dna'):
            self.s3_load(self.matrix.s3_dna_key, awss3.S3())
