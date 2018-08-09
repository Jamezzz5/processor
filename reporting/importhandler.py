import os
import logging
import pandas as pd
import datetime as dt
import reporting.fbapi as fbapi
import reporting.awapi as awapi
import reporting.twapi as twapi
import reporting.ttdapi as ttdapi
import reporting.gaapi as gaapi
import reporting.nbapi as nbapi
import reporting.scapi as scapi
import reporting.ajapi as ajapi
import reporting.dcapi as dcapi
import reporting.rsapi as rsapi
import reporting.dbapi as dbapi
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

    def output(self, api_df, filename, api_merge=None, first_row=None,
               last_row=None, date_col=None, start_date=None, end_date=None):
        utl.dir_check(utl.raw_path)
        if str(api_merge) != 'nan':
            api_df = self.merge_df(api_df, filename, date_col, start_date,
                                   end_date, first_row, last_row, api_merge)
        full_file = os.path.join(utl.raw_path, filename)
        try:
            api_df.to_csv(full_file, index=False, encoding='utf-8')
        except IOError:
            logging.warning('{} could not be opened.'
                            'API data was not saved.'.format(full_file))

    def merge_df(self, api_df, filename, date_col, start_date, end_date,
                 first_row, last_row, api_merge):
        if not os.path.isfile(os.path.join(utl.raw_path, filename)):
            return api_df
        df = utl.import_read_csv(filename, utl.raw_path)
        df = self.merge_df_cleaning(df, first_row, last_row, date_col, pd.NaT,
                                    end_date - dt.timedelta(days=api_merge))
        api_df = self.merge_df_cleaning(api_df, first_row, last_row, date_col,
                                        start_date, end_date)
        df = df.append(api_df, ignore_index=True).reset_index(drop=True)
        df = utl.add_dummy_header(df, first_row)
        df = utl.add_dummy_header(df, last_row, location='foot')
        return df

    @staticmethod
    def merge_df_cleaning(df, first_row, last_row, date_col,
                          start_date, end_date):
        df = utl.first_last_adj(df, first_row, last_row)
        df = utl.data_to_type(df, date_col=date_col)
        df = utl.date_removal(df, date_col[0], start_date, end_date)
        return df

    @staticmethod
    def create_all_col(df):
        df['ALL'] = ''
        for col in df.columns:
            df['ALL'] = df['ALL'] + df[col].astype('U')
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

    @staticmethod
    def set_start(sd, ed, max_date):
        if str(max_date) != 'nan':
            date_diff = (ed - sd).days
            if date_diff >= max_date:
                sd = ed - dt.timedelta(days=max_date)
        return sd

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
            params[vmc.startdate] = self.set_start(params[vmc.startdate],
                                                   params[vmc.enddate],
                                                   params[vmc.apimerge])
            df = api_class.get_data(sd=params[vmc.startdate],
                                    ed=params[vmc.enddate],
                                    fields=params[vmc.apifields])
            self.output(df, params[vmc.filename], params[vmc.apimerge],
                        params[vmc.firstrow], params[vmc.lastrow],
                        params[vmc.date], params[vmc.startdate],
                        params[vmc.enddate])

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
        if self.arg_check('aj'):
            self.api_calls(self.matrix.api_aj_key, ajapi.AjApi())
        if self.arg_check('dc'):
            self.api_calls(self.matrix.api_dc_key, dcapi.DcApi())
        if self.arg_check('rs'):
            self.api_calls(self.matrix.api_rs_key, rsapi.RsApi())
        if self.arg_check('db'):
            self.api_calls(self.matrix.api_db_key, dbapi.DbApi())

    def ftp_load(self, ftp_key, ftp_class):
        for vk in ftp_key:
            params = self.matrix.vendor_set(vk)
            ftp_class.input_config(params[vmc.apifile])
            ftp_class.header = params[vmc.firstrow]
            df = ftp_class.get_data()
            self.output(df, params[vmc.filename], params[vmc.apimerge],
                        params[vmc.firstrow], params[vmc.lastrow],
                        params[vmc.date], params[vmc.startdate],
                        params[vmc.enddate])

    def ftp_loop(self):
        if self.arg_check('sz'):
            self.ftp_load(self.matrix.ftp_sz_key, ftp.FTP())

    def db_load(self, db_key, db_class):
        for vk in db_key:
            params = self.matrix.vendor_set(vk)
            db_class.input_config(params[vmc.apifile])
            df = db_class.get_data(filename=params[vmc.apifields][0])
            self.output(df, params[vmc.filename], params[vmc.apimerge],
                        params[vmc.firstrow], params[vmc.lastrow],
                        params[vmc.date], params[vmc.startdate],
                        params[vmc.enddate])

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
            self.output(df, params[vmc.filename], params[vmc.apimerge],
                        params[vmc.firstrow], params[vmc.lastrow],
                        params[vmc.date], params[vmc.startdate],
                        params[vmc.enddate])

    def s3_loop(self):
        if self.arg_check('dna'):
            self.s3_load(self.matrix.s3_dna_key, awss3.S3())
