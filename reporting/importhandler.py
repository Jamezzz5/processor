import os
import time
import logging
import pandas as pd
import datetime as dt
import reporting.fbapi as fbapi
import reporting.awapi as awapi
import reporting.twapi as twapi
import reporting.gaapi as gaapi
import reporting.nbapi as nbapi
import reporting.scapi as scapi
import reporting.ajapi as ajapi
import reporting.dcapi as dcapi
import reporting.rsapi as rsapi
import reporting.dbapi as dbapi
import reporting.vkapi as vkapi
import reporting.rcapi as rcapi
import reporting.ttdapi as ttdapi
import reporting.szkapi as szkapi
import reporting.redapi as redapi
import reporting.dvapi as dvapi
import reporting.adkapi as adkapi
import reporting.innapi as innapi
import reporting.tikapi as tikapi
import reporting.amzapi as amzapi
import reporting.criapi as criapi
import reporting.pmapi as pmapi
import reporting.samapi as samapi
import reporting.gsapi as gsapi
import reporting.qtapi as qtapi
import reporting.yvapi as yvapi
import reporting.ssapi as ssapi
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
        """Writes a df to disk from an API

        Keyword arguments:
        api_df -- the dataframe to be written
        filename -- the name of the file to write to on disk
        """
        utl.dir_check(utl.raw_path)
        if str(api_merge) != 'nan':
            api_df = self.merge_df(api_df, filename, date_col, start_date,
                                   end_date, first_row, last_row, api_merge)
        if '/' in filename:
            full_file = filename
        else:
            full_file = os.path.join(utl.raw_path, filename)
        self.write_df(api_df, full_file)

    def write_df(self, api_df, full_file, attempt=0):
        if not api_df.empty:
            file_written = utl.write_file(api_df, full_file)
            if not file_written:
                logging.warning('{} could not be opened.  API data was '
                                'not saved will attempt {} '
                                'more times.'.format(full_file, 4 - attempt))
                attempt += 1
                if attempt < 5:
                    time.sleep(15)
                    self.write_df(api_df, full_file, attempt)
        else:
            logging.warning('Imported df empty - '
                            'not overwriting {}.'.format(full_file))

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
        """Makes an API Call

        Keyword arguments:
        key_list -- list of Vendormatrix keys for a given API
        api_class -- The class of API to call
        """
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
        """Loops through all APIs and makes function call to retrieve data.

        """
        apis = [('fb', self.matrix.api_fb_key, fbapi.FbApi),
                ('aw', self.matrix.api_aw_key, awapi.AwApi),
                ('tw', self.matrix.api_tw_key, twapi.TwApi),
                ('ttd', self.matrix.api_ttd_key, ttdapi.TtdApi),
                ('ga', self.matrix.api_ga_key, gaapi.GaApi),
                ('nb', self.matrix.api_nb_key, nbapi.NbApi),
                ('af', self.matrix.api_af_key, afapi.AfApi),
                ('sc', self.matrix.api_sc_key, scapi.ScApi),
                ('aj', self.matrix.api_aj_key, ajapi.AjApi),
                ('dc', self.matrix.api_dc_key, dcapi.DcApi),
                ('db', self.matrix.api_db_key, dbapi.DbApi),
                ('vk', self.matrix.api_vk_key, vkapi.VkApi),
                ('rs', self.matrix.api_rs_key, rsapi.RsApi),
                ('rc', self.matrix.api_rc_key, rcapi.RcApi),
                ('szk', self.matrix.api_szk_key, szkapi.SzkApi),
                ('red', self.matrix.api_red_key, redapi.RedApi),
                ('dv', self.matrix.api_dv_key, dvapi.DvApi),
                ('adk', self.matrix.api_adk_key, adkapi.AdkApi),
                ('inn', self.matrix.api_inn_key, innapi.InnApi),
                ('tik', self.matrix.api_tik_key, tikapi.TikApi),
                ('amz', self.matrix.api_amz_key, amzapi.AmzApi),
                ('cri', self.matrix.api_cri_key, criapi.CriApi),
                ('pm', self.matrix.api_pm_key, pmapi.PmApi),
                ('sam', self.matrix.api_sam_key, samapi.SamApi),
                ('gs', self.matrix.api_gs_key, gsapi.GsApi),
                ('qt', self.matrix.api_qt_key, qtapi.QtApi),
                ('yv', self.matrix.api_yv_key, yvapi.YvApi),
                ('amd', self.matrix.api_amd_key, amzapi.AmzApi),
                ('ss', self.matrix.api_ss_key, ssapi.SsApi)]
        for api in apis:
            if self.arg_check(api[0]) and api[1]:
                self.api_calls(api[1], api[2]())

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
