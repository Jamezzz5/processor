import os
import time
import logging
import pandas as pd
import datetime as dt
import processor.reporting.fbapi as fbapi
import processor.reporting.awapi as awapi
import processor.reporting.twapi as twapi
import processor.reporting.gaapi as gaapi
import processor.reporting.nbapi as nbapi
import processor.reporting.afapi as afapi
import processor.reporting.scapi as scapi
import processor.reporting.ajapi as ajapi
import processor.reporting.dcapi as dcapi
import processor.reporting.rsapi as rsapi
import processor.reporting.dbapi as dbapi
import processor.reporting.vkapi as vkapi
import processor.reporting.rcapi as rcapi
import processor.reporting.ttdapi as ttdapi
import processor.reporting.szkapi as szkapi
import processor.reporting.redapi as redapi
import processor.reporting.dvapi as dvapi
import processor.reporting.adkapi as adkapi
import processor.reporting.innapi as innapi
import processor.reporting.tikapi as tikapi
import processor.reporting.amzapi as amzapi
import processor.reporting.criapi as criapi
import processor.reporting.pmapi as pmapi
import processor.reporting.samapi as samapi
import processor.reporting.gsapi as gsapi
import processor.reporting.qtapi as qtapi
import processor.reporting.yvapi as yvapi
import processor.reporting.ssapi as ssapi
import processor.reporting.nzapi as nzapi
import processor.reporting.ytdapi as ytdapi
import processor.reporting.simapi as simapi
import processor.reporting.pixapi as pixapi
import processor.reporting.iasapi as iasapi
import processor.reporting.steapi as steapi
import processor.reporting.asaapi as asaapi
import processor.reporting.ftp as ftp
import processor.reporting.awss3 as awss3
import processor.reporting.azapi as azu
import processor.reporting.export as export
import processor.reporting.vmcolumns as vmc
import processor.reporting.vendormatrix as vm
import processor.reporting.utils as utl


def api_key_of(vk):
    """The API type a vendor key names: ``API_Facebook_A`` -> ``Facebook``."""
    parts = str(vk).split('_')
    return parts[1] if len(parts) > 1 else ''


def reach_figures(df, reach_cols):
    """``df``'s reach columns as numbers, blank where a row has none."""
    present = [x for x in reach_cols or [] if x in df.columns]
    return df[present].apply(pd.to_numeric, errors='coerce')


def unreplaced_reach(old, new, reach_cols):
    """The reach rows of ``old`` for a report ``new`` has no figure in, such
    as the whole range row a platform's reach cap skipped."""
    old_figures = reach_figures(old, reach_cols)
    new_figures = reach_figures(new, reach_cols)
    filled = [x for x in new_figures if new_figures[x].notna().any()]
    replaced = old_figures.reindex(columns=filled).notna().any(axis=1)
    return old.loc[old_figures.notna().any(axis=1) & ~replaced]


class ImportHandler(object):
    def __init__(self, args, matrix):
        self.args = args
        self.matrix = matrix
        self.class_list = {
            vmc.api_fb_key: fbapi.FbApi,
            vmc.api_aw_key: awapi.AwApi,
            vmc.api_goad_key: awapi.AwApi,
            vmc.api_tw_key: twapi.TwApi,
            vmc.api_ttd_key: ttdapi.TtdApi,
            vmc.api_ga_key: gaapi.GaApi,
            vmc.api_nb_key: nbapi.NbApi,
            vmc.api_af_key: afapi.AfApi,
            vmc.api_sc_key: scapi.ScApi,
            vmc.api_aj_key: ajapi.AjApi,
            vmc.api_dc_key: dcapi.DcApi,
            vmc.api_db_key: dbapi.DbApi,
            vmc.api_dvo_key: dbapi.DbApi,
            vmc.api_vk_key: vkapi.VkApi,
            vmc.api_rs_key: rsapi.RsApi,
            vmc.api_rc_key: rcapi.RcApi,
            vmc.api_szk_key: szkapi.SzkApi,
            vmc.api_red_key: redapi.RedApi,
            vmc.api_dv_key: dvapi.DvApi,
            vmc.api_adk_key: adkapi.AdkApi,
            vmc.api_inn_key: innapi.InnApi,
            vmc.api_tik_key: tikapi.TikApi,
            vmc.api_amz_key: amzapi.AmzApi,
            vmc.api_cri_key: criapi.CriApi,
            vmc.api_pm_key: pmapi.PmApi,
            vmc.api_sam_key: samapi.SamApi,
            vmc.api_gs_key: gsapi.GsApi,
            vmc.api_qt_key: qtapi.QtApi,
            vmc.api_yv_key: yvapi.YvApi,
            vmc.api_amd_key: amzapi.AmzApi,
            vmc.api_ss_key: ssapi.SsApi,
            vmc.api_nz_key: nzapi.NzApi,
            vmc.api_ytd_key: ytdapi.YtdApi,
            vmc.api_wal_key: ttdapi.TtdApi,
            vmc.api_sim_key: simapi.SimApi,
            vmc.api_pix_key: pixapi.PixApi,
            vmc.api_ias_key: iasapi.IasApi,
            vmc.api_ste_key: steapi.SteApi,
            vmc.api_asa_key: asaapi.AsaApi,
        }

    def output(self, api_df, filename, api_merge=None, first_row=None,
               last_row=None, date_col=None, start_date=None, end_date=None,
               reach_cols=None, keep_reach=False):
        """Writes a df to disk from an API

        Keyword arguments:
        api_df -- the dataframe to be written
        filename -- the name of the file to write to on disk
        reach_cols -- the api's reach columns, whose old rows new ones replace
        keep_reach -- keep the old rows of a reach report this pull left empty
        """
        utl.dir_check(utl.raw_path)
        if '/' in filename:
            full_file = filename
        else:
            full_file = os.path.join(utl.raw_path, filename)
        if str(api_merge) != 'nan':
            api_df = self.merge_df(api_df, full_file, date_col, start_date,
                                   end_date, first_row, last_row, api_merge,
                                   reach_cols, keep_reach)
        elif keep_reach and not api_df.empty and os.path.isfile(full_file):
            old = utl.import_read_csv(full_file, empty_df=True)
            api_df = pd.concat(
                [api_df, unreplaced_reach(old, api_df, reach_cols)],
                ignore_index=True)
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
                 first_row, last_row, api_merge, reach_cols=None,
                 keep_reach=False):
        """The old raw file's delivery before the merge window beside the
        api's for it, and the api's reach rows whole, as pull_card pulls
        them from the card's start.  Each replaces the old rows of its
        report, which the export would sum with it; with ``keep_reach`` a
        report the api left empty keeps its old rows.
        """
        if not os.path.isfile(filename):
            return api_df
        df = self.merge_df_cleaning(utl.import_read_csv(filename), first_row,
                                    last_row, date_col, pd.NaT, pd.NaT)
        api_df = self.merge_df_cleaning(api_df, first_row, last_row,
                                        date_col, pd.NaT, pd.NaT)
        old_reach = reach_figures(df, reach_cols).notna().any(axis=1)
        new_reach = reach_figures(api_df, reach_cols).notna().any(axis=1)
        cutoff = end_date - dt.timedelta(days=api_merge)
        frames = [
            utl.date_removal(df.loc[~old_reach], date_col[0], pd.NaT, cutoff),
            utl.date_removal(api_df.loc[~new_reach], date_col[0], start_date,
                             end_date),
            api_df.loc[new_reach]]
        if keep_reach:
            frames.append(unreplaced_reach(df, api_df, reach_cols))
        df = pd.concat(frames, ignore_index=True)
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
        if isinstance(self.args, str):
            args = [self.args]
        else:
            args = self.args
        if arg_check in args or 'all' in self.args:
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

        A key's campaign filter record is drained even when the key
        raises, so it never lands on the next card.
        """
        stats_by_vk = {}
        utl.drain_campaign_filter_stats()
        for vk in key_list:
            try:
                self.api_call(vk, api_class)
            finally:
                stat = utl.drain_campaign_filter_stats()
            if stat:
                stat['date'] = dt.datetime.today().strftime(
                    '%Y-%m-%d %H:%M:%S')
                stats_by_vk[vk] = stat
        utl.write_campaign_filter_stats(stats_by_vk)

    def api_call(self, vk, api_class):
        """Pull one vendor key as a timed import step; a Selenium class's
        failure is recorded without stopping the loop."""
        params = self.matrix.vendor_set(vk)
        with utl.TimedStep('import', api_key_of(vk), vk) as step:
            try:
                api_class.input_config(params[vmc.apifile])
            except (FileNotFoundError, ValueError, SystemExit) as e:
                reason = '' if isinstance(e, SystemExit) else ': {}'.format(e)
                logging.warning('Skipped {}{}'.format(vk, reason))
                step.status = 'skipped'
                step.error = e
                return
            utl.set_card_placement(api_class, params)
            start_check = self.date_check(params[vmc.startdate])
            end_check = self.date_check(params[vmc.enddate])
            if params[vmc.apifields] == ['nan']:
                params[vmc.apifields] = None
            if start_check:
                params[vmc.startdate] = None
            if end_check:
                params[vmc.enddate] = None
            card_start = params[vmc.startdate]
            params[vmc.startdate] = self.set_start(params[vmc.startdate],
                                                   params[vmc.enddate],
                                                   params[vmc.apimerge])
            df = pd.DataFrame()
            try:
                df = self.pull_card(api_class, params, card_start)
            except Exception as e:
                logging.error("API error occurred", exc_info=True)
                if hasattr(api_class, 'sw') and api_class.sw:
                    api_class.sw.quit()
                    step.error = e
                else:
                    raise e
            self.output_source(df, params, step,
                               getattr(api_class, 'reach_cols', None))

    @staticmethod
    def pull_card(api_class, params, card_start):
        """The card's delivery and, on a merge card whose api reports
        reach, its reach pulled apart from ``card_start``: reach over the
        merge window would count only the window's people."""
        fields = params[vmc.apifields]
        sd, ed = params[vmc.startdate], params[vmc.enddate]
        if (str(params[vmc.apimerge]) == 'nan'
                or not hasattr(api_class, 'get_reach')
                or not utl.pull_reach(fields)):
            return api_class.get_data(sd=sd, ed=ed, fields=fields)
        delivery = api_class.get_data(
            sd=sd, ed=ed, fields=[*(fields or []), utl.no_reach_field])
        reach = api_class.get_reach(sd=card_start, ed=ed, fields=fields)
        return pd.concat([delivery, reach], ignore_index=True)

    def output_source(self, df, params, step, reach_cols=None):
        """Write a vendor key's pulled ``df`` and count its rows on the
        timed ``step``; unless the card says No Reach, a reach report the
        pull left empty keeps its last rows."""
        step.rows = len(df)
        keep_reach = bool(reach_cols) and utl.pull_reach(params[vmc.apifields])
        self.output(df, params[vmc.filename], params[vmc.apimerge],
                    params[vmc.firstrow], params[vmc.lastrow],
                    params[vmc.date], params[vmc.startdate],
                    params[vmc.enddate], reach_cols, keep_reach)

    def api_loop(self):
        """Loops through all APIs and makes function call to retrieve data.

        """
        for key, api in self.class_list.items():
            if (self.arg_check(vmc.api_translation[key]) and
                    self.matrix.vks[key]):
                self.api_calls(self.matrix.vks[key], api())

    def test_api_calls(self, key_list):
        """Makes an API Call to Test Connection

        Keyword arguments:
        key_list -- list of Vendormatrix keys to test
        """
        import_config = vm.ImportConfig()
        import_config.import_vm()
        df = pd.DataFrame()
        for vk in key_list:
            data_source = self.matrix.get_data_source(vk=vk)
            if import_config.config_file not in data_source.params:
                continue
            config_name = str(data_source.params[import_config.config_file])
            api_type = api_key_of(vk)
            api_class = self.class_list[api_type]()
            ic_df = import_config.df.loc[
                import_config.df[import_config.key] == api_type]
            acc_col = ic_df.iloc[0][import_config.account_id]
            camp_col = ic_df.iloc[0][import_config.filter]
            acc_pre = ic_df.iloc[0][import_config.account_id_pre]
            tdf = self.config_failure(api_class, config_name, acc_col)
            if tdf is None:
                tdf = api_class.test_connection(acc_col, camp_col, acc_pre)
            tdf[vmc.vendorkey] = vk
            df = pd.concat([df, tdf])
        return df

    @staticmethod
    def config_failure(api_class, config_name, acc_col):
        """Load a card's config for Test Connection.

        A card the import loop would skip gets a failure row with the
        reason instead: the skip is a SystemExit or a ValueError, which
        would otherwise end the whole test.

        :param api_class: the API object that reads the config
        :param config_name: the card's API_FILE value
        :param acc_col: the account column the result row is under
        :returns: None when the config loaded, else a one-row frame
        """
        counter = utl.StepLogCounter()
        logging.getLogger().addHandler(counter)
        try:
            api_class.input_config(config_name)
        except (FileNotFoundError, ValueError, SystemExit) as e:
            reason = counter.last_error
            if not isinstance(e, SystemExit):
                reason = str(e)
            reason = reason or ("This card's API file could not be loaded.  "
                                "Check the API file on the card.")
            return pd.DataFrame([[acc_col, 'FAILURE: {}'.format(reason),
                                  False]], columns=vmc.r_cols)
        finally:
            logging.getLogger().removeHandler(counter)
        return None

    def ftp_load(self, ftp_key, ftp_class):
        for vk in ftp_key:
            params = self.matrix.vendor_set(vk)
            with utl.TimedStep('files', api_key_of(vk), vk) as step:
                ftp_class.input_config(params[vmc.apifile])
                ftp_class.header = params[vmc.firstrow]
                df = ftp_class.get_data()
                self.output_source(df, params, step)

    def ftp_loop(self):
        if self.arg_check('sz'):
            self.ftp_load(self.matrix.ftp_sz_key, ftp.FTP())

    def db_load(self, db_key, db_class):
        for vk in db_key:
            params = self.matrix.vendor_set(vk)
            with utl.TimedStep('files', api_key_of(vk), vk) as step:
                db_class.input_config(params[vmc.apifile])
                df = db_class.get_data(filename=params[vmc.apifields][0])
                self.output_source(df, params, step)

    def db_loop(self):
        if self.arg_check('dna'):
            self.db_load(self.matrix.db_dna_key, export.DB())

    def s3_load(self, s3_key, s3_class):
        for vk in s3_key:
            params = self.matrix.vendor_set(vk)
            with utl.TimedStep('files', api_key_of(vk), vk) as step:
                s3_class.input_config(params[vmc.apifile])
                start_check = self.date_check(params[vmc.startdate])
                end_check = self.date_check(params[vmc.enddate])
                if start_check:
                    params[vmc.startdate] = None
                if end_check:
                    params[vmc.enddate] = None
                df = s3_class.get_data(sd=params[vmc.startdate],
                                       ed=params[vmc.enddate])
                self.output_source(df, params, step)

    def s3_loop(self):
        if self.arg_check('dna'):
            self.s3_load(self.matrix.s3_dna_key, awss3.S3())

    def azu_load(self, azu_key, azu_class):
        for vk in azu_key:
            params = self.matrix.vendor_set(vk)
            with utl.TimedStep('files', api_key_of(vk), vk) as step:
                azu_class.input_config(params[vmc.apifile])
                azu_class.header = params[vmc.firstrow]
                df = azu_class.get_data()
                self.output_source(df, params, step)

    def azu_loop(self):
        if self.arg_check('dna'):
            self.azu_load(self.matrix.azu_dna_key, azu.AzuApi())
