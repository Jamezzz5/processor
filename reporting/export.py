import os
import re
import io
import sys
import json
import math
import time
import logging
import psycopg2
import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy as sqa
import reporting.ftp as ftp
import reporting.utils as utl
import reporting.models as mdl
import reporting.awss3 as awss3
import reporting.tbapi as tbapi
import reporting.expcolumns as exc

log = logging.getLogger()
config_path = utl.config_path


class ExportHandler(object):
    def __init__(self):
        self.export_list = None
        self.config = None
        self.args = None
        self.config_file = 'config/export_handler.csv'
        self.load_config(self.config_file)

    def load_config(self, config_file):
        df = pd.read_csv(config_file)
        self.export_list = df[exc.export_key].tolist()
        self.config = df.set_index(exc.export_key).to_dict()

    def export_loop(self, args):
        self.args = args
        for exp_key in self.export_list:
            self.export_item_check_type(exp_key)

    def export_item_check_type(self, exp_key):
        if (self.config[exc.export_type][exp_key] == 'DB' and
           (self.args == 'db' or self.args == 'all')):
            self.export_db(exp_key)
        elif (self.config[exc.export_type][exp_key] == 'FTP' and
              (self.args == 'ftp' or self.args == 'all')):
            self.export_ftp(exp_key)
        elif (self.config[exc.export_type][exp_key] == 'S3' and
              (self.args == 's3' or self.args == 'all')):
            self.export_s3(exp_key)

    def export_db(self, exp_key):
        dbu = DBUpload()
        upload_success = dbu.upload_to_db(
            db_file=self.config[exc.config_file][exp_key],
            schema_file=self.config[exc.schema_file][exp_key],
            translation_file=self.config[exc.translation_file][exp_key],
            data_file=self.config[exc.output_file][exp_key])
        if not upload_success:
            return False
        if exp_key == exc.default_export_db_key:
            filter_val = dbu.dft.df[exc.product_name].drop_duplicates()[0]
            view_name = 'auto_{}'.format(re.sub(r'\W+', '', filter_val).lower())
            self.create_view(dbu, filter_val, view_name)
            self.update_tableau(dbu.db, view_name)
        return True

    @staticmethod
    def create_view(dbu, filter_val, view_name):
        sb = ScriptBuilder()
        exist_script = """
        select
            count(*)
        from
            INFORMATION_SCHEMA.VIEWS
        where
            table_name = '{}'
            and table_schema = '{}'
        """.format('{}'.format(view_name), 'lqadb')
        dbu.db.cursor.execute(exist_script)
        dbu.db.connection.commit()
        data = dbu.db.cursor.fetchall()
        if data[0][0] == 1:
            logging.info('Dropping view: {}'.format(view_name))
            drop_script = """drop view {}.{};""".format('lqadb', view_name)
            try:
                dbu.db.cursor.execute(drop_script)
            except psycopg2.errors.UndefinedTable as e:
                logging.warning('Could not find table error: {}'.format(e))
                dbu.db.connection.rollback()
                return None
            dbu.db.connection.commit()
        logging.info('Creating view {}'.format(view_name))
        view_script = sb.get_view_script(
            exc.product_name, filter_val, exc.product_table,
            view_name)
        dbu.db.connect()
        dbu.db.cursor.execute(view_script, [filter_val])
        dbu.db.connection.commit()
        logging.info('View created.')

    @staticmethod
    def update_tableau(db, view_name):
        tb = tbapi.TabApi()
        tb.create_publish_workbook_hyper(
            db, table_name=view_name, new_wb_name=view_name)

    def export_ftp(self, exp_key):
        ftp_class = ftp.FTP()
        dft_class = DFTranslation(self.config[exc.translation_file][exp_key],
                                  self.config[exc.output_file][exp_key])
        ftp_class.input_config(self.config[exc.config_file][exp_key])
        ftp_class.ftp_write_file(dft_class.df,
                                 self.config[exc.output_file][exp_key])

    def export_s3(self, exp_key):
        s3_class = awss3.S3()
        db = DB(config='dbconfig.json')
        dft_class = DFTranslation(self.config[exc.translation_file][exp_key],
                                  self.config[exc.output_file][exp_key], db)
        if dft_class.df.empty:
            logging.warning('Empty df stopping export.')
            return False
        s3_class.input_config(self.config[exc.config_file][exp_key])
        if exc.default_format in self.config:
            default_format = self.config[exc.default_format][exp_key]
            if str(default_format) == 'nan':
                default_format = True
        else:
            default_format = True
        s3_class.s3_write_file(dft_class.df, default_format=default_format)
        return True


class DBUpload(object):
    def __init__(self):
        self.db = None
        self.dbs = None
        self.dft = None
        self.table = None
        self.id_col = None
        self.name = None
        self.values = None

    def upload_to_db(self, db_file, schema_file, translation_file, data_file):
        self.db = DB(db_file)
        logging.info('Uploading {} to {}'.format(data_file, self.db.db))
        self.dbs = DBSchema(schema_file)
        self.dft = DFTranslation(translation_file, data_file, self.db)
        if self.dft.df.empty:
            logging.warning('Dataframe empty stopping upload.')
            return False
        for table in self.dbs.table_list:
            self.upload_table_to_db(table)
        logging.info(
            '{} successfully uploaded to {}'.format(data_file, self.db.db))
        return True

    def upload_table_to_db(self, table):
        logging.info('Uploading table ' + table + ' to ' + self.db.db)
        ul_df = self.get_upload_df(table)
        if ul_df.empty:
            return None
        self.dbs.set_table(table)
        pk_config = {table: list(self.dbs.pk.items())[0]}
        self.set_id_info(table, pk_config, ul_df)
        if exc.upload_id_col in ul_df.columns:
            where_col = exc.upload_id_col
            where_val = [int(self.dft.upload_id)]
        else:
            where_col = self.name
            where_val = self.values
        df_rds = self.read_rds_table(table, list(ul_df.columns),
                                     where_col, where_val)
        df = pd.merge(df_rds, ul_df, how='outer', on=self.name, indicator=True)
        df = df.drop_duplicates(self.name).reset_index()
        self.update_rows(df, df_rds.columns, table)
        self.delete_rows(df, table)
        self.insert_rows(df, table)

    def get_upload_df(self, table):
        cols = self.dbs.get_cols_for_export(table)
        cols_to_add = []
        other_event_cols = [
            exc.event_steam_name, exc.event_conv_name, exc.event_plan_name]
        if exc.event_name in cols and not any(
                [x in cols for x in other_event_cols]):
            cols_to_add = [x for x in self.dft.real_columns
                           if x not in cols and
                           x not in ['plannednetcost', 'modelcoefa',
                                     'modelcoefb', 'modelcoefc']]
            cols += cols_to_add
        ul_df = self.dft.slice_for_upload(cols)
        if cols_to_add:
            ul_df = utl.col_removal(ul_df, key='None', removal_cols=cols_to_add,
                                    warn=False)
        if not ul_df.empty:
            ul_df = self.add_ids_to_df(self.dbs.fk, ul_df)
        return ul_df

    def read_rds_table(self, table, cols, where_col, where_val):
        df_rds = self.db.read_rds_table(table, where_col, where_val)
        df_rds = df_rds[cols]
        df_rds = self.dft.clean_types_for_upload(df_rds)
        df_rds = self.delete_rds_duplicates(df_rds, table, cols,
                                            where_col, where_val)
        return df_rds

    def delete_rds_duplicates(self, df_rds, table, cols, where_col, where_val):
        if exc.upload_id_col in df_rds.columns:
            del_vals = (df_rds[df_rds[self.name].duplicated()]
                        [self.name].tolist())
            if del_vals:
                self.db.delete_rows(table, exc.upload_id_col,
                                    self.dft.upload_id, self.name, del_vals)
                df_rds = self.read_rds_table(table, cols, where_col, where_val)
        return df_rds

    def update_rows(self, df, cols, table):
        df_update = df[df['_merge'] == 'both']
        updated_index = []
        set_cols = [x for x in cols if x not in
                    [self.name, self.id_col, exc.upload_id_col]]
        for col in set_cols:
            df_changed = (df_update[df_update[col + '_y'] !=
                                    df_update[col + '_x']]
                          [[self.name, col + '_y']])
            updated_index.extend(df_changed.index)
        if updated_index:
            df_update = self.get_right_df(df_update)
            df_update = df_update.loc[updated_index]
            df_update = df_update[[self.name] + set_cols]
            set_vals = [tuple(x) for x in df_update.values]
            set_vals = self.size_check_and_split(set_vals)
            for set_val in set_vals:
                if exc.upload_id_col + '_x' in df.columns:
                    self.db.update_rows_two_where(table, set_cols, set_val,
                                                  self.name, exc.upload_id_col,
                                                  self.dft.upload_id)
                else:
                    self.db.update_rows(table, set_cols, set_val, self.name)

    @staticmethod
    def size_check_and_split(set_vals):
        size = sys.getsizeof(set_vals)
        max_mem = 1048576.0
        lists_needed = size / max_mem
        n = int(math.ceil(len(set_vals) / lists_needed))
        set_vals = [set_vals[i:i + n] for i in range(0, len(set_vals), n)]
        return set_vals

    def delete_rows(self, df, table):
        if exc.upload_id_col + '_x' not in df.columns:
            return None
        df_delete = df[df['_merge'] == 'left_only']
        delete_vals = df_delete[self.name].tolist()
        if delete_vals:
            self.db.delete_rows(table, exc.upload_id_col, self.dft.upload_id,
                                self.name, delete_vals)

    def insert_rows(self, df, table):
        df_insert = df[df['_merge'] == 'right_only']
        df_insert = self.get_right_df(df_insert)
        for fk_table in self.dbs.fk:
            col = self.dbs.fk[fk_table][0]
            if col in df_insert.columns:
                df_insert = self.dft.df_col_to_type(df_insert, col, 'INT')
        if self.id_col in df_insert.columns:
            df_insert = df_insert.drop([self.id_col], axis=1)
        if not df_insert.empty:
            self.db.copy_from(table, df_insert, df_insert.columns)

    def get_right_df(self, df):
        cols = [x for x in df.columns if x[-2:] == '_y']
        df = df[[self.name] + cols]
        df.columns = ([self.name] + [x[:-2] for x in df.columns
                      if x[-2:] == '_y'])
        return df

    def add_ids_to_df(self, id_config, sliced_df):
        for id_table in id_config:
            if id_table == exc.upload_tbl:
                continue
            df_rds = self.format_and_read_rds(id_table, id_config, sliced_df)
            sliced_df = sliced_df.merge(df_rds, how='outer', on=self.name)
            sliced_df = sliced_df.drop(self.name, axis=1)
            sliced_df = self.dft.df_col_to_type(sliced_df, self.id_col, 'INT')
        return sliced_df

    def format_and_read_rds(self, table, id_config, sliced_df):
        self.set_id_info(table, id_config, sliced_df)
        self.dbs.set_table(table)
        if exc.upload_id_col in self.dbs.cols:
            df_rds = self.db.read_rds_two_where(table, self.id_col, self.name,
                                                self.values, exc.upload_id_col,
                                                self.dft.upload_id)
        else:
            df_rds = self.db.read_rds(table, self.id_col, self.name,
                                      self.values)
        return df_rds

    def set_id_info(self, table, id_config, sliced_df):
        self.table = table
        self.id_col = id_config[table][0]
        self.name = id_config[table][1]
        self.values = sliced_df[self.name].tolist()


# noinspection SqlResolve
class DB(object):
    def __init__(self, config=None):
        self.user = None
        self.pw = None
        self.host = None
        self.port = None
        self.db = None
        self.schema = None
        self.config_list = []
        self.configfile = None
        self.engine = None
        self.connection = None
        self.cursor = None
        self.output = None
        self.conn_string = None
        self.config = config
        if self.config:
            self.input_config(self.config)

    def input_config(self, config):
        logging.info('Loading DB config file: ' + str(config))
        self.configfile = config_path + config
        self.load_config()
        self.check_config()
        self.conn_string = ('postgresql://{0}:{1}@{2}:{3}/{4}'.
                            format(*self.config_list))

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(self.configfile + ' not found.  Aborting.')
            sys.exit(0)
        self.user = self.config['USER']
        self.pw = self.config['PASS']
        self.host = self.config['HOST']
        self.port = self.config['PORT']
        self.db = self.config['DATABASE']
        if 'SCHEMA' not in self.config.keys():
            self.schema = self.db
        else:
            self.schema = self.config['SCHEMA']
        self.config_list = [self.user, self.pw, self.host, self.port, self.db,
                            self.schema]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning(item + 'not in DB config file.  Aborting.')
                sys.exit(0)

    def connect(self):
        logging.debug('Connecting to DB at Host: ' + self.host)
        self.engine = sqa.create_engine(self.conn_string,
                                        connect_args={'sslmode': 'prefer'})
        try:
            self.connection = self.engine.raw_connection()
        except AssertionError:
            self.connection = self.engine.raw_connection()
        except sqa.exc.OperationalError:
            logging.warning('Connection timed out. Reconnecting in 30s.')
            time.sleep(30)
            self.connect()
        self.cursor = self.connection.cursor()

    def df_to_output(self, df):
        if sys.version_info[0] == 3:
            self.output = io.StringIO()
        else:
            self.output = io.BytesIO()
        df.to_csv(self.output, sep='\t', header=False, index=False,
                  encoding='utf-8')
        self.output.seek(0)

    def copy_from(self, table, df, columns):
        table_path = self.schema + '.' + table
        self.connect()
        logging.info('Writing ' + str(len(df)) + ' row(s) to ' + table)
        self.df_to_output(df)
        cur = self.connection.cursor()
        cur.copy_from(self.output, table=table_path, columns=columns)
        self.connection.commit()
        cur.close()

    def insert_rds(self, table, columns, values, return_col):
        self.connect()
        command = """
                  INSERT INTO {0}.{1} ({2})
                   VALUES ({3})
                   RETURNING ({4})
                  """.format(self.schema, table, ', '.join(columns),
                             ', '.join(['%s'] * len(values)), return_col)
        self.cursor.execute(command, values)
        self.connection.commit()
        data = self.cursor.fetchall()
        data = pd.DataFrame(data=data, columns=[return_col])
        return data

    def delete_rows(self, table, where_col, where_val,
                    where_col2, where_vals2):
        logging.info('Deleting ' + str(len(where_vals2)) +
                     ' row(s) from ' + table)
        self.connect()
        command = """
                  DELETE FROM {0}.{1}
                   WHERE {0}.{1}.{2} IN ({3}) AND {0}.{1}.{4} IN ({5})
                  """.format(self.schema, table, where_col, where_val,
                             where_col2,
                             ', '.join(['%s'] * len(where_vals2)))
        self.cursor.execute(command, where_vals2)
        self.connection.commit()

    def read_rds_two_where(self, table, select_col, where_col, where_val,
                           where_col2, where_val2):
        self.connect()
        if select_col == where_col:
            command = """
                      SELECT {0}.{1}.{2} FROM {0}.{1}
                       WHERE {0}.{1}.{3} IN ({4}) AND {0}.{1}.{5} IN ({6})
                      """.format(self.schema, table, select_col, where_col,
                                 ', '.join(['%s'] * len(where_val)),
                                 where_col2, where_val2)
        else:
            command = """
                      SELECT {0}.{1}.{2}, {0}.{1}.{3} FROM {0}.{1}
                       WHERE {0}.{1}.{3} IN ({4}) AND {0}.{1}.{5} IN ({6})
                      """.format(self.schema, table, select_col, where_col,
                                 ', '.join(['%s'] * len(where_val)),
                                 where_col2, where_val2)
        self.cursor.execute(command, where_val)
        data = self.cursor.fetchall()
        if select_col == where_col:
            data = pd.DataFrame(data=data, columns=[select_col])
        else:
            data = pd.DataFrame(data=data, columns=[select_col, where_col])
        return data

    def read_rds(self, table, select_col, where_col, where_val):
        self.connect()
        if select_col == where_col:
            command = """
                      SELECT {0}.{1}.{2}
                       FROM {0}.{1}
                       WHERE {0}.{1}.{3} IN ({4})
                      """.format(self.schema, table, select_col, where_col,
                                 ', '.join(['%s'] * len(where_val)))
        else:
            command = """
                      SELECT {0}.{1}.{2}, {0}.{1}.{3}
                       FROM {0}.{1}
                       WHERE {0}.{1}.{3} IN ({4})
                      """.format(self.schema, table, select_col, where_col,
                                 ', '.join(['%s'] * len(where_val)))
        self.cursor.execute(command, where_val)
        data = self.cursor.fetchall()
        if select_col == where_col:
            data = pd.DataFrame(data=data, columns=[select_col])
        else:
            data = pd.DataFrame(data=data, columns=[select_col, where_col])
        return data

    def read_rds_table(self, table, where_col, where_val):
        self.connect()
        command = """
                  SELECT *
                   FROM {0}.{1}
                   WHERE {0}.{1}.{2} IN ({3})
                  """.format(self.schema, table, where_col,
                             ', '.join(['%s'] * len(where_val)))
        self.cursor.execute(command, where_val)
        columns = [i[0] for i in self.cursor.description]
        data = self.cursor.fetchall()
        data = pd.DataFrame(data=data, columns=columns)
        return data

    def update_rows(self, table, set_cols, set_vals, where_col):
        logging.info('Updating ' + str(len(set_vals)) +
                     ' row(s) from ' + table)
        self.connect()
        command = """
                  UPDATE {0}.{1} AS t
                   SET {2}
                   FROM (VALUES {3})
                   AS c({4})
                   WHERE c.{5} = t.{5}
                  """.format(self.schema, table,
                             (', '.join(x + ' = c.' + x
                              for x in [where_col] + set_cols)),
                             ', '.join(['%s'] * len(set_vals)),
                             ', '.join([where_col] + set_cols),
                             where_col)
        self.cursor.execute(command, set_vals)
        self.connection.commit()

    def update_rows_two_where(self, table, set_cols, set_vals, where_col,
                              where_col2, where_val2):
        logging.info('Updating ' + str(len(set_vals)) +
                     ' row(s) from ' + table)
        self.connect()
        command = """
                  UPDATE {0}.{1} AS t
                   SET {2}
                   FROM (VALUES {3})
                   AS c({4})
                   WHERE c.{5} = t.{5}
                   AND t.{6} = {7}
                  """.format(self.schema, table,
                             (', '.join(x + ' = c.' + x
                              for x in [where_col] + set_cols)),
                             ', '.join(['%s'] * len(set_vals)),
                             ', '.join([where_col] + set_cols),
                             where_col, where_col2, where_val2)
        self.cursor.execute(command, set_vals)
        self.connection.commit()

    @staticmethod
    def read_file(filename):
        with open('config/' + str(filename), 'r') as f:
            sqlfile = f.read()
        return sqlfile

    def get_data(self, filename):
        logging.info('Querying ')
        self.connect()
        command = self.read_file(filename)
        self.cursor.execute(command)
        data = self.cursor.fetchall()
        columns = [i[0] for i in self.cursor.description]
        df = pd.DataFrame(data=data, columns=columns)
        return df


class DBSchema(object):
    def __init__(self, config_file):
        self.config_file = config_file
        self.full_config_file = config_path + self.config_file
        self.table_list = None
        self.config = None
        self.pk = None
        self.cols = None
        self.fk = None
        self.load_config(self.full_config_file)

    def load_config(self, config_file):
        df = pd.read_csv(config_file)
        self.table_list = df[exc.table].tolist()
        self.config = df.set_index(exc.table).to_dict()
        for col in exc.split_columns:
            self.config[col] = {key: list(str(value).split(',')) for
                                key, value in self.config[col].items()}
        for table in self.table_list:
            for col in exc.dirty_columns:
                clean_dict = self.clean_table_item(table, col,
                                                   exc.dirty_columns[col])
                self.config[col][table] = clean_dict

    def clean_table_item(self, table, config_col, split_char):
        clean_dict = {}
        for item in self.config[config_col][table]:
            if item == str('nan'):
                continue
            cln_item = item.strip().split(split_char)
            if len(cln_item) == 3:
                clean_dict.update({cln_item[0]: [cln_item[1], cln_item[2]]})
            elif len(cln_item) == 2:
                clean_dict.update({cln_item[0]: cln_item[1]})
        return clean_dict

    def set_table(self, table):
        self.pk = self.config[exc.pk][table]
        self.cols = self.config[exc.columns][table]
        self.fk = self.config[exc.fk][table]

    def get_cols_for_export(self, table):
        self.set_table(table)
        fk_list = [self.fk[x][1] for x in self.fk]
        cols_list = self.cols.keys()
        cols_list = [x for x in cols_list if x not in fk_list]
        return fk_list + cols_list


class DFTranslation(object):
    def __init__(self, config_file, data_file, db=None):
        self.config_file = config_file
        self.full_config_file = config_path + self.config_file
        self.data_file = data_file
        self.db = db
        self.translation = None
        self.db_columns = None
        self.df_columns = None
        self.df = None
        self.sliced_df = None
        self.type_columns = None
        self.translation = None
        self.translation_type = None
        self.text_columns = None
        self.date_columns = None
        self.int_columns = None
        self.real_columns = None
        self.upload_id = None
        self.load_translation(self.full_config_file)
        self.load_df(self.data_file)

    def load_translation(self, config_file):
        df = pd.read_csv(config_file)
        self.db_columns = df[exc.translation_db].tolist()
        self.df_columns = df[exc.translation_df].tolist()
        self.type_columns = df[exc.translation_type].tolist()
        self.translation = dict(zip(df[exc.translation_df],
                                    df[exc.translation_db]))
        self.translation_type = dict(zip(df[exc.translation_db],
                                         df[exc.translation_type]))
        self.text_columns = [k for k, v in self.translation_type.items()
                             if v == 'TEXT']
        self.date_columns = [k for k, v in self.translation_type.items()
                             if v == 'DATE']
        self.int_columns = [k for k, v in self.translation_type.items()
                            if v == 'INT' or v == 'BIGINT'
                            or v == 'BIGSERIAL']
        self.real_columns = [k for k, v in self.translation_type.items()
                             if v == 'REAL' or v == 'DECIMAL']

    def load_df(self, datafile):
        self.df = utl.import_read_csv(datafile)
        if self.df.empty:
            logging.warning('Dataframe empty, stopping upload.')
            return False
        self.df_columns = [x for x in self.df_columns
                           if x in list(self.df.columns)]
        self.df = self.df[self.df_columns]
        self.df = self.df.rename(columns=self.translation)
        self.df = self.clean_types_for_upload(self.df)
        if self.db:
            self.get_upload_id()
        self.add_event_name()
        self.df = self.df.groupby(self.text_columns + self.date_columns +
                                  self.int_columns).sum().reset_index()
        real_columns = [x for x in self.real_columns if x in self.df.columns]
        if real_columns:
            self.df = self.df[self.df[real_columns].sum(axis=1) != 0]
        self.df = self.df.reset_index(drop=True)
        replace_dict = {'"': '', "\\\\": '/', '\r': '', '\n': '', '\t': ''}
        self.df.replace(replace_dict, regex=True, inplace=True)
        return True

    def get_upload_id(self):
        for col in exc.upload_name_param:
            if col not in self.df:
                return False
        self.add_upload_cols()
        ul_id_file_path = config_path + exc.upload_id_file
        if not os.path.isfile(ul_id_file_path):
            ul_id_df = pd.DataFrame(columns=[exc.upload_id_col])
            ul_id_df.to_csv(ul_id_file_path, index=False, encoding='utf-8')
        ul_id_df = pd.read_csv(ul_id_file_path)
        if ul_id_df.empty:
            ul_id_df = self.new_upload()
            ul_id_df.to_csv(ul_id_file_path, index=False, encoding='utf-8')
        self.upload_id = ul_id_df[exc.upload_id_col][0].astype(int)
        self.df[exc.upload_id_col] = int(self.upload_id)
        self.check_db_for_upload()

    def new_upload(self):
        upload_df = self.slice_for_upload(exc.upload_cols)
        ul_id_df = self.db.insert_rds(exc.upload_tbl, exc.upload_cols,
                                      upload_df.values[0], exc.upload_id_col)
        return ul_id_df

    def check_db_for_upload(self):
        df = self.db.read_rds(exc.upload_tbl, exc.upload_id_col,
                              exc.upload_id_col, [int(self.upload_id)])
        if df.empty:
            full_upload_cols = exc.upload_cols + [exc.upload_id_col]
            upload_df = self.slice_for_upload(full_upload_cols)
            self.db.insert_rds(exc.upload_tbl, full_upload_cols,
                               upload_df.values[0], exc.upload_id_col)

    def add_upload_cols(self):
        self.df[exc.upload_last_upload_date] = dt.datetime.today()
        self.df[exc.upload_data_ed] = self.df[exc.event_date].dropna().max()
        self.df[exc.upload_data_sd] = self.df[exc.event_date].dropna().min()
        self.add_upload_name()

    def add_upload_name(self):
        upload_name_items = [x for param in exc.upload_name_param for x in
                             self.df[param].drop_duplicates()]
        upload_name = '_'.join(str(x) for x in upload_name_items)
        self.df[exc.upload_name] = upload_name

    def add_event_name(self):
        if exc.full_placement_name not in self.df:
            return False
        self.df[exc.full_placement_name] = (self.df[exc.full_placement_name] +
                                            str(self.upload_id))
        self.df[exc.event_name] = (self.df[exc.event_date].astype('U') +
                                   self.df[exc.full_placement_name])
        for col in [exc.plan_name, exc.event_steam_name, exc.event_conv_name,
                    exc.event_plan_name]:
            self.df[col] = self.df[exc.event_name]

    def slice_for_upload(self, columns):
        exp_cols = [x for x in columns if x in list(self.df.columns)]
        sliced_df = self.df[exp_cols].drop_duplicates()
        sliced_df = self.group_for_upload(sliced_df, exp_cols)
        sliced_df = self.clean_types_for_upload(sliced_df)
        sliced_df = self.remove_zero_rows(sliced_df, original_cols=columns)
        return sliced_df

    def group_for_upload(self, df, exp_cols):
        if any(x in exp_cols for x in self.real_columns):
            group_cols = [x for x in exp_cols if x in
                          (self.text_columns + self.date_columns +
                           self.int_columns)]
            if group_cols:
                df = df.groupby(group_cols).sum().reset_index()
        return df

    def clean_types_for_upload(self, df):
        for col in df.columns:
            if col not in self.translation_type.keys():
                continue
            data_type = self.translation_type[col]
            df = self.df_col_to_type(df, col, data_type)
        return df

    @staticmethod
    def df_col_to_type(df, col, data_type):
        if data_type == 'TEXT':
            df[col] = df[col].replace(np.nan, 'None')
            df[col] = df[col].astype('U')
        if data_type == 'REAL':
            df[col] = df[col].replace(np.nan, 0)
            df[col] = df[col].astype(float)
        if data_type == 'DATE':
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].replace(pd.NaT, None)
            df[col] = df[col].replace(pd.NaT, dt.datetime.today())
        if data_type == 'INT':
            df[col] = df[col].replace(np.nan, 0)
            df[col] = df[col].astype(np.int64)
        return df

    def remove_zero_rows(self, df, original_cols):
        real_cols = [x for x in self.real_columns if x in df.columns]
        ori_real_cols = [x for x in self.real_columns if x in original_cols]
        if not real_cols:
            if not ori_real_cols:
                return df
            else:
                for col in ori_real_cols:
                    df[col] = 0
        df['realcolsum'] = df[real_cols].sum(axis=1)
        df = df[df['realcolsum'] != 0]
        df = df.drop(['realcolsum'], axis=1)
        return df


class ScriptBuilder(object):
    def __init__(self, metadata=mdl.Base.metadata, omit_tables=None):
        self.omit_tables = omit_tables
        if not self.omit_tables:
            self.omit_tables = ['model']
        self.metadata = metadata
        self.tables = [x for x in self.metadata.sorted_tables
                       if x.name not in self.omit_tables]
        self.completed_tables = self.omit_tables
        self.dimensions, self.metrics = self.get_all_columns()

    def get_all_columns(self):
        import decimal
        dimensions, metrics = [], []
        for table in self.tables:
            for col in table.columns:
                if not col.foreign_keys and not col.primary_key:
                    if col.type.python_type == decimal.Decimal:
                        metrics.append(col.name)
                    else:
                        dimensions.append(col.name)
        dimensions = set(dimensions)
        metrics = set(metrics)
        return dimensions, metrics

    def append_plan_join(self, original_from_script):
        table = [x for x in self.tables if x.name == 'plan'][0]
        fcs = [x for x in table.columns if x.foreign_keys]
        table_name = '"{}"."{}"'.format(table.schema, table.name)
        fc = fcs[0]
        fk = [x for x in fc.foreign_keys][0]
        join_table = '"{}"."{}"'.format(
            fk.column.table.schema, fk.column.table.name)
        table_relation = '{}."{}"'.format(table_name, fc.name)
        foreign_relation = '{}."{}"'.format(join_table, fk.column.name)
        join_script = """\nFULL JOIN {} ON ({} = {})""".format(
            table_name, foreign_relation, table_relation)
        from_script = original_from_script + join_script
        return from_script

    def get_from_script(self, table, original_from_script=''):
        fcs = [x for x in table.columns if x.foreign_keys]
        full_join_script = []
        table_name = '"{}"."{}"'.format(table.schema, table.name)
        for fc in fcs:
            fk = [x for x in fc.foreign_keys][0]
            if fk.column.table.name in self.completed_tables:
                continue
            join_table = '"{}"."{}"'.format(
                fk.column.table.schema, fk.column.table.name)
            table_relation = '{}."{}"'.format(table_name, fc.name)
            foreign_relation = '{}."{}"'.format(join_table, fk.column.name)
            if fc.table.name == 'event' and fc.name == 'fullplacementid':
                join_type = 'FULL'
            else:
                join_type = 'LEFT'
            join_script = """\n{} JOIN {} ON ({} = {})""".format(
                join_type, join_table, table_relation, foreign_relation)
            full_join_script.append(join_script)
            self.completed_tables.append(fk.column.table.name)
        if not original_from_script:
            from_script = """FROM {}""".format(table_name)
        else:
            from_script = ''
        if full_join_script:
            from_script = """{} {}""".format(
                from_script, ' '.join(full_join_script))
        if from_script:
            from_script = original_from_script + from_script
        else:
            from_script = original_from_script
        for fc in fcs:
            fk = [x for x in fc.foreign_keys][0]
            table = fk.column.table
            from_script = self.get_from_script(table, from_script)
        return from_script

    def get_column_names(self, base_table):
        import decimal
        column_names = []
        tables = [base_table] + [x for x in self.tables
                                 if x.name in self.completed_tables]
        for table in tables:
            columns = table.columns
            names = ['"{}"."{}"."{}"'.format(table.schema, table.name, x.name)
                     for x in columns
                     if not x.foreign_keys and not x.primary_key and
                     x.type.python_type != decimal.Decimal]
            column_names.extend(names)
        sum_columns = ['SUM("{}"."{}"."{}") AS "{}"'.format(
            base_table.schema, base_table.name, x.name, x.name)
                 for x in base_table.columns
                 if x.type.python_type == decimal.Decimal]
        sum_columns.append('SUM("{}"."{}"."{}") AS "{}"'.format(
            'lqadb', 'plan',
            'plannednetcost', 'plannednetcost'))
        return column_names, sum_columns

    @staticmethod
    def optimize_from_script(filter_table, from_script):
        split_from_script = from_script.split('\n')
        join = [x for x in split_from_script if '"{}"'.format(filter_table)
                in x.split('ON')[0]]
        split_from_script = [x for x in split_from_script if x not in join]
        parent_table = join[0].split('ON')[1].split('=')[0].split('.')[1]
        parent_idx = [idx for idx, x in enumerate(split_from_script)
                      if '{}'.format(parent_table) in x.split('ON')[0]][0]
        split_from_script = (split_from_script[:parent_idx + 1] + join +
                             split_from_script[parent_idx + 1:])
        from_script = '\n'.join(split_from_script)
        return from_script

    def get_full_script(self, filter_col, filter_val, filter_table):
        base_table = [x for x in self.tables if x.name == 'event'][0]
        from_script = self.get_from_script(table=base_table)
        from_script = self.append_plan_join(from_script)
        from_script = self.optimize_from_script(filter_table, from_script)
        column_names, sum_columns = self.get_column_names(base_table)
        column_names = ['{}'.format(x) for x in set(column_names)]
        where_clause = "({} = {}::text)".format(filter_col, '%s')
        sel_script = \
            """SELECT {},\n{} \n{} \nWHERE {} \nGROUP BY {}""".format(
                ','.join(column_names), ','.join(sum_columns),
                from_script, where_clause, ','.join(column_names))
        return sel_script

    def get_view_script(self, filter_col, filter_val, filter_table, view_name):
        sel_script = self.get_full_script(filter_col, filter_val, filter_table)
        view_script = 'CREATE OR REPLACE VIEW lqadb.{} AS \n {}'.format(
            view_name, sel_script)
        return view_script
