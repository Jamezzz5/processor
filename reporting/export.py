import logging
import json
import sys
import cStringIO
import pandas as pd
from sqlalchemy import create_engine
import expcolumns as exc

log = logging.getLogger()
config_path = 'Config/'


class DBUpload(object):
    def __init__(self):
        self.db = None
        self.dbs = None
        self.dft = None
        self.table = None
        self.id_col = None
        self.name = None
        self.values = None
        self.parent_table = None
        self.parent_id = None
        self.parent_name = None
        self.parent_values = None

    def upload_to_db(self, db_file, schema_file, translation_file, data_file):
        self.db = DB(db_file)
        logging.info('Uploading ' + data_file + ' to ' + self.db.db)
        self.dbs = DBSchema(schema_file)
        self.dft = DFTranslation(translation_file, data_file)
        for table in self.dbs.table_list:
            self.upload_table_to_db(table)
        logging.info(data_file + ' successfully upload to ' + self.db.db)

    def upload_table_to_db(self, table):
        logging.info('Uploading table ' + table + ' to ' + self.db.db)
        cols = self.dbs.get_cols_for_export(table)
        ul_df = self.dft.slice_for_upload(cols)
        ul_df = self.set_parent(ul_df)
        ul_df = self.add_ids_to_df(self.dbs.fk, ul_df)
        pk_config = {table: self.dbs.pk.keys() + self.dbs.pk.values()}
        df_rds = self.format_and_read_rds(table, pk_config, ul_df)
        db_values = self.values_for_db(df_rds, ul_df, self.id_col, self.name)
        if not db_values.empty:
            self.db.copy_from(db_values, self.db, table, ul_df.columns)

    def set_parent(self, ul_df):
        if not self.dbs.parent:
            return ul_df
        ul_df = self.add_ids_to_df(self.dbs.parent, ul_df)
        self.parent_table = self.table
        self.parent_id = self.id_col
        self.parent_name = self.name
        self.parent_values = self.values
        return ul_df

    def add_ids_to_df(self, id_config, sliced_df):
        for table in id_config:
            df_rds = self.format_and_read_rds(table, id_config, sliced_df)
            sliced_df = sliced_df.merge(df_rds, how='outer', on=self.name)
            sliced_df = sliced_df.drop(self.name, axis=1)
        return sliced_df

    def format_and_read_rds(self, table, id_config, sliced_df):
        self.set_id_info(table, id_config, sliced_df)
        df_rds = self.db.read_from_rds(table, self.id_col, self.name,
                                       self.values, self.parent_table,
                                       self.parent_id, self.parent_values)
        return df_rds

    def set_id_info(self, table, id_config, sliced_df):
        self.table = table
        self.id_col = id_config[table][0]
        self.name = id_config[table][1]
        print self.table, self.id_col, self.name
        print sliced_df
        self.values = sliced_df[self.name].tolist()

    @staticmethod
    def values_for_db(df_db, df_raw, id_col, name_col):
        df = df_raw.merge(df_db, how='outer', on=name_col)
        df = df[pd.isnull(df[id_col])]
        df = df.drop(id_col, axis=1)
        return df


class DB(object):
    def __init__(self, config):
        self.user = None
        self.pw = None
        self.host = None
        self.port = None
        self.db = None
        self.config_list = []
        self.configfile = None
        self.engine = None
        self.connection = None
        self.cursor = None
        self.output = None
        self.config = config
        self.input_config(self.config)
        self.conn_string = ('postgresql://{0}:{1}@{2}:{3}/{4}'.
                            format(*self.config_list))

    def input_config(self, config):
        logging.info('Loading DB config file: ' + str(config))
        self.configfile = config_path + config
        self.load_config()
        self.check_config()

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
        self.config_list = [self.user, self.pw, self.host, self.port, self.db]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warn(item + 'not in DB config file.  Aborting.')
                sys.exit(0)

    def connect(self):
        logging.debug('Connecting to DB at Host: ' + self.host)
        self.engine = create_engine(self.conn_string)
        self.connection = self.engine.raw_connection()
        self.cursor = self.connection.cursor()

    def df_to_output(self, df):
        self.output = cStringIO.StringIO()
        df.to_csv(self.output, sep='\t', header=False, index=False,
                  encoding='utf-8')
        self.output.seek(0)

    def copy_from(self, df, db, table, columns):
        table_path = db + '.' + table
        self.connect()
        logging.info('Writing to RDS')
        self.df_to_output(df)
        cur = self.connection.cursor()
        cur.copy_from(self.output, table=table_path, columns=columns)
        self.connection.commit()
        cur.close()
        logging.info('Successfully wrote to RDS')

    def read_from_rds(self, table, select_col, where_col, where_val,
                      parent_table, parent_id, parent_value):
        self.connect()
        if parent_table is None:
            command = """
                      SELECT {0}.{1}.{2}, {0}.{1}.{3}
                       FROM {0}.{1}
                       WHERE {0}.{1}.{3} IN ({4})
                      """.format(self.db, table, select_col, where_col,
                                 ', '.join(['%s'] * len(where_val)))
            self.cursor.execute(command, where_val)
        else:
            command = """
                      SELECT {0}.{1}.{2}, {0}.{1}.{3}
                       FROM {0}.{1}, {0}.{5}
                       WHERE {0}.{1}.{3} IN ({4})
                        AND {0}.{5}.{6} IN ({7})
                      """.format(self.db, table, select_col, where_col,
                                 ', '.join(['%s'] * len(where_val)),
                                 parent_table, parent_id,
                                 ', '.join(['%s'] * len(parent_value)))
            self.cursor.execute(command, where_val, parent_value)
        data = self.cursor.fetchall()
        data = pd.DataFrame(data=data, columns=[select_col, where_col])
        return data


class DBSchema(object):
    def __init__(self, config_file):
        self.config_file = config_file
        self.full_config_file = config_path + self.config_file
        self.table_list = None
        self.config = None
        self.pk = None
        self.cols = None
        self.fk = None
        self.parent = None
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
        self.parent = self.config[exc.parent][table]

    def get_cols_for_export(self, table):
        self.set_table(table)
        parent_list = [self.parent[x][1] for x in self.parent]
        fk_list = [self.fk[x][1] for x in self.fk]
        fk_list = [x for x in fk_list if x not in parent_list]
        cols_list = self.cols.keys()
        return parent_list + fk_list + cols_list


class DFTranslation(object):
    def __init__(self, config_file, data_file):
        self.config_file = config_file
        self.full_config_file = config_path + self.config_file
        self.data_file = data_file
        self.translation = None
        self.db_columns = None
        self.df_columns = None
        self.df = None
        self.sliced_df = None
        self.load_translation(self.full_config_file)
        self.load_df(self.data_file)

    def load_translation(self, config_file):
        df = pd.read_csv(config_file)
        self.db_columns = df[exc.translation_db].tolist()
        self.df_columns = df[exc.translation_df].tolist()
        self.translation = dict(zip(df[exc.translation_df],
                                    df[exc.translation_db]))

    def load_df(self, datafile):
        self.df = pd.read_csv(datafile, encoding='iso-8859-1')
        self.df_columns = [x for x in self.df_columns
                           if x in list(self.df.columns)]
        self.df = self.df[self.df_columns]
        self.df = self.df.rename(columns=self.translation)

    def slice_for_upload(self, columns):
        exp_cols = [x for x in columns if x in list(self.df.columns)]
        sliced_df = self.df[exp_cols].drop_duplicates()
        return sliced_df
