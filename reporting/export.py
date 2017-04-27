import logging
import json
import sys
import cStringIO
import pandas as pd
from sqlalchemy import create_engine
import vmcolumns as vmc
import dictcolumns as dctc
import expcolumns as exc

log = logging.getLogger()
config_path = 'Config/'


class DB(object):
    def __init__(self, datafile, config):
        self.user = None
        self.pw = None
        self.host = None
        self.port = None
        self.database = None
        self.config_list = []
        self.configfile = None
        self.engine = None
        self.connection = None
        self.cursor = None
        self.config = config
        self.df_rds = pd.DataFrame()
        self.df = pd.read_csv(datafile, encoding='iso-8859-1')
        self.df = self.clean_for_export(self.df)
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
        self.database = self.config['DATABASE']
        self.config_list = [self.user, self.pw, self.host, self.port,
                            self.database]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warn(item + 'not in DB config file.  Aborting.')
                sys.exit(0)

    def connect(self):
        logging.info('Connecting to DB at Host: ' + self.host)
        self.engine = create_engine(self.conn_string)
        self.connection = self.engine.raw_connection()
        self.cursor = self.connection.cursor()

    def export_to_rds(self):
        self.connect()
        if self.db_table_check() is False:
            logging.info('CREATING DB Table')
            command = '''
                  DROP TABLE IF EXISTS {0};
                  CREATE TABLE {0}
                  (
                  {1}
                  );'''.format(self.database, exc.db_columns)
            self.cursor.execute(command)
            self.connection.commit()
        logging.info('Writing to RDS')
        output = cStringIO.StringIO()
        self.df.to_csv(output, sep='\t', header=False, index=False,
                       encoding='utf-8')
        output.seek(0)
        output.getvalue()
        cur = self.connection.cursor()
        cur.copy_from(output, self.database, null="")
        self.connection.commit()
        cur.close()
        logging.info('Successfully wrote to RDS')

    def read_from_rds(self):
        logging.info('Reading from RDS')
        self.df_rds = pd.read_sql_table(self.database, self.engine)
        logging.info('Successfully read from RDS')

    def write_from_rds(self, filename):
        self.read_from_rds()
        logging.info('Writing from RDS')
        self.df_rds.to_csv(filename)
        logging.info('Successfully wrote from RDS')

    @staticmethod
    def clean_for_export(df):
        df = (df.loc[:, lambda df: vmc.datacol + dctc.COLS +
              ['Planned Net Cost', 'Net Cost Final']])
        df.rename(columns=lambda x: x.replace(' ', ''), inplace=True)
        df.rename(columns=lambda x: x.replace('-', '_'), inplace=True)
        df.rename(columns=lambda x: x.replace('/', '_'), inplace=True)
        return df

    def db_table_check(self):
        command = """
                  SELECT EXISTS(
                  SELECT * FROM information_schema.tables
                   WHERE table_name = '{0}')
                  """.format(self.database)
        self.cursor.execute(command)
        if self.cursor.fetchone():
            return True
        else:
            return False


def get_table_fact(df):
    tdf = df[[dctc.FPN, dctc.PNC] + vmc.datacol]
    tdf = pd.melt(tdf, [dctc.FPN, vmc.date], var_name='EventName',
                  value_name='EventValue')
    tdf = tdf.dropna()
    return tdf


def get_table_vendor(df):
    tdf = df[dctc.vendor_cols]
    return tdf
