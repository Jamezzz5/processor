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
configpath = 'Config/'


class DB(object):
    def __init__(self, datafile, config):
        self.config = config
        self.df = pd.read_csv(datafile, encoding='iso-8859-1')
        self.df = self.clean_for_export(self.df)
        self.inputconfig(self.config)
        self.connstring = ('postgresql://{0}:{1}@{2}:{3}/{4}'.
                           format(*self.configlist))

    def inputconfig(self, config):
        logging.info('Loading DB config file: ' + str(config))
        self.configfile = configpath + config
        self.loadconfig()
        self.checkconfig()

    def loadconfig(self):
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
        self.table = self.config['TABLE']
        self.configlist = [self.user, self.pw, self.host, self.port,
                           self.table]

    def checkconfig(self):
        for item in self.configlist:
            if item == '':
                logging.warn(item + 'not in DB config file.  Aborting.')
                sys.exit(0)

    def connect(self):
        logging.info('Connecting to DB at Host: ' + self.host)
        self.engine = create_engine(self.connstring)
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
                  );'''.format(self.table, exc.db_columns)
            self.cursor.execute(command)
            self.connection.commit()
        logging.info('Writing to RDS')
        output = cStringIO.StringIO()
        self.df.to_csv(output, sep='\t', header=False, index=False,
                       encoding='utf-8')
        output.seek(0)
        contents = output.getvalue()
        cur = self.connection.cursor()
        cur.copy_from(output, self.table, null="")
        self.connection.commit()
        cur.close()
        logging.info('Successfully wrote to RDS')

    def read_from_rds(self):
        logging.info('Reading from RDS')
        self.df_rds = pd.read_sql_table(self.table, self.engine)
        logging.info('Successfully read from RDS')

    def write_from_rds(self, filename):
        self.read_from_rds()
        logging.info('Writing from RDS')
        self.df_rds.to_csv(filename)
        logging.info('Successfully wrote from RDS')

    def clean_for_export(self, df):
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
                  """.format(self.table)
        self.cursor.execute(command)
        if self.cursor.fetchone():
            return True
        else:
            return False
