import sys
import json
import logging
import boto
import gzip
import cStringIO
from StringIO import StringIO
import pandas as pd
import datetime as dt
from boto.s3.connection import S3Connection

config_path = 'Config/'


class S3(object):
    def __init__(self):
        self.config_file = None
        self.bucket = None
        self.prefix = None
        self.access_key = None
        self.access_secret = None
        self.config = None
        self.config_list = None
        self.conn = None
        self.bucket = None
        self.key_list = None
        self.df = pd.DataFrame()

    def input_config(self, config):
        logging.info('Loading S3 config file: ' + str(config))
        self.config_file = config_path + config
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(self.config_file + ' not found.  Aborting.')
            sys.exit(0)
        self.bucket = self.config['bucket']
        self.prefix = self.config['prefix']
        self.access_key = self.config['access_key']
        self.access_secret = self.config['access_secret']
        self.config_list = [self.bucket, self.prefix, self.access_key,
                            self.access_secret]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warn(item + 'not in S3 config file.  Aborting.')
                sys.exit(0)

    @staticmethod
    def get_data_default_check(sd, ed):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None:
            ed = dt.datetime.today() - dt.timedelta(days=1)
        return sd, ed

    def get_all_keys_in_bucket(self):
        self.conn = S3Connection(self.access_key, self.access_secret,
                                 calling_format=(
                                  boto.s3.connection.OrdinaryCallingFormat()))
        self.bucket = self.conn.get_bucket(self.bucket, validate=False)
        self.key_list = self.bucket.list(prefix=self.prefix)

    @staticmethod
    def get_file_as_str(key):
        s3_string = key.get_contents_as_string()
        data = cStringIO.StringIO(s3_string)
        data = gzip.GzipFile(fileobj=data).read()
        return data

    def get_file_as_df(self, key):
        logging.info('Downloading: ' + str(key.name))
        data = self.get_file_as_str(key)
        r = StringIO(data)
        df = pd.read_csv(r)
        return df

    def get_data(self, sd, ed):
        self.get_all_keys_in_bucket()
        sd, ed = self.get_data_default_check(sd, ed)
        for key in self.key_list:
            key_date = boto.utils.parse_ts(key.last_modified)
            if ed <= key_date >= sd:
                tdf = self.get_file_as_df(key)
                self.df = self.df.append(tdf)
        return self.df
