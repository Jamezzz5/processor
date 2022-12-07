import re
import io
import sys
import boto
import json
import logging
import boto3
import gzip
from io import BytesIO, StringIO
import pandas as pd
import datetime as dt
from boto.s3.connection import S3Connection
import reporting.utils as utl

config_path = utl.config_path


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
        logging.info('Loading S3 config file: {}'.format(config))
        self.config_file = config_path + config
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
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
                logging.warning('{} not in config file. Aborting.'.format(item))
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
        data = StringIO(s3_string)
        data = gzip.GzipFile(fileobj=data).read()
        return data

    def get_file_as_df(self, key):
        logging.info('Downloading: ' + str(key.name))
        data = self.get_file_as_str(key)
        r = BytesIO(data)
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

    def get_client(self):
        client = boto3.client(service_name='s3',
                              use_ssl=True,
                              aws_access_key_id=self.access_key,
                              aws_secret_access_key=self.access_secret)
        return client

    def s3_write_file(self, df, file_name='raw', default_format=True):
        csv_file = '{}{}'.format(file_name, '.csv')
        if default_format:
            gzip_extension = '.gzip'
        else:
            gzip_extension = '.gz'
        zip_file = '{}{}'.format(file_name, gzip_extension)
        today_yr = dt.datetime.strftime(dt.datetime.today(), '%Y')
        today_str = dt.datetime.strftime(dt.datetime.today(), '%m%d')
        today_folder_name = '{}/{}/'.format(today_yr, today_str)
        product_name = '{}_{}'.format(df['uploadid'].unique()[0],
                                      '_'.join(df['productname'].unique()))
        product_name = re.sub(r'\W+', '', product_name)
        zip_file = '{}/{}{}/{}'.format(
            self.prefix, today_folder_name, product_name, zip_file)
        buffer = io.BytesIO()
        with gzip.GzipFile(filename=csv_file, fileobj=buffer, mode="wb") as f:
            f.write(df.to_csv().encode())
        buffer.seek(0)
        self.s3_upload_file_obj(buffer, zip_file)

    def s3_upload_file_obj(self, file_object, key):
        client = self.get_client()
        client.upload_fileobj(Fileobj=file_object, Bucket=self.bucket, Key=key,
                              ExtraArgs={'ACL': 'bucket-owner-full-control'})
        logging.info('File successfully uploaded as: {}'.format(key))
        object_url = 'https://{}.s3.amazonaws.com/{}'.format(self.bucket, key)
        return object_url
