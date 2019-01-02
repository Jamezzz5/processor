import os
import sys
import json
import time
import ftplib
import logging
import pandas as pd
import datetime as dt
from io import BytesIO
import reporting.utils as utl

config_path = utl.config_path


class FTP(object):
    def __init__(self):
        self.df = pd.DataFrame()
        self.files = []
        self.ftp = None
        self.config_file = None
        self.config = None
        self.ftp_host = None
        self.ftp_path = None
        self.ftp_file = None
        self.username = None
        self.password = None
        self.header = None
        self.config_list = []

    def input_config(self, config):
        logging.info('Loading FTP config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.ftp_host = self.config['FTP']
        self.ftp_path = self.config['FTP_PATH']
        self.ftp_file = self.config['FTP_FILE']
        self.username = self.config['USERNAME']
        self.password = self.config['PASSWORD']
        self.config_list = [self.ftp, self.ftp_path, self.ftp_file,
                            self.username, self.password]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in FTP config file.  '
                                 'Aborting.'.format(item))
                sys.exit(0)

    def ftp_init(self):
        self.ftp = ftplib.FTP_TLS(self.ftp_host)
        try:
            self.ftp.sendcmd('USER {}'.format(self.username))
            self.ftp.sendcmd('PASS {}'.format(self.password))
        except ftplib.all_errors:
            self.ftp = ftplib.FTP_TLS(self.ftp_host, user=self.username,
                                      passwd=self.password)
            self.ftp.prot_p()
        self.ftp.cwd(self.ftp_path)

    def get_data(self):
        logging.info('Getting FTP data from {}'.format(self.ftp_host))
        self.ftp_init()
        newest_file = self.ftp_newest_file()
        self.ftp_remove_files(newest_file)
        if newest_file == '.' or newest_file == '..':
            logging.warning('No files found, returning empty Dataframe.')
            return self.df
        r = self.ftp_read_file(newest_file)
        self.ftp.quit()
        self.string_to_df(newest_file, r)
        return self.df

    def ftp_newest_file(self):
        file_dict = {}
        self.files = []
        try:
            self.ftp.dir(self.files.append)
        except ftplib.all_errors as e:
            self.ftp_force_close_error(e)
            self.ftp_newest_file()
        for item in self.files:
            file_info = item.split()
            file_date = ' '.join(item.split()[5:8])
            file_date = '{0} {1}'.format(file_date,
                                         str(dt.datetime.today().year))
            file_date = dt.datetime.strptime(file_date, '%b %d %H:%M %Y')
            if file_date.date() > dt.date.today():
                file_date = file_date.replace(year=file_date.year - 1)
            if file_info[8] != '.' and file_info[8] != '..':
                item_dict = {file_date: file_info[8]}
                file_dict.update(item_dict)
        date_list = list([key for key, value in file_dict.items()])
        newest_file = file_dict[max(date_list)]
        return newest_file

    def ftp_write_file(self, df, write_file):
        logging.info('Writing {} to {}.'.format(write_file, self.ftp_host))
        self.ftp_init()
        r = BytesIO()
        df.to_csv(r, index=False, encoding='utf-8')
        r.seek(0)
        self.ftp.storbinary('STOR {}'.format(write_file), r)
        self.ftp.quit()

    def ftp_read_file(self, read_file):
        r = BytesIO()
        try:
            self.ftp.retrbinary('RETR {}'.format(read_file), r.write)
        except ftplib.all_errors as e:
            self.ftp_force_close_error(e)
            r = self.ftp_read_file(read_file)
        return r

    def ftp_force_close_error(self, e):
        logging.warning('Connection to the FTP was closed due to below '
                        'error, retrying in 30 seconds. {}'.format(e))
        time.sleep(30)
        self.ftp_init()

    def string_to_df(self, read_file, r):
        r.seek(0)
        if read_file[-4:] == '.zip':
            comp = 'zip'
        else:
            comp = 'infer'
        try:
            self.df = pd.read_csv(r, compression=comp)
        except pd.errors.ParserError:
            r.seek(0)
            self.df = pd.read_csv(r, header=self.header, compression=comp)
            self.df = utl.add_dummy_header(self.df, self.header)

    def ftp_remove_files(self, newest_file):
        for item in self.files:
            file_info = item.split()[8]
            if file_info != newest_file:
                try:
                    self.ftp.delete('{}/{}'.format(self.ftp_path, file_info))
                except ftplib.error_perm:
                    continue
