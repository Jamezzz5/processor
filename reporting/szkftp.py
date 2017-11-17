import ftplib
import logging
import json
import sys
import time
from io import BytesIO
import pandas as pd

config_path = 'Config/'


class SzkFtp(object):
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
        logging.info('Loading Sizmek config file: ' + config)
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
                logging.warning(item + 'not in Sizmek config file.  Aborting.')
                sys.exit(0)

    def ftp_init(self):
        self.ftp = ftplib.FTP_TLS(self.ftp_host)
        self.ftp.sendcmd('USER {}'.format(self.username))
        self.ftp.sendcmd('PASS {}'.format(self.password))
        self.ftp.cwd(self.ftp_path)

    def get_data(self):
        logging.info('Getting Sizmek data from ' + str(self.ftp))
        self.ftp_init()
        newest_file = self.ftp_newest_file()
        self.ftp_remove_files(newest_file)
        if newest_file == '.' or newest_file == '..':
            logging.warning('No files found, returning empty Dataframe.')
            return self.df
        r = self.ftp_read_file(newest_file)
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
            if file_info[8] != '.' and file_info[8] != '..':
                item_dict = {time.strptime(file_date, '%b %d %H:%M'):
                             file_info[8]}
                file_dict.update(item_dict)
        date_list = list([key for key, value in file_dict.items()])
        newest_file = file_dict[max(date_list)]
        return newest_file

    def ftp_read_file(self, read_file):
        r = BytesIO()
        try:
            self.ftp.retrbinary('RETR ' + read_file, r.write)
        except ftplib.all_errors as e:
            self.ftp_force_close_error(e)
            self.ftp_read_file(read_file)
        try:
            self.ftp.quit()
        except AttributeError as e:
            logging.warning('FTP could not quit due to the below ' +
                            'error, continuing. ' + str(e))
        r.seek(0)
        return r

    def ftp_force_close_error(self, e):
        logging.warning('Connection to the FTP was closed due to below ' +
                        'error, retrying in 30 seconds. ' + str(e))
        time.sleep(30)
        self.ftp_init()

    def string_to_df(self, read_file, r):
        if read_file[-4:] == '.zip':
            try:
                self.df = pd.read_csv(r, compression='zip')
            except pd.errors.ParserError:
                r.seek(0)
                self.df = pd.read_csv(r, header=self.header, compression='zip')
                self.add_dummy_header()
        else:
            try:
                self.df = pd.read_csv(r)
            except pd.errors.ParserError:
                r.seek(0)
                self.df = pd.read_csv(r, header=self.header)
                self.add_dummy_header()

    def add_dummy_header(self):
        cols = self.df.columns
        dummy_df = pd.DataFrame(data=[cols] * self.header, columns=cols)
        self.df = dummy_df.append(self.df).reset_index(drop=True)

    def ftp_remove_files(self, newest_file):
        for item in self.files:
            file_info = item.split()[8]
            if file_info != newest_file:
                try:
                    self.ftp.delete(self.ftp_path + '/' + file_info)
                except ftplib.error_perm:
                    continue
