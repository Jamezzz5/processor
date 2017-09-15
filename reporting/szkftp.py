import ftplib
import logging
import json
import sys
import time
from StringIO import StringIO
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
                logging.warn(item + 'not in Sizmek config file.  Aborting.')
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
            logging.warn('No files found, returning empty Dataframe.')
            return self.df
        self.ftp_read_file(newest_file)
        return self.df

    def ftp_newest_file(self):
        file_dict = {}
        self.files = []
        self.ftp.dir(self.files.append)
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
        r = StringIO()
        try:
            self.ftp.retrbinary('RETR ' + read_file, r.write)
        except ftplib.all_errors as e:
            logging.warning('Connection to the FTP was closed due to below ' +
                            'error, pausing for 30 seconds. ' + str(e))
            time.sleep(30)
            self.ftp.quit()
            self.ftp_init()
            self.ftp_read_file(read_file)
        self.ftp.quit()
        r.seek(0)
        if read_file[-4:] == '.zip':
            self.df = pd.read_csv(r, compression='zip')
        else:
            self.df = pd.read_csv(r)

    def ftp_remove_files(self, newest_file):
        for item in self.files:
            file_info = item.split()[8]
            if file_info != newest_file:
                try:
                    self.ftp.delete(self.ftp_path + '/' + file_info)
                except ftplib.error_perm:
                    continue
