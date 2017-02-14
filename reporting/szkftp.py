import ftplib
import logging
import json
import sys
import time
from StringIO import StringIO
import pandas as pd

configpath = 'Config/'


class SzkFtp(object):
    def __init__(self):
        self.df = pd.DataFrame()

    def inputconfig(self, config):
        logging.info('Loading Sizmek config file: ' + config)
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
        self.ftp = self.config['FTP']
        self.ftp_path = self.config['FTP_PATH']
        self.ftp_file = self.config['FTP_FILE']
        self.username = self.config['USERNAME']
        self.password = self.config['PASSWORD']
        self.configlist = [self.ftp, self.ftp_path, self.ftp_file,
                           self.username, self.password]

    def checkconfig(self):
        for item in self.configlist:
            if item == '':
                logging.warn(item + 'not in Sizmek config file.  Aborting.')
                sys.exit(0)

    def getdata(self):
        logging.info('Getting Sizmek data from ' + self.ftp)
        self.ftp_init()
        newestfile = self.ftp_newestfile()
        self.ftp_remove_files(newestfile)
        if newestfile == '.' or newestfile == '..':
            logging.warn('No files found, returning empty Dataframe.')
            return self.df
        self.ftp_readfile(newestfile)
        return self.df

    def ftp_init(self):
        self.ftp = ftplib.FTP(self.ftp)
        self.ftp.login(self.username, self.password)
        self.ftp.cwd(self.ftp_path)

    def ftp_newestfile(self):
        self.files = []
        file_dict = {}
        self.ftp.dir(self.files.append)
        for item in self.files:
            file_info = item.split()
            file_date = ' '.join(item.split()[5:8])
            item_dict = {time.strptime(file_date, '%b %d %H:%M'): file_info[8]}
            file_dict.update(item_dict)
        date_list = list([key for key, value in file_dict.items()])
        newestfile = file_dict[max(date_list)]
        return newestfile

    def ftp_readfile(self, readfile):
        r = StringIO()
        self.ftp.retrbinary('RETR ' + self.ftp_path + readfile, r.write)
        self.ftp.quit()
        r.seek(0)
        if readfile[-4:] == '.zip':
            self.df = pd.read_csv(r, compression='zip')
        else:
            self.df = pd.read_csv(r)

    def ftp_remove_files(self, newestfile):
        for item in self.files:
            file_info = item.split()[8]
            if (file_info != newestfile and file_info != '.' and
               file_info != '..'):
                self.ftp.delete(self.ftp_path + file_info)
