import logging
import fbapi
import awapi
import twapi
import szkftp
import os
import datetime as dt
import pandas as pd
import vmcolumns as vmc

log = logging.getLogger()


class ImportHandler(object):
    def __init__(self, args, matrix):
        self.args = args
        self.matrix = matrix

    def output(self, apimerge, apidf, filename):
        if str(apimerge) != 'nan':
            apimergefile = vmc.pathraw + apimerge
            if os.path.isfile(apimergefile):
                try:
                    df = pd.read_csv(apimergefile)
                    df = df.append(apidf, ignore_index=True)
                    df.to_csv(apimergefile, index=False)
                except IOError:
                    logging.warn(apimerge + ' could not be opened.  ' +
                                 'API data was not merged.')
                    apidf.to_csv(apimergefile)
            else:
                logging.warn(apimerge + ' not found.  Creating file.')
                df = pd.DataFrame()
                df = df.append(apidf, ignore_index=True)
                df.to_csv(apimergefile, index=False)
        else:
            apidf.to_csv(vmc.pathraw + filename, index=False)

    def arg_check(self, argcheck):
        if self.args == argcheck or self.args == 'all':
            return True
        else:
            return False

    def date_check(self, date):
        if date.date() == (dt.date.today() - dt.timedelta(weeks=520)):
            return True
        return False

    def api_call(self, keylist, apiclass):
        for vk in keylist:
            params = self.matrix.vendor_set(vk)
            apiclass.inputconfig(params[vmc.apifile])
            startcheck = self.date_check(params[vmc.startdate])
            endcheck = self.date_check(params[vmc.enddate])
            if startcheck and endcheck:
                df = apiclass.getdata()
            elif startcheck:
                df = apiclass.getdata(ed=params[vmc.enddate])
            elif endcheck:
                df = apiclass.getdata(sd=params[vmc.startdate])
            else:
                df = apiclass.getdata(sd=params[vmc.startdate],
                                      ed=params[vmc.enddate])
            self.output(params[vmc.apimerge], df, params[vmc.filename])

    def api_loop(self):
        if self.arg_check('fb'):
            self.api_call(self.matrix.apifbkey, fbapi.FbApi())
        if self.arg_check('aw'):
            self.api_call(self.matrix.apiawkey, awapi.AwApi())
        if self.arg_check('tw'):
            self.api_call(self.matrix.apitwkey, twapi.TwApi())

    def ftp_load(self, ftpkey, ftpclass):
        for vk in ftpkey:
            params = self.matrix.vendor_set(vk)
            ftpclass.inputconfig(params[vmc.apifile])
            df = ftpclass.getdata()
            self.output(params[vmc.apimerge], df, params[vmc.filename])

    def ftp_loop(self):
        if self.arg_check('sz'):
            self.ftp_load(self.matrix.ftpszkey, szkftp.SzkFtp())
