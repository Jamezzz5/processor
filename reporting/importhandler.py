import logging
import fbapi
import awapi
import twapi
import szkftp
import os
import pandas as pd
import vendormatrix as vm

log = logging.getLogger()


class ImportHandler(object):
    def __init__(self, args, matrix):
        self.args = args
        self.matrix = matrix

    def output(self, apimerge, apidf, filename):
        if str(apimerge) != 'nan':
            apimergefile = vm.pathraw + apimerge
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
            apidf.to_csv(vm.pathraw + filename, index=False)

    def arg_check(self, argcheck):
        if self.args == argcheck or self.args == 'all':
            return True
        else:
            return False

    def api_call(self, keylist, apiclass):
        for vk in keylist:
            params = self.matrix.vendor_set(vk)
            apiclass.inputconfig(params[vm.apifile])
            df = apiclass.getdata(params[vm.startdate], params[vm.enddate])
            self.output(params[vm.apimerge], df, params[vm.filename])

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
            ftpclass.inputconfig(params[vm.apifile])
            df = ftpclass.getdata()
            self.output(params[vm.apimerge], df, params[vm.filename])

    def ftp_loop(self):
        if self.arg_check('sz'):
            self.ftp_load(self.matrix.ftpszkey, szkftp.SzkFtp())
