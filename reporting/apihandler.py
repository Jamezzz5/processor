import logging
import fbapi
import awapi
import os
import pandas as pd
import vendormatrix as vm

log = logging.getLogger()


def api_output(apimerge, apidf, filename):
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


def api_call(args, inparg, apikey, matrix, apiclass):
    if args == inparg or args == 'all':
        for vk in apikey:
            params = matrix.vendor_set(vk)
            apiclass.inputconfig(params[vm.apifile])
            df = apiclass.getdata(params[vm.startdate].date(),
                                  params[vm.enddate].date())
            api_output(params[vm.apimerge], df, params[vm.filename])


def apicalls(args, matrix):
    api_call(args, 'fb', matrix.apifbkey, matrix, fbapi.FbApi())
    api_call(args, 'aw', matrix.apiawkey, matrix, awapi.AwApi())
