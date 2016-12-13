import logging
import fbapi
import os
import pandas as pd
import vendormatrix as vm

log = logging.getLogger()


def apicalls(matrix):
    params = matrix.vendor_set('Facebook API')
    fb = fbapi.FbApi()
    df = fb.getdata(params[vm.startdate].date(), params[vm.enddate].date())
    if params[vm.apimerge]:
        odf = fb.renamecols()
        if os.path.isfile(vm.pathraw + params[vm.apimerge]):
            try:
                df = pd.read_csv(vm.pathraw + params[vm.apimerge])
                df = df.append(odf, ignore_index=True)
                df.to_csv(vm.pathraw + params[vm.apimerge], index=False)
            except IOError:
                logging.warn(params[vm.apimerge] + ' could not be opened.  ' +
                             'API data was not merged.')
                odf.to_csv(vm.pathraw + params[vm.filename])
        else:
            logging.warn(params[vm.apimerge] + ' not found.  Creating file.')
            df = pd.DataFrame()
            df = df.append(odf, ignore_index=True)
            df.to_csv(vm.pathraw + params[vm.apimerge], index=False)
    else:
        df.to_csv(vm.pathraw + params[vm.filename], index=False)
