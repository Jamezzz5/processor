import logging
import fbapi
import pandas as pd
import vendormatrix as vm

log = logging.getLogger()


def apicalls(matrix):
    params = matrix.vendor_set('Facebook')
    fb = fbapi.FbApi()
    print (params[vm.startdate], params[vm.enddate], params[vm.apifields])
    df = fb.getdata(params[vm.startdate].date())
    df.to_csv('test.csv')
