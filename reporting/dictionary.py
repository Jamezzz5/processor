import os.path
import logging
import pandas as pd
import cleaning as cln

log = logging.getLogger()

csvpath = 'Dictionaries/'

FPN = 'Full Placement Name'
PN = 'mpPlacement Name'
BUD = 'mpBudget'
CAM = 'mpCampaign'
VEN = 'mpVendor'
COU = 'mpCountry/Region'
VT = 'mpVendor Type'
MC = 'mpMedia Channel'
TAR = 'mpTargeting'
SIZ = 'mpSize'
CRE = 'mpCreative'
COP = 'mpCopy'
BM = 'mpBuy Model'
BR = 'mpBuy Rate'
PD = 'mpPlacement Date'
SRV = 'mpServing'
MIS = 'mpMisc'
RET = 'mpRetailer'
AM = 'mpAd Model'
AR = 'mpAd Rate'
BR2 = 'mpBuy Rate 2'
BR3 = 'mpBuy Rate 3'
BR4 = 'mpBuy Rate 4'
BR5 = 'mpBuy Rate 5'
PD2 = 'mpPlacement Date 2'
PD3 = 'mpPlacement Date 3'
PD4 = 'mpPlacement Date 4'
PD5 = 'mpPlacement Date 5'
MIS2 = 'mpMisc 2'
MIS3 = 'mpMisc 3'
MIS4 = 'mpMisc 4'
MIS5 = 'mpMisc 5'
MIS6 = 'mpMisc 6'
COLS = [FPN, PN, BUD, CAM, VEN, COU, VT, MC, TAR, CRE, COP, SIZ, BM, BR, PD,
        SRV, MIS, RET, AM, AR, BR2, BR3, BR4, BR5, PD2, PD3, PD4, PD5, MIS2,
        MIS3, MIS4, MIS5, MIS6]

PFN = 'plannet_dictionary.csv'
PNC = 'Planned Net Cost'
PCOLS = [FPN, PNC]
PFPN = 'PNC FPN'

floatcol = [BR, AR, BR2, BR3]
datecol = [PD, PD2, PD3, PD4, PD5]
strcol = [BM, AM, VEN]


class Dict(object):
    def __init__(self, filename):
        self.filename = filename
        self.dictfile = csvpath + self.filename
        self.read()

    def read(self):
        if os.path.isfile(self.dictfile) == False:
            logging.info('Creating ' + self.filename)
            if self.filename == PFN:
                data_dict = pd.DataFrame(columns=PCOLS, index=None)
            else:
                data_dict = pd.DataFrame(columns=COLS, index=None)
            data_dict.to_csv(self.dictfile, index=False)
        self.data_dict = pd.read_csv(self.dictfile)
        self.clean()

    def get(self):
        return self.data_dict

    def merge(self, df, colname):
        logging.info('Merging ' + self.filename)
        df = df.merge(self.data_dict, on=colname, how='left')
        return df

    def auto(self, err, autodicord, placement):
        error = err.get()
        if not autodicord == ['nan'] and not error.empty:
            logging.info('Populating ' + self.filename)
            i = 0
            for value in autodicord:
                error[value] = error[PN].str.split('_').str[i]
                i = i + 1
            error = error.ix[~error[FPN].isin(self.data_dict[FPN])]
            self.data_dict = self.data_dict.append(error)
            self.data_dict = self.data_dict[COLS]
            self.write()
            err.dic = self
            err.reset()
            self.clean()

    def write(self):
        logging.info('Writing ' + self.filename)
        try:
            self.data_dict.to_csv(self.dictfile, index=False)
        except IOError:
            logging.warn(self.filename + ' could not be opened.  ' +
                         'This dictionary was not saved.')

    def clean(self):
        self.data_dict = cln.data_to_type(self.data_dict,
                                          floatcol, datecol, strcol)
