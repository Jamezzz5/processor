
FPN = 'Full Placement Name'
PN = 'mpPlacement Name'
AGY = 'mpAgency'
CLI = 'mpClient'
BUD = 'mpBudget'
FRA = 'mpFranchise'
CAM = 'mpCampaign'
CTIM = 'mpCampaign Timing'
CT = 'mpCampaign Type'
CP = 'mpCampaign Phase'
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
AGE = 'mpAge'
GEN = 'mpGender'
CTA = 'mpCTA'
URL = 'mpClickthrough URL'
PRN = 'mpProduct Name'
PRD = 'mpProduct Detail'
FOR = 'mpFormat'
ENV = 'mpEnvironment'
DT1 = 'mpData Type 1'
DT2 = 'mpData Type 2'
TB = 'mpTargeting Bucket'
GT = 'mpGenre Targeting'
KPI = 'mpKPI'
DN = 'mpDescriptive Name'
AT = 'mpAd Type'
AF = 'mpAd Format'
CD = 'mpCreative Description'
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
COLS = [FPN, PN, AGY, CLI, BUD, FRA, CAM, CTIM, CT, CP, VEN, COU, VT, MC, TAR,
        CRE, COP, SIZ, BM, BR, PD, SRV, MIS, RET, AM, AR, AGE, GEN, CTA, URL,
        PRN, PRD, FOR, ENV, DT1, DT2, TB, GT, KPI, DN, AT, AF, CD, BR2, BR3,
        BR4, BR5, PD2, PD3, PD4, PD5, MIS2, MIS3, MIS4, MIS5, MIS6]

floatcol = [BR, AR, BR2, BR3]
datecol = [PD, PD2, PD3, PD4, PD5]
strcol = [BM, AM, VEN]

PFN = 'plannet_dictionary.csv'
PNC = 'Planned Net Cost'
PCOLS = [FPN, PNC]
PFPN = 'PNC FPN'


full_placement_cols = [FPN, CAM, VEN, COU, MC, TAR, CRE, COP, BM, BR, PD, SRV,
                       RET, ENV, KPI]

product_cols = [PRN, PRD]
campaign_cols = [CAM, CTIM, CP, CT]
vendor_cols = [VEN, VT]
targeting_cols = [TAR, AGE, GEN, DT1, DT2, TB, GT]
creative_cols = [CRE, SIZ, AF, AT, CTA]
copy_cols = [COP]


filename_rel_config = 'relational_dictionary_config.csv'
RK = 'Relational_Key'
FN = 'Filename'
KEY = 'Key'
DEP = 'Dependent'

filename_con_config = 'constant_dictionary_config.csv'
DICT_COL_NAME = 'Column Name'
DICT_COL_VALUE = 'Value'
