import logging
import numpy as np
import pandas as pd
import dictionary as dct
import vendormatrix as vm
import cleaning as cln

log = logging.getLogger()

BM_CPM = 'CPM'
BM_CPC = 'CPC'
BM_AV = 'AV'
BM_FLAT = 'FLAT'
BM_FLAT2 = 'Flat'
BM_FB = 'Facebook'
BM_GS = 'Google'
BM_PA = 'Programmaddict'
BM_TW = 'Twitter'
BM_BS = 'Bing'
BM_CPI = 'CPI'
BM_CPA = 'CPA'
BM_CPA2 = 'CPA2'
BM_CPA3 = 'CPA3'
BM_CPA4 = 'CPA4'
BM_CPA5 = 'CPA5'
BM_FLATDATE = 'FlatDate'

NCF = 'Net Cost Final'

CLI_PNPD = 'Clicks by Placement & Placement Date'
CLI_PD = 'Clicks by Placement Date'

DIF_PNC = 'Dif - PNC'
DIF_NC = 'Dif - NC'
DIF_NCPNC = 'Dif - NC/PNC'
DIF_COL = [dct.PFPN, DIF_PNC, DIF_NC, DIF_NCPNC]

NC_CUMSUM = 'Net Cost CumSum'
NC_SUMDATE = 'Net Cost Sum Date'
NC_CUMSUMMINDATE = 'NC_CUMSUMMINDATE'

NC_CUMSUM_COL = [dct.PFPN, vm.date, NC_CUMSUM]
NC_SUMDATE_COL = [dct.PFPN, vm.date, NC_SUMDATE]

DROPCOL = ([dct.FPN, 'index', CLI_PD, NC_CUMSUM, NC_SUMDATE,
            NC_CUMSUMMINDATE] + DIF_COL)


def clicks_by_placedate(df):
    df_click_placedate = (df.groupby([vm.date, dct.PN])[[vm.clicks]]
                          .apply(lambda x: x/float(x.sum())).astype(float))
    df_click_placedate = df_click_placedate.replace(np.nan, 0).astype(float)
    df_click_placedate.columns = [CLI_PD]
    df = pd.concat([df, df_click_placedate], axis=1)
    return df


def netcost(df):
    if df[dct.BM] == BM_CPM or df[dct.BM] == BM_AV:
        return df[dct.BR] * (df[vm.impressions] / 1000)
    elif df[dct.BM] == BM_CPC:
        return df[dct.BR] * df[vm.clicks]
    elif df[dct.BM] == BM_PA:
        return df[vm.cost] / .85
    elif df[dct.BM] == BM_FLAT or df[dct.BM] == BM_FLAT2:
        if df[vm.date] == df[dct.PD]:
            return df[dct.BR] * df[CLI_PD]
    elif df[dct.BM] == BM_CPA:
        return df[dct.BR] * df[vm.conv1]
    elif df[dct.BM] == BM_CPA2:
        if df[vm.date] < df[dct.PD]:
            return df[dct.BR] * df[vm.conv1]
        else:
            return df[dct.BR2] * df[vm.conv1]
    elif df[dct.BM] == BM_CPA3:
        if df[vm.date] >= df[dct.PD2]:
            return df[dct.BR3] * df[vm.conv1]
        elif df[vm.date] < df[dct.PD]:
            return df[dct.BR] * df[vm.conv1]
        elif df[vm.date] >= df[dct.PD] and df[vm.date] < df[dct.PD2]:
            return df[dct.BR2] * df[vm.conv1]
    elif df[dct.BM] == BM_CPA4:
        if df[vm.date] >= df[dct.PD3]:
            return df[dct.BR4] * df[vm.conv1]
        elif df[vm.date] < df[dct.PD]:
            return df[dct.BR] * df[vm.conv1]
        elif df[vm.date] >= df[dct.PD2] and df[vm.date] < df[dct.PD3]:
            return df[dct.BR3] * df[vm.conv1]
        elif df[vm.date] >= df[dct.PD] and df[vm.date] < df[dct.PD2]:
            return df[dct.BR2] * df[vm.conv1]
    elif df[dct.BM] == BM_CPA5:
        if df[vm.date] >= df[dct.PD4]:
            return df[dct.BR5] * df[vm.conv1]
        elif df[vm.date] < df[dct.PD]:
            return df[dct.BR] * df[vm.conv1]
        elif df[vm.date] >= df[dct.PD3] and df[vm.date] < df[dct.PD4]:
            return df[dct.BR4] * df[vm.conv1]
        elif df[vm.date] >= df[dct.PD2] and df[vm.date] < df[dct.PD3]:
            return df[dct.BR3] * df[vm.conv1]
        elif df[vm.date] >= df[dct.PD] and df[vm.date] < df[dct.PD2]:
            return df[dct.BR2] * df[vm.conv1]
    else:
        return df[vm.cost]


def netcost_calculation(df):
    logging.info('Calculating Net Cost')
    df = clicks_by_placedate(df)
    df[vm.cost] = df.apply(netcost, axis=1)
    return df


def net_plan_comp(df):
    df = df.replace(np.nan, 0)
    nc_pnc = df.groupby(dct.PFPN)[dct.PNC, vm.cost].sum()
    nc_pnc[DIF_NCPNC] = nc_pnc[vm.cost] - nc_pnc[dct.PNC]
    nc_pnc = nc_pnc.reset_index()
    nc_pnc.columns = DIF_COL
    df = df.merge(nc_pnc, on=dct.PFPN, how='left')
    return df


def net_cumsum(df):
    nc_cumsum = (df.groupby([dct.PFPN, vm.date])[vm.cost].sum()
                 .groupby(level=[0]).cumsum()).reset_index()
    nc_cumsum.columns = NC_CUMSUM_COL
    df = df.merge(nc_cumsum, on=[dct.PFPN, vm.date], how='left')
    return df


def net_sumdate(df):
    nc_sumdate = df.groupby([dct.PFPN, vm.date])[vm.cost].sum().reset_index()
    nc_sumdate.columns = NC_SUMDATE_COL
    df = df.merge(nc_sumdate, on=[dct.PFPN, vm.date], how='left')
    return df


def netcostfinal(df):
    nc_cumsummin = (df[df[NC_CUMSUM] > df[DIF_PNC]].groupby([dct.PFPN]).min()
                    .reset_index())
    if not nc_cumsummin.empty:
        nc_cumsummin = nc_cumsummin[[vm.date, dct.PFPN]]
        nc_cumsummin[NC_CUMSUMMINDATE] = True
        df = df.merge(nc_cumsummin, on=[dct.PFPN, vm.date], how='left')
        df[NCF] = np.where(df[NC_CUMSUM] > df[DIF_PNC],
                           (np.where(df[NC_CUMSUMMINDATE] == True,
                                     (df[vm.cost] -
                                     (df[vm.cost] / (df[NC_SUMDATE])) *
                                     (df[NC_CUMSUM] - df[DIF_PNC])),
                                     0)),
                           df[vm.cost])
    else:
        df[NC_CUMSUMMINDATE] = 0
        df[NCF] = df[vm.cost]
    df = cln.col_removal(df, 'Raw Data', DROPCOL)
    return df


def netcostfinal_calculation(df):
    logging.info('Calculating Net Cost Final')
    df = net_plan_comp(df)
    df = net_cumsum(df)
    df = net_sumdate(df)
    df = netcostfinal(df)
    return df
