import logging
import numpy as np
import pandas as pd
import dictcolumns as dctc
import vmcolumns as vmc
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
PLACEDATE = 'Placement Date'

DIF_PNC = 'Dif - PNC'
DIF_NC = 'Dif - NC'
DIF_NCPNC = 'Dif - NC/PNC'
DIF_COL = [dctc.PFPN, DIF_PNC, DIF_NC, DIF_NCPNC]

NC_CUMSUM = 'Net Cost CumSum'
NC_SUMDATE = 'Net Cost Sum Date'
NC_CUMSUMMINDATE = 'NC_CUMSUMMINDATE'

NC_CUMSUM_COL = [dctc.PFPN, vmc.date, NC_CUMSUM]
NC_SUMDATE_COL = [dctc.PFPN, vmc.date, NC_SUMDATE]

DROPCOL = ([dctc.FPN, 'index', CLI_PD, NC_CUMSUM, NC_SUMDATE, PLACEDATE,
            NC_CUMSUMMINDATE] + DIF_COL)


def clicks_by_placedate(df):
    df[PLACEDATE] = (df[vmc.date].dt.strftime('%m/%d/%Y') +
                     df[dctc.PN].astype(str))
    df_click_placedate = (df.groupby([PLACEDATE])[[vmc.clicks]]
                          .apply(lambda x: x/float(x.sum())).astype(float))
    df_click_placedate = df_click_placedate.replace(np.nan, 0).astype(float)
    df_click_placedate.columns = [CLI_PD]
    df = pd.concat([df, df_click_placedate], axis=1)
    return df


def netcost(df):
    if df[dctc.BM] == BM_CPM or df[dctc.BM] == BM_AV:
        return df[dctc.BR] * (df[vmc.impressions] / 1000)
    elif df[dctc.BM] == BM_CPC:
        return df[dctc.BR] * df[vmc.clicks]
    elif df[dctc.BM] == BM_PA:
        return df[vmc.cost] / .85
    elif df[dctc.BM] == BM_FLAT or df[dctc.BM] == BM_FLAT2:
        if df[vmc.date] == df[dctc.PD]:
            return df[dctc.BR] * df[CLI_PD]
    elif df[dctc.BM] == BM_CPA:
        return df[dctc.BR] * df[vmc.conv1]
    elif df[dctc.BM] == BM_CPA2:
        if df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        else:
            return df[dctc.BR2] * df[vmc.conv1]
    elif df[dctc.BM] == BM_CPA3:
        if df[vmc.date] >= df[dctc.PD2]:
            return df[dctc.BR3] * df[vmc.conv1]
        elif df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        elif df[vmc.date] >= df[dctc.PD] and df[vmc.date] < df[dctc.PD2]:
            return df[dctc.BR2] * df[vmc.conv1]
    elif df[dctc.BM] == BM_CPA4:
        if df[vmc.date] >= df[dctc.PD3]:
            return df[dctc.BR4] * df[vmc.conv1]
        elif df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        elif df[vmc.date] >= df[dctc.PD2] and df[vmc.date] < df[dctc.PD3]:
            return df[dctc.BR3] * df[vmc.conv1]
        elif df[vmc.date] >= df[dctc.PD] and df[vmc.date] < df[dctc.PD2]:
            return df[dctc.BR2] * df[vmc.conv1]
    elif df[dctc.BM] == BM_CPA5:
        if df[vmc.date] >= df[dctc.PD4]:
            return df[dctc.BR5] * df[vmc.conv1]
        elif df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        elif df[vmc.date] >= df[dctc.PD3] and df[vmc.date] < df[dctc.PD4]:
            return df[dctc.BR4] * df[vmc.conv1]
        elif df[vmc.date] >= df[dctc.PD2] and df[vmc.date] < df[dctc.PD3]:
            return df[dctc.BR3] * df[vmc.conv1]
        elif df[vmc.date] >= df[dctc.PD] and df[vmc.date] < df[dctc.PD2]:
            return df[dctc.BR2] * df[vmc.conv1]
    else:
        return df[vmc.cost]


def netcost_calculation(df):
    logging.info('Calculating Net Cost')
    df = clicks_by_placedate(df)
    df[vmc.cost] = df.apply(netcost, axis=1)
    return df


def net_plan_comp(df):
    df = df.replace(np.nan, 0)
    nc_pnc = df.groupby(dctc.PFPN)[dctc.PNC, vmc.cost].sum()
    nc_pnc[DIF_NCPNC] = nc_pnc[vmc.cost] - nc_pnc[dctc.PNC]
    nc_pnc = nc_pnc.reset_index()
    nc_pnc.columns = DIF_COL
    df = df.merge(nc_pnc, on=dctc.PFPN, how='left')
    return df


def net_cumsum(df):
    nc_cumsum = (df.groupby([dctc.PFPN, vmc.date])[vmc.cost].sum()
                 .groupby(level=[0]).cumsum()).reset_index()
    nc_cumsum.columns = NC_CUMSUM_COL
    df = df.merge(nc_cumsum, on=[dctc.PFPN, vmc.date], how='left')
    return df


def net_sumdate(df):
    nc_sumdate = (df.groupby([dctc.PFPN, vmc.date])[vmc.cost]
                  .sum().reset_index())
    nc_sumdate.columns = NC_SUMDATE_COL
    df = df.merge(nc_sumdate, on=[dctc.PFPN, vmc.date], how='left')
    return df


def netcostfinal(df):
    nc_cumsummin = (df[df[NC_CUMSUM] > df[DIF_PNC]].groupby([dctc.PFPN]).min()
                    .reset_index())
    if not nc_cumsummin.empty:
        nc_cumsummin = nc_cumsummin[[vmc.date, dctc.PFPN]]
        nc_cumsummin[NC_CUMSUMMINDATE] = True
        df = df.merge(nc_cumsummin, on=[dctc.PFPN, vmc.date], how='left')
        df[NCF] = np.where(df[NC_CUMSUM] > df[DIF_PNC],
                           (np.where(df[NC_CUMSUMMINDATE] == True,
                                     (df[vmc.cost] -
                                     (df[vmc.cost] / (df[NC_SUMDATE])) *
                                     (df[NC_CUMSUM] - df[DIF_PNC])),
                                     0)),
                           df[vmc.cost])
    else:
        df[NC_CUMSUMMINDATE] = 0
        df[NCF] = df[vmc.cost]
    df = cln.col_removal(df, 'Raw Data', DROPCOL)
    return df


def netcostfinal_calculation(df):
    logging.info('Calculating Net Cost Final')
    df = net_plan_comp(df)
    df = net_cumsum(df)
    df = net_sumdate(df)
    df = netcostfinal(df)
    return df


def calculate_cost(df):
    df = netcost_calculation(df)
    df = netcostfinal_calculation(df)
    return df
