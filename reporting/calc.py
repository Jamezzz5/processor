import logging
import numpy as np
import pandas as pd
import reporting.dictcolumns as dctc
import reporting.vmcolumns as vmc
import reporting.utils as utl

BM_CPM = 'CPM'
BM_CPC = 'CPC'
BM_AV = 'AV'
BM_FLAT = 'FLAT'
BM_FLAT2 = 'Flat'
BM_FLATIMP = 'FlatImp'
BM_PA = 'Programmaddict'
BM_CPA = 'CPA'
BM_CPACPM = 'CPA/CPM'
BM_CPA2 = 'CPA2'
BM_CPA3 = 'CPA3'
BM_CPA4 = 'CPA4'
BM_CPA5 = 'CPA5'
BM_FLATDATE = 'FlatDate'
BUY_MODELS = [BM_CPM, BM_CPC, BM_AV, BM_FLAT, BM_FLATIMP, BM_FLAT2, BM_PA,
              BM_CPA, BM_CPA2, BM_CPA3, BM_CPA4, BM_CPA5, BM_FLATDATE,
              BM_CPACPM]

NCF = 'Net Cost Final'

AGENCY_FEES = 'Agency Fees'
TOTAL_COST = 'Total Cost'

CLI_PD = 'Clicks by Placement Date'
IMP_PD = 'Impressions by Placement Date'
PLACE_DATE = 'Placement Date'

DIF_PNC = 'Dif - PNC'
DIF_NC = 'Dif - NC'
DIF_NC_PNC = 'Dif - NC/PNC'
DIF_COL = [dctc.PFPN, DIF_PNC, DIF_NC, DIF_NC_PNC]

NC_CUM_SUM = 'Net Cost CumSum'
NC_SUM_DATE = 'Net Cost Sum Date'
NC_CUM_SUM_MIN_DATE = 'NC_CUMSUMMINDATE'

NC_CUM_SUM_COL = [dctc.PFPN, vmc.date, NC_CUM_SUM]
NC_SUM_DATE_COL = [dctc.PFPN, vmc.date, NC_SUM_DATE]

DROP_COL = ([CLI_PD, NC_CUM_SUM, NC_SUM_DATE, PLACE_DATE,
             NC_CUM_SUM_MIN_DATE] + DIF_COL)


def clicks_by_place_date(df):
    df[dctc.PN] = df[dctc.PN].replace(np.nan, 'None')
    df[PLACE_DATE] = (df[vmc.date].astype('U') + df[dctc.PN].astype('U'))
    df_cpd = df.loc[df[dctc.BM].isin([BM_FLAT, BM_FLAT2, BM_FLATIMP])]
    if not df_cpd.empty:
        df_cpd = (df_cpd.groupby([PLACE_DATE])[vmc.impressions, vmc.clicks]
                  .apply(lambda x: x / x.astype(float).sum()))
        df_cpd.columns = [IMP_PD, CLI_PD]
        df = pd.concat([df, df_cpd], axis=1)  # type: pd.DataFrame
        for col in [CLI_PD, IMP_PD]:
            df[col] = df[col].replace(np.nan, 0).astype(float)
    return df


def net_cost(df):
    if df[dctc.BM] == BM_CPM or df[dctc.BM] == BM_AV:
        return df[dctc.BR] * (df[vmc.impressions] / 1000)
    elif df[dctc.BM] == BM_CPC:
        return df[dctc.BR] * df[vmc.clicks]
    elif df[dctc.BM] == BM_PA:
        return df[vmc.cost] / .85
    elif df[dctc.BM] == BM_FLAT or df[dctc.BM] == BM_FLAT2:
        if df[vmc.date] == df[dctc.PD]:
            return df[dctc.BR] * df[CLI_PD]
    elif df[dctc.BM] == BM_FLATIMP:
        if df[vmc.date] == df[dctc.PD]:
            return df[dctc.BR] * df[IMP_PD]
    elif df[dctc.BM] == BM_CPACPM:
        if df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        else:
            return df[dctc.BR2] * (df[vmc.impressions] / 1000)
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
        elif df[dctc.PD2] > df[vmc.date] >= df[dctc.PD]:
            return df[dctc.BR2] * df[vmc.conv1]
    elif df[dctc.BM] == BM_CPA4:
        if df[vmc.date] >= df[dctc.PD3]:
            return df[dctc.BR4] * df[vmc.conv1]
        elif df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        elif df[dctc.PD3] > df[vmc.date] >= df[dctc.PD2]:
            return df[dctc.BR3] * df[vmc.conv1]
        elif df[dctc.PD2] > df[vmc.date] >= df[dctc.PD]:
            return df[dctc.BR2] * df[vmc.conv1]
    elif df[dctc.BM] == BM_CPA5:
        if df[vmc.date] >= df[dctc.PD4]:
            return df[dctc.BR5] * df[vmc.conv1]
        elif df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        elif df[dctc.PD4] > df[vmc.date] >= df[dctc.PD3]:
            return df[dctc.BR4] * df[vmc.conv1]
        elif df[dctc.PD3] > df[vmc.date] >= df[dctc.PD2]:
            return df[dctc.BR3] * df[vmc.conv1]
        elif df[dctc.PD2] > df[vmc.date] >= df[dctc.PD]:
            return df[dctc.BR2] * df[vmc.conv1]
    else:
        return df[vmc.cost]


def net_cost_calculation(df):
    logging.info('Calculating Net Cost')
    df = clicks_by_place_date(df)
    calc_ser = df[df[dctc.BM].isin(BUY_MODELS)].apply(net_cost, axis=1)
    if not calc_ser.empty:
        df[vmc.cost].update(calc_ser)
    return df


def net_plan_comp(df):
    nc_pnc = df[df[dctc.UNC] != True]
    nc_pnc = nc_pnc.groupby(dctc.PFPN)[dctc.PNC, vmc.cost].sum()
    if dctc.PNC not in nc_pnc.columns:
        nc_pnc[dctc.PNC] = 0
    nc_pnc[DIF_NC_PNC] = nc_pnc[vmc.cost] - nc_pnc[dctc.PNC]
    nc_pnc = nc_pnc.reset_index()
    nc_pnc.columns = DIF_COL
    df = df.merge(nc_pnc, on=dctc.PFPN, how='left')
    return df


def net_cum_sum(df):
    nc_cum_sum = (df.groupby([dctc.PFPN, vmc.date])[vmc.cost].sum()
                  .groupby(level=[0]).cumsum()).reset_index()
    nc_cum_sum.columns = NC_CUM_SUM_COL
    df = df.merge(nc_cum_sum, on=[dctc.PFPN, vmc.date], how='left')
    return df


def net_sum_date(df):
    nc_sum_date = (df.groupby([dctc.PFPN, vmc.date])[vmc.cost]
                   .sum().reset_index())
    nc_sum_date.columns = NC_SUM_DATE_COL
    df = df.merge(nc_sum_date, on=[dctc.PFPN, vmc.date], how='left')
    return df


def net_cost_final(df):
    nc_cum_sum_min = (df[df[NC_CUM_SUM] > df[DIF_PNC]].groupby([dctc.PFPN]).
                      min().reset_index())
    if not nc_cum_sum_min.empty:
        nc_cum_sum_min = nc_cum_sum_min[[vmc.date, dctc.PFPN]]
        nc_cum_sum_min[NC_CUM_SUM_MIN_DATE] = True
        df = df.merge(nc_cum_sum_min, on=[dctc.PFPN, vmc.date], how='left')
        df[NCF] = np.where(df[NC_CUM_SUM] > df[DIF_PNC],
                           (np.where(df[NC_CUM_SUM_MIN_DATE] == True,
                                     (df[vmc.cost] -
                                      (df[vmc.cost] / (df[NC_SUM_DATE])) *
                                      (df[NC_CUM_SUM] - df[DIF_PNC])),
                                     0)),
                           df[vmc.cost])
    else:
        df[NC_CUM_SUM_MIN_DATE] = 0
        df[NCF] = df[vmc.cost]
    df = utl.col_removal(df, 'Raw Data', DROP_COL)
    return df


def net_cost_final_calculation(df):
    logging.info('Calculating Net Cost Final')
    df = net_plan_comp(df)
    df = net_cum_sum(df)
    df = net_sum_date(df)
    df = net_cost_final(df)
    return df


def agency_fees_calculation(df):
    logging.info('Calculating Agency Fees')
    if dctc.AGF not in df.columns:
        logging.warning('Agency Fee Rates not in dict.  '
                        'Update dict and run again to calculate agency fees.')
        return df
    df = utl.data_to_type(df, float_col=[NCF, dctc.AGF])
    df[AGENCY_FEES] = df[dctc.AGF] * df[NCF]
    return df


def total_cost_calculation(df):
    logging.info('Calculating Total Cost')
    if AGENCY_FEES not in df.columns:
        logging.warning('Agency Fees not in dataframe.  '
                        'Update dict and run again to calculate total cost.')
        return df
    df = utl.data_to_type(df, float_col=[NCF, AGENCY_FEES, vmc.AD_COST,
                                         vmc.dcm_service_fee, vmc.REP_COST])
    df[TOTAL_COST] = df[NCF] + df[AGENCY_FEES]
    for col in [vmc.AD_COST, vmc.dcm_service_fee, vmc.REP_COST]:
        if col in df.columns:
            df[TOTAL_COST] += df[col]
    return df


def calculate_cost(df):
    if vmc.cost in df.columns:
        df = net_cost_calculation(df)
        df = net_cost_final_calculation(df)
        df = agency_fees_calculation(df)
        df = total_cost_calculation(df)
    return df
