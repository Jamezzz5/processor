import logging
import numpy as np
import pandas as pd
import reporting.utils as utl
import reporting.vmcolumns as vmc
import reporting.dictcolumns as dctc

BM_CPM = 'CPM'
BM_CPC = 'CPC'
BM_CPV = 'CPV'
BM_CPCV = 'CPCV'
BM_CPLP = 'CPLP'
BM_CPVM = 'CPVM'
BM_AV = 'AV'
BM_FLAT = 'FLAT'
BM_FLAT2 = 'Flat'
BM_FLATIMP = 'FlatImp'
BM_PA = 'Programmaddict'
BM_CPA = 'CPA'
BM_CPACPM = 'CPA/CPM'
BM_CPNUCPSU = 'CPNU/CPSU'
BM_CPE = 'CPE'
BM_CPA2 = 'CPA2'
BM_CPA3 = 'CPA3'
BM_CPA4 = 'CPA4'
BM_CPA5 = 'CPA5'
BM_FLATDATE = 'FlatDate'
BUY_MODELS = [BM_CPM, BM_CPC, BM_CPV, BM_CPCV, BM_CPLP, BM_CPVM, BM_AV, BM_FLAT,
              BM_FLATIMP, BM_FLAT2, BM_PA, BM_CPA, BM_CPA2, BM_CPA3, BM_CPA4,
              BM_CPA5, BM_FLATDATE, BM_CPACPM, BM_CPNUCPSU, BM_CPE]

AGENCY_FEES = 'Agency Fees'
AGENCY_THRESH = 'Agency Fee Threshold'
agency_fee_file = 'agencyfee_threshold.csv'

NCF = 'Net Cost Final'
TOTAL_COST = 'Total Cost'

CLI_PD = 'Clicks by Placement Date'
IMP_PD = 'Impressions by Placement Date'
PLACE_DATE = 'Placement Date'

DIF_PNC = 'Dif - PNC'
DIF_NC = 'Dif - NC'
DIF_NC_PNC = 'Dif - NC/PNC'
DIF_COL = [DIF_PNC, DIF_NC, DIF_NC_PNC]

NC_CUM_SUM = 'Net Cost CumSum'
NC_SUM_DATE = 'Net Cost Sum Date'
NC_CUM_SUM_MIN_DATE = 'NC_CUMSUMMINDATE'

NC_CUM_SUM_COL = [vmc.date, NC_CUM_SUM]
NC_SUM_DATE_COL = [vmc.date, NC_SUM_DATE]

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


def net_cost(df, cost_col=vmc.cost, bm_col=dctc.BM, br_col=dctc.BR):
    if df[bm_col] == BM_CPM or df[bm_col] == BM_AV:
        return df[br_col] * (df[vmc.impressions] / 1000)
    elif df[bm_col] == BM_CPC:
        return df[br_col] * df[vmc.clicks]
    elif df[bm_col] == BM_CPV:
        return df[br_col] * df[vmc.views]
    elif df[bm_col] == BM_CPCV:
        return df[br_col] * df[vmc.views100]
    elif df[bm_col] == BM_CPLP:
        return df[br_col] * df[vmc.landingpage]
    elif df[bm_col] == BM_CPVM:
        return df[br_col] * (df[vmc.view_imps] / 1000)
    elif df[bm_col] == BM_PA:
        return df[vmc.cost] / .85
    elif df[bm_col] == BM_CPE:
        return df[br_col] * df[vmc.engagements]
    elif df[bm_col] == BM_FLAT or df[bm_col] == BM_FLAT2:
        if df[vmc.date] == df[dctc.PD]:
            return df[br_col] * df[CLI_PD]
    elif df[bm_col] == BM_FLATIMP:
        if df[vmc.date] == df[dctc.PD]:
            return df[br_col] * df[IMP_PD]
    elif df[bm_col] == BM_CPACPM:
        if df[vmc.date] < df[dctc.PD]:
            return df[br_col] * df[vmc.conv1]
        else:
            return df[dctc.BR2] * (df[vmc.impressions] / 1000)
    elif df[bm_col] == BM_CPNUCPSU:
        return (df[br_col] * df[vmc.newuser]) + (df[dctc.BR2] * df[vmc.signup])
    elif df[bm_col] == BM_CPA:
        if vmc.conv1 in df:
            return df[dctc.BR] * df[vmc.conv1]
    elif df[bm_col] == BM_CPA2:
        if df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        else:
            return df[dctc.BR2] * df[vmc.conv1]
    elif df[bm_col] == BM_CPA3:
        if df[vmc.date] >= df[dctc.PD2]:
            return df[dctc.BR3] * df[vmc.conv1]
        elif df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        elif df[dctc.PD2] > df[vmc.date] >= df[dctc.PD]:
            return df[dctc.BR2] * df[vmc.conv1]
    elif df[bm_col] == BM_CPA4:
        if df[vmc.date] >= df[dctc.PD3]:
            return df[dctc.BR4] * df[vmc.conv1]
        elif df[vmc.date] < df[dctc.PD]:
            return df[dctc.BR] * df[vmc.conv1]
        elif df[dctc.PD3] > df[vmc.date] >= df[dctc.PD2]:
            return df[dctc.BR3] * df[vmc.conv1]
        elif df[dctc.PD2] > df[vmc.date] >= df[dctc.PD]:
            return df[dctc.BR2] * df[vmc.conv1]
    elif df[bm_col] == BM_CPA5:
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
        return df[cost_col]


def net_cost_calculation(df):
    logging.info('Calculating Net Cost')
    df = clicks_by_place_date(df)
    if BM_CPA in df[dctc.BM].unique() and vmc.conv1 not in df.columns:
        logging.warning('CPA buy model specified '
                        'without conversion {}.'.format(vmc.conv1))
    calc_ser = df[df[dctc.BM].isin(BUY_MODELS)].apply(net_cost, axis=1)
    if not calc_ser.empty:
        df[vmc.cost].update(calc_ser)
    return df


def net_plan_comp(df, p_col=dctc.PFPN, n_cost=vmc.cost, p_cost=dctc.PNC):
    df[p_cost] = df[p_cost].fillna(0)
    nc_pnc = df[df[dctc.UNC] != True]
    nc_pnc = nc_pnc.groupby(p_col)[p_cost, n_cost].sum()
    nc_pnc = nc_pnc[nc_pnc[p_cost] > 0]
    if p_cost not in nc_pnc.columns:
        nc_pnc[p_cost] = 0
    nc_pnc[DIF_NC_PNC] = nc_pnc[n_cost] - nc_pnc[p_cost]
    nc_pnc = nc_pnc.reset_index()
    nc_pnc.columns = [p_col] + DIF_COL
    df = df.merge(nc_pnc, on=p_col, how='left')
    return df


def net_cum_sum(df, p_col=dctc.PFPN, n_cost=vmc.cost):
    nc_cum_sum = (df.groupby([p_col, vmc.date])[n_cost].sum()
                  .groupby(level=[0]).cumsum()).reset_index()
    nc_cum_sum.columns = [p_col] + NC_CUM_SUM_COL
    df = df.merge(nc_cum_sum, on=[p_col, vmc.date], how='left')
    return df


def net_sum_date(df, p_col=dctc.PFPN, n_cost=vmc.cost):
    nc_sum_date = df.groupby([p_col, vmc.date])[n_cost].sum().reset_index()
    nc_sum_date.columns = [p_col] + NC_SUM_DATE_COL
    df = df.merge(nc_sum_date, on=[p_col, vmc.date], how='left')
    return df


def net_cost_final(df, p_col=dctc.PFPN, n_cost=vmc.cost):
    tdf = (df[df[NC_CUM_SUM] > df[DIF_PNC]].groupby([p_col]).
           min().reset_index())
    if not tdf.empty:
        tdf = tdf[[vmc.date, p_col]]
        tdf[NC_CUM_SUM_MIN_DATE] = True
        df = df.merge(tdf, on=[p_col, vmc.date], how='left')
        df[NCF] = np.where(df[NC_CUM_SUM] > df[DIF_PNC],
                           (np.where(df[NC_CUM_SUM_MIN_DATE] == True,
                                     (df[n_cost] -
                                      (df[n_cost] / (df[NC_SUM_DATE])) *
                                      (df[NC_CUM_SUM] - df[DIF_PNC])),
                                     0)),
                           df[vmc.cost])
    else:
        df[NC_CUM_SUM_MIN_DATE] = 0
        df[NCF] = df[n_cost]
    df = utl.col_removal(df, 'Raw Data', DROP_COL)
    return df


def net_cost_final_calculation(df, p_col=dctc.PFPN, n_cost=vmc.cost,
                               p_cost=dctc.PNC):
    logging.info('Calculating Net Cost Final')
    df = net_plan_comp(df, p_col=p_col, n_cost=n_cost, p_cost=p_cost)
    df = net_cum_sum(df, p_col=p_col, n_cost=n_cost)
    df = net_sum_date(df, p_col=p_col, n_cost=n_cost)
    df = net_cost_final(df, p_col=p_col, n_cost=n_cost)
    return df


def agency_fees_calculation(df):
    logging.info('Calculating Agency Fees')
    if dctc.AGF not in df.columns:
        logging.warning('Agency Fee Rates not in dict.  '
                        'Update dict and run again to calculate agency fees.')
        return df
    threshold = utl.import_read_csv(agency_fee_file, utl.config_path)
    df = utl.data_to_type(df, float_col=[NCF, dctc.AGF])
    if not df.empty and not threshold.empty:
        threshold = threshold[AGENCY_THRESH].fillna(0).astype(float).values[0]
        threshold = (df[NCF].sum() - threshold) / df[NCF].sum()
        df[dctc.AGF] = df[dctc.AGF] * threshold
    df[AGENCY_FEES] = df[dctc.AGF] * df[NCF]
    return df


def total_cost_calculation(df):
    logging.info('Calculating Total Cost')
    if AGENCY_FEES not in df.columns:
        logging.warning('Agency Fees not in dataframe.  '
                        'Update dict and run again to calculate total cost.')
        return df
    df = utl.data_to_type(df, float_col=[NCF, AGENCY_FEES, vmc.AD_COST,
                                         vmc.dcm_service_fee, vmc.REP_COST,
                                         vmc.VER_COST])
    df[TOTAL_COST] = df[NCF] + df[AGENCY_FEES]
    for col in [vmc.AD_COST, vmc.dcm_service_fee, vmc.REP_COST, vmc.VER_COST]:
        if col in df.columns:
            df[TOTAL_COST] += df[col]
    return df


class MetricCap(object):
    def __init__(self, config_file='config/cap_config.csv'):
        self.file_name = 'file_name'
        self.file_dim = 'file_dim'
        self.file_metric = 'file_metric'
        self.proc_dim = 'processor_dim'
        self.proc_metric = 'processor_metric'
        self.temp_metric = None
        self.config = utl.import_read_csv(config_file)
        self.config = self.config.to_dict(orient='index')

    def get_cap_file(self, c):
        pdf = utl.import_read_csv(c[self.file_name])
        p_dict = self.col_dict(c)
        pdf = pdf.rename(columns=p_dict)
        return pdf

    def col_dict(self, c):
        self.temp_metric = c[self.proc_metric] + ' - TEMP'
        p_dict = {c[self.file_dim]: c[self.proc_dim],
                  c[self.file_metric]: self.temp_metric}
        return p_dict

    def apply_cap(self, df, c):
        logging.info('Calculating metric cap from: '
                     '{}'.format(c[self.file_name]))
        pdf = self.get_cap_file(c)
        df = df.append(pdf)
        df = net_cost_final_calculation(df, p_col=c[self.proc_dim],
                                        p_cost=self.temp_metric)
        df = df[~df[dctc.FPN].isnull()]
        df[vmc.cost] = df[NCF]
        df = utl.col_removal(df, 'Raw Data', [self.temp_metric])
        return df

    def apply_all_caps(self, df):
        if self.config:
            for cfg in self.config:
                c = self.config[cfg]
                df = self.apply_cap(df, c)
        return df


def calculate_cost(df):
    if vmc.cost not in df.columns:
        df[vmc.cost] = 0
    df = net_cost_calculation(df)
    df = MetricCap().apply_all_caps(df)
    df = net_cost_final_calculation(df)
    df = agency_fees_calculation(df)
    df = total_cost_calculation(df)
    return df
