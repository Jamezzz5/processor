import logging
import pandas as pd
from sqlalchemy import create_engine
import vmcolumns as vmc
import dictcolumns as dctc

log = logging.getLogger()


def export_to_rds():
    df = pd.read_csv('Raw Data Output.csv')
    df = df.loc[:, lambda df: vmc.vmkeys + dctc.COLS + dctc.PCOLS]
    """
    for col in df.columns:

    """
    logging.info('Writing to RDS')
    df.to_sql('df-test', conn, index=False, if_exists='replace')
    logging.info('Successfully wrote to RDS')
    logging.info('Reading from RDS')
    ndf  = pd.read_sql_table('df-test', conn)
    logging.info('Successfully read from RDS')
    ndf.to_csv('rds.csv')

def clean_for_export(df):
    df = df.loc[:, lambda df: vmc.vmkeys + dctc.COLS + dctc.PCOLS]
    """
    for col in df.columns:
        df[col]
    """