import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv('Raw Data Output.csv')
df = df[['Impressions', 'Clicks', 'mpVendor', 'mpCampaign']]

conn = create_engine('postgresql://masteruser:Masteruser1@examplecluster.cjbqou0u1s0b.us-east-2.redshift.amazonaws.com:5439/dev')
df.to_sql('df-test', conn, index=False, if_exists='replace')
ndf  = pd.read_sql_table('df-test', conn)
