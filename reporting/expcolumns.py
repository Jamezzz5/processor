export_key = 'Export_Key'
export_type = 'Export_Type'
output_file = 'Output_File'
config_file = 'Config_File'
translation_file = 'Translation_File'
schema_file = 'Schema_File'

db_config_file = 'dbconfig.json'

db_schema_file = 'dbschema.csv'
table = 'Table'
pk = 'PK'
columns = 'Columns'
fk = 'FK'
split_columns = [pk, columns, fk]
dirty_columns = {pk: ':', columns: ' ', fk: ':'}

db_translation_file = 'db_df_translation.csv'
translation_db = 'DB'
translation_df = 'DF'
translation_type = 'TYPE'

upload_id_file = 'upload_id_file.csv'
upload_tbl = 'upload'
upload_id_col = 'uploadid'
upload_data_ed = 'dataenddate'
upload_last_upload_date = 'lastuploaddate'
upload_name = 'uploadname'
upload_data_sd = 'datastartdate'

upload_cols = [upload_data_ed, upload_last_upload_date,
               upload_name, upload_data_sd]

event_date = 'eventdate'
upload_name_param = ['agencyname', 'clientname', 'productname', 'campaignname']

event_name = 'eventname'
plan_name = 'planname'
full_placement_name = 'fullplacementname'
