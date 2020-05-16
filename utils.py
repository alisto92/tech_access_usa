import pandas as pd
from pandas.io.json import json_normalize
from google.cloud import storage, bigquery
import google.cloud.bigquery
from google.cloud.bigquery import job
import datetime
from datetime import timedelta
# from vv_schemas import vv_table_schemas
import time
import numpy as np

def upload_csv_from_df(bucket_name,bucket_folder,file_name,dataframe):
    client=storage.Client()
    bucket=client.get_bucket(bucket_name)
    blob = bucket.blob(bucket_folder+file_name)
    blob.upload_from_string(dataframe.to_csv(index=False,encoding='UTF-8',sep='|'),'text/csv')
    print(file_name,"uploaded to",bucket_name,'/',bucket_folder)

def load_data_gbq(dataset_id, table_name, bucket_name, bucket_folder, file_name, schema, autodetect=False, append=True, bad_records=1):
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    if autodetect==True:
        job_config.autodetect = True
    else:
        job_config.autodetect=False
        job_config.schema = schema
    job_config.skip_leading_rows = 1
    if append==True:
        job_config.write_disposition = job.WriteDisposition.WRITE_APPEND
    else:
        job_config.write_disposition = job.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = job.SourceFormat.CSV
    job_config.max_bad_records = bad_records
    job_config.allow_quoted_newlines=True
    job_config.ignore_unknown_values=True
    job_config.field_delimiter='|'
    job_config.encoding='UTF-8'

    uri = 'gs://'+ bucket_name + '/' + bucket_folder + file_name
    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table_name),
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print('Job finished.')

    destination_table = client.get_table(dataset_ref.table(table_name))
    print('Loaded {} rows.'.format(destination_table.num_rows))
    
def enforce_bq_schema(report_df, alias):
    schema = []
    float_fields = []
    int_fields = []
    str_fields = []
    date_fields = []
    timestamp_fields = []
    bool_fields = []
    for record in vv_table_schemas[alias]:
        if 'mode' not in record.keys():
            record['mode'] = 'NULLABLE'
        schema.append(bigquery.SchemaField(record['name'], record['type'], record['mode']))
        if record['type'] == 'FLOAT':
            float_fields.append(record['name'])
        elif record['type'] == 'STRING':
            str_fields.append(record['name'])              
        elif record['type'] == 'DATE':
            date_fields.append(record['name'])
        elif record['type'] == 'TIMESTAMP':
            timestamp_fields.append(record['name'])
        elif record['type'] == 'INTEGER':
            int_fields.append(record['name'])
        elif record['type'] == 'BOOLEAN':
            bool_fields.append(record['name'])
    column_list = [record['name'] for record in vv_table_schemas[alias]]
    response_columns = list(report_df.columns)
    
    missing_columns = list(set(column_list) - set(response_columns))
    for column in missing_columns:
        report_df[column] = np.nan
    
    for field in float_fields:
        try:
            report_df[field] = pd.to_numeric(report_df[field].astype(str).str.replace('%','').str.replace(',','').str.replace('$','').fillna(0.0)).astype(float)
        except Exception as ex:
            print(field,'float')
            print(ex)
            pass
        
    for field in int_fields:
        try:
            report_df[field] = pd.to_numeric(report_df[field].astype(str).str.replace('%','').str.replace(',','').str.replace('$','').replace('.0','').fillna(0)).astype(int)
        except Exception as ex:
            print(field,'int')
            print(ex)
            pass
    for field in str_fields:
        try:
            report_df[field] = report_df[field].fillna('').astype(str)
        except Exception as ex:
            print(field,'str')
            print(ex)
            pass
    for field in date_fields:
        try:
            report_df[field] = pd.to_datetime(report_df[field],errors='ignore').dt.date
        except Exception as ex:
            print(field,'date')
            print(ex)
            pass
    for field in timestamp_fields:
        try:
            #if alias == 'settlement_reports':
            #    report_df[field] = pd.to_datetime(report_df[field],format='%d.%m
            #else:
            report_df[field] = pd.to_datetime(report_df[field],errors='ignore').dt.strftime('%Y-%m-%d %H:%M:%S').fillna('').astype(str).replace('NaT','')
        except Exception as ex:
            print(field,'timestamp')
            print(ex)
            pass
        
    for field in bool_fields:
        try:
            if field in response_columns:
                d = {'true': True, 
                     'false': False, 
                     '': False, 
                     'True': True, 
                     'False': False, 
                     'N':False, 
                     'Y':True, 
                     'y':True, 
                     'n':True,
                      True:True,
                      False:False}
                report_df[field] = report_df[field].fillna(False).map(d)
            else:
                report_df[field] = False
        except Exception as ex:
            print(field,'bool')
            print(ex)
            pass
                
    report_df = report_df[column_list]
    
    return report_df, schema