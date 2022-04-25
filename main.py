#import create_engine, MetaData , Pandas
from genericpath import exists
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, Boolean, inspect
import sqlalchemy
from sqlalchemy.schema import CreateSchema,DropSchema
import pandas as pd
import numpy as np
#define Engine to MySQL
engine_mysql=create_engine('mysql+pymysql://digitalskola:D6GhCbaaiq8LlNy7@35.222.7.78/digitalskola')
#test print(engine_mysql.table_names())
#create a connection engine
connection_mysql=engine_mysql.connect()
#loading a JSON in to table:
json_file=pd.read_json('./data/data_covid.json')
#test print(json_file)
covid19_df=pd.DataFrame(json_file['data']['content'])
#lower column name
covid19_df.columns = [x.lower() for x in covid19_df.to_dict()]
#test print(covid19_df)
#load data json to mysql
covid19_df.to_sql(name='ogi_raw_covid', con=connection_mysql, if_exists='replace',index=False)
engine_mysql.dispose()
#test print(engine_mysql.table_names())
#define engine to PostgreSQL
engine_postgresql = create_engine('postgresql+psycopg2://digitalskola:D6GhCbaaiq8LlNy7@35.193.53.27/digitalskola')
#create schema if not exist
engine_postgresql.execute('CREATE SCHEMA IF NOT EXISTS ogi')
metadata=MetaData(schema="ogi")
print(engine_postgresql.table_names(schema='ogi'))
connection_postgresql=engine_postgresql.connect()
#create dim table
dim_province=Table('dim_province',metadata,Column('province_id',String(255),primary_key=True),Column('province_name',String(255)))
dim_district=Table('dim_district',metadata,Column('district_id',String(255),primary_key=True),Column('province_id',String(255)),Column('district_name',String(255)))
#Serial Function
i = 0
def serial():
    global i
    i += 1
    return i
dim_case=Table('dim_case',metadata,Column('id',Integer,primary_key=True,default=serial),Column('status_name',String(255)),Column('status_detail',String(255)))
#create fact table
fact_province_daily=Table('fact_province_daily',metadata,Column('id',Integer, default=serial),Column('province_id',String(255)),Column('case_id',Integer),Column('date',String(255)),Column('total',Integer))
fact_province_monthly=Table('fact_province_monthly',metadata,Column('id',Integer, default=serial),Column('province_id',String(255)),Column('case_id',Integer),Column('date',String(255)),Column('total',Integer))
fact_province_yearly=Table('fact_province_yearly',metadata,Column('id',Integer, default=serial),Column('province_id',String(255)),Column('case_id',Integer),Column('date',String(255)),Column('total',Integer))
fact_district_monthly=Table('fact_district_monthly',metadata,Column('id',Integer, default=serial),Column('district_id',String(255)),Column('case_id',Integer),Column('date',String(255)),Column('total',Integer))
fact_district_yearly=Table('fact_district_yearly',metadata,Column('id',Integer, default=serial),Column('district_id',String(255)),Column('case_id',Integer),Column('date',String(255)),Column('total',Integer))
metadata.create_all(engine_postgresql)
# test print(engine_postgresql.table_names(schema='ogi'))
#read data from mysql
data=pd.read_sql(sql='ogi_raw_covid', con=connection_mysql)
def insert_dim_province(data):
    column_start = ["kode_prov", "nama_prov"]
    column_end = ["province_id", "province_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data


def insert_dim_district(data):
    column_start = ["kode_kab", "kode_prov", "nama_kab"]
    column_end = ["district_id", "province_id", "district_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data


def insert_dim_case(data):
    column_start = ["suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ["id", "status_name", "status_detail", "status"]

    data = data[column_start]
    data = data[:1]
    data = data.melt(var_name="status", value_name="total")
    data = data.drop_duplicates("status").sort_values("status")
    
    data['id'] = np.arange(1, data.shape[0]+1)
    data[['status_name', 'status_detail']] = data['status'].str.split('_', n=1, expand=True)
    data = data[column_end]

    return data


def insert_fact_province_daily(data, dim_case):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['date', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')
    
    data = data[['id', 'province_id', 'case_id', 'date', 'total']]
    
    return data


def insert_fact_province_monthly(data, dim_case):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['month', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'province_id', 'case_id', 'month', 'total']]
    
    return data


def insert_fact_province_yearly(data, dim_case):
    column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['year', 'province_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'province_id', 'case_id', 'year', 'total']]
    
    return data


def insert_fact_district_monthly(data, dim_case):
    column_start = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['month', 'district_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:7])
    data = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'district_id', 'case_id', 'month', 'total']]
    
    return data


def insert_fact_district_yearly(data, dim_case):
    column_start = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ['year', 'district_id', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['tanggal'] = data['tanggal'].apply(lambda x: x[:4])
    data = data.melt(id_vars=["tanggal", "kode_kab"], var_name="status", value_name="total").sort_values(["tanggal", "kode_kab", "status"])
    data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)
    
    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')

    data = data[['id', 'district_id', 'case_id', 'year', 'total']]
    
    return data

column = ["tanggal", "kode_prov", "nama_prov", "kode_kab", "nama_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
data = data[column]

dim_province = insert_dim_province(data)
dim_district = insert_dim_district(data)
dim_case = insert_dim_case(data)

fact_province_daily = insert_fact_province_daily(data, dim_case)
fact_province_monthly = insert_fact_province_monthly(data, dim_case)
fact_province_yearly = insert_fact_province_yearly(data, dim_case)
fact_district_monthly = insert_fact_district_monthly(data, dim_case)
fact_district_yearly = insert_fact_district_yearly(data, dim_case)

dim_province.to_sql('dim_province', metadata, con=connection_postgresql, index=False, if_exists='replace')
dim_district.to_sql('dim_district', metadata, con=connection_postgresql, index=False, if_exists='replace')
dim_case.to_sql('dim_case', metadata, con=connection_postgresql, index=False, if_exists='replace')

fact_province_daily.to_sql('fact_province_daily', metadata, con=connection_postgresql, index=False, if_exists='replace')
fact_province_monthly.to_sql('fact_province_monthly', metadata, con=connection_postgresql, index=False, if_exists='replace')
fact_province_yearly.to_sql('fact_province_yearly', metadata, con=connection_postgresql, index=False, if_exists='replace')
fact_district_monthly.to_sql('fact_district_monthly', metadata, con=connection_postgresql, index=False, if_exists='replace')
fact_district_yearly.to_sql('fact_district_yearly', metadata, con=connection_postgresql, index=False, if_exists='replace')

engine_postgresql.dispose()
