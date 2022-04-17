#import create_engine, MetaData , Pandas
from genericpath import exists
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, Boolean, inspect
import sqlalchemy
from sqlalchemy.schema import CreateSchema,DropSchema
import pandas as pd
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
engine_postgresql = create_engine('postgresql+psycopg2://postgres:12345@localhost/postgres')
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
#test print(data)
engine_mysql.dispose()
