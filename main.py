#import create_engine, MetaData , Pandas
from sqlalchemy import create_engine, MetaData
import pandas as pd
# #define Engine to MySQL
# engine_mysql=create_engine('mysql+pymysql://digitalskola:D6GhCbaaiq8LlNy7@35.222.7.78/digitalskola')
# #test print(engine_mysql.table_names())
# #create a connection engine
# connection_mysql=engine_mysql.connect()
# #loading a JSON in to table:
# json_file=pd.read_json('./data/data_covid.json')
# #test print(json_file)
# covid19_df=pd.DataFrame(json_file['data']['content'])
# #lower column name
# covid19_df.columns = [x.lower() for x in covid19_df.to_dict()]
# #test print(covid19_df)
# #load data json to mysql
# covid19_df.to_sql(name='ogi_raw_covid', con=connection_mysql, if_exists='replace',index=False)
# #test print(engine_mysql.table_names())
# #define engine to PostgreSQL
engine_postgresql = create_engine('postgresql+psycopg2://digitalskola:D6GhCbaaiq8LlNy7@35.193.53.27/digitalskola')
print(engine_postgresql.table_names())
connection_postgresql=engine_postgresql.connect()