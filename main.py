#Import create_engine, MetaData , Pandas
from sqlalchemy import create_engine, MetaData
import pandas as pd
#Define Engine to MySQL
engine=create_engine('mysql+pymysql://digitalskola:D6GhCbaaiq8LlNy7@35.222.7.78/digitalskola')
#Test print(engine.table_names())
#Create a connection engine
connection=engine.connect()
#Loading a JSON in to table:
json_file=pd.read_json('./data/data_covid.json')
# test print(json_file)
covid19_df=pd.DataFrame(json_file['data']['content'])
# lower column name
covid19_df.columns = [x.lower() for x in covid19_df.to_dict()]
#test print(covid19_df)
#load data json to mysql
covid19_df.to_sql(name='ogi_raw_covid', con=connection, if_exists='replace',index=False)
print(engine.table_names())