#Import create_engine, MetaData
from sqlalchemy import create_engine, MetaData
#Define Engine to MySQL
engine=create_engine('mysql+pymysql://digitalskola:D6GhCbaaiq8LlNy7@35.222.7.78/digitalskola')
#Test 
print(engine.table_names())