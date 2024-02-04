import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.point import Point
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time 
import dask.dataframe as dd #use to run multiplr panda dataframes into singel instance

start_time = time.time()

geolocator = Nominatim(user_agent="otodomprojectanalysis")#take longititude and latitude and convert to dict
    
engine = create_engine(URL(
                    account = '-------',
                    user = '-------',
                    password = '------',
                    database = 'demo',
                    schema = 'public',
                    warehouse = 'demo_wh'))#create engine to connect python to snowflake
                
with engine.connect() as conn:
    try:
        query = """ SELECT RN, concat(latitude,',',longitude) as LOCATION
                    FROM (SELECT RN
                            , SUBSTR(location, REGEXP_INSTR(location,' ',1,4)+1) AS LATITUDE 
                            , SUBSTR(location, REGEXP_INSTR(location,' ',1,1)+1, (REGEXP_INSTR(location,' ',1,2) - REGEXP_INSTR(location,' ',1,1) - 1) ) AS LONGITUDE
                        FROM otodom_data_short_flatten WHERE rn between 1 and 1000
                        ORDER BY rn  ) """
        print("--- %s seconds ---" % (time.time() - start_time))
        
        df = pd.read_sql(query,conn)#load the query to pandas dataframe as df
                      
        df.columns = map(lambda x: str(x).upper(), df.columns)#convert column to uppercase because to_sql expects the column in uppercase
        
        ddf = dd.from_pandas(df,npartitions=10) #divide panda dataframe in 10 partitions for faster evaluation becuase its 10x faster
        print(ddf.head(5,npartitions=-1))
#create new column called address by passing location using api called geolocator.reverse and return dict with address
        ddf['ADDRESS'] = ddf['LOCATION'].apply(lambda x: geolocator.reverse(x).raw['address'],meta=(None, 'str'))
        print("--- %s seconds ---" % (time.time() - start_time))
#move back dask dataframe to pandas dataframe
        pandas_df = ddf.compute()
        print(pandas_df.head())
        print("--- %s seconds ---" % (time.time() - start_time))
#load pandas dataframe back to snowflake using to_sql and pd_write (uses copy into command)
        pandas_df.to_sql('otodom_data_flatten_address', con=engine, if_exists='append', index=False, chunksize=16000, method=pd_writer)
    except Exception as e:
        print('--- Error --- ',e)
    finally:
        conn.close()
engine.dispose()

print("--- %s seconds ---" % (time.time() - start_time))