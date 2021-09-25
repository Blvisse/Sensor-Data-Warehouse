import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import sys
from tqdm import tqdm as tq
import logging 

logging.basicConfig(filename='../logs/store.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.DEBUG)
print("Done Loading libraries")
logging.info("Done loading libraries")

for i in tq(range(10),desc="Creating database connection"):
    
    try:

        # first step is to access the database through a database connection

        connection=mysql.connector.connect(
                    host='localhost',
                    user='root',
                    password='root',
                    database='sensor_datawarehouse'
        )
        logging.info("Connection Success")
        mycursor = connection.cursor()
    
    except Error as e:
        #if the connection fails safely exit the system
        print(e)
        print("Exiting...")
        logging.debug("Connection Failure...")
        logging.debug("Connection error {}".format(e))
        sys.exit(1)


print (connection)

# data=pd.read_csv('../data/station_summary.csv')
# print(data)

# data=data[:10]

# #insert data into the database using the csv file
# for row in data.itertuples():
#     print(row)
#     mycursor.execute(

#         '''
#         INSERT INTO summary_table (ID,flow_99,flow_max,flow_median,flow_total,n_obs)

#         VALUES(%s,%s,%s,%s,%s,%s);
        
        
#         ''',
#         (row.ID, row.flow_99, row.flow_max, row.flow_median, row.flow_total,row.n_obs)
#      )

# connection.commit()
# print ("successfully inserted data into the database")

data=pd.read_csv('../data/I80_stations.csv')
print(data)
data=data[:10]
data=data.fillna(-1)

for row in data.itertuples():
    print(row)
    mycursor.execute(

        '''
        INSERT INTO i80_stations(ID,Fwy,Dir,District,County,City,State_PM,Abs_PM,Latitude,Longitude,Length,Type,Lanes,Name,User_ID_1,User_ID_2,User_ID_3,User_ID_4)

        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        
        ''',
        (row.ID,row.Fwy,row.Dir,row.District,row.County,row.City,row.State_PM,row.Abs_PM,row.Latitude,row.Longitude,row.Length,row.Type,row.Lanes,row.Name,row.User_ID_1,row.User_ID_2,row.User_ID_3,row.User_ID_4)



    )
connection.commit()
print ("successfully inserted data into the database")

data=pd.read_csv('../data/I80_median.csv')
print(data)
data=data[:10]
data=data.fillna(-1)

for row in data.itertuples():
    print(row)
    mycursor.execute(

        '''
        INSERT INTO i80_stations(ID,Fwy,Dir,District,County,City,State_PM,Abs_PM,Latitude,Longitude,Length,Type,Lanes,Name,User_ID_1,User_ID_2,User_ID_3,User_ID_4)

        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        
        ''',
        (row.ID,row.Fwy,row.Dir,row.District,row.County,row.City,row.State_PM,row.Abs_PM,row.Latitude,row.Longitude,row.Length,row.Type,row.Lanes,row.Name,row.User_ID_1,row.User_ID_2,row.User_ID_3,row.User_ID_4)



    )
connection.commit()
print ("successfully inserted data into the database")



