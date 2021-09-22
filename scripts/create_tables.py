'''
This script creates the tables required for the project

'''

import pandas as pd 
import sqlite3
import numpy as np 
import mysql.connector
from mysql.connector import Error
import sys
from tqdm import tqdm as tq

print("Done Loading libraries")


for i in tq(range(10),desc="Creating database connection"):

    try:
        connection=mysql.connector.connect(
                    host='localhost',
                    user='root',
                    password='root',
                    database='sensor_datawarehouse'
        )

        mycursor = connection.cursor()
    except Error as e:
        print(e)
        print("Exiting...")
        sys.exit(1)


print (connection)

# for x in mycursor:
#     print (x)

def create_stations_table():
    print("Accessing station tables ....")
    '''
    This functions creates tables for the i80 stations dataset

    
    '''
    for i in tq(range(10),desc="Creating query..."):
        query= '''

        CREATE TABLE IF NOT EXISTS stations ( 
            ID INT NOT NULL PRIMARY KEY,
            Fwy INT , 
            Dir VARCHAR(255), 
            District VARCHAR(255), 
            County VARCHAR(255), 
            City VARCHAR(255),
            STate_PM DOUBLE , 
            Abs_PM DOUBLE , 
            Latitude FLOAT, 
            Longitude FLOAT,
            Length DOUBLE, 
            Type VARCHAR(255), 
            Lanes VARCHAR(255), 
            Name VARCHAR(255), 
            User_ID_1 VARCHAR(255), 
            User_ID_2 VARCHAR(255),
            User_ID_3 VARCHAR(255), 
            User_ID_4 VARCHAR(255)
            
            )
        
        '''
    mycursor.execute(query)

    print("Successfully run query")

    print(mycursor)

    print("finished querying the database")


    for i in tq(range(100),desc="Displaying tables"):
        pass

    print("Confirming table has been created")
    mycursor.execute(" SHOW TABLES ")


    for x in mycursor:
        print (x)

    print("Accessing table description")

    mycursor.execute(" desc stations")

    for x in mycursor:
        print(x)
    
    mycursor.close()

if __name__ == "__main__":


    create_stations_table()