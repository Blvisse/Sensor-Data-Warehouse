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
import logging 

logging.basicConfig(filename='../logs/tables.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.DEBUG)
print("Done Loading libraries")
logging.info("Done loading libraries")



#stylish progress bar for aesthetics 
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

# for x in mycursor:
#     print (x)

def create_stations_table():
    #create statsions table
    '''
    This functions creates tables for the i80 stations dataset

    
    '''
    print("Accessing station tables ....")
    logging.debug("Creating station tables ....")
    
    for i in tq(range(10),desc="Creating query..."):
        logging.debug("loading query")
        
        #write an SQL query that will generate a new table 
        
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
    #execute query
    mycursor.execute(query)

    print("Successfully run query")
    logging.debug("Successfully run query")

    print(mycursor)

    print("finished querying the database")


    #confirm that the table has been created
    for i in tq(range(100),desc="Displaying tables"):
        pass

    print("Confirming table has been created")
    mycursor.execute(" SHOW TABLES ")


    #look at the table schema 
    for x in mycursor:
        print (x)

    print("Accessing table description")

    mycursor.execute(" desc stations")

    for x in mycursor:
        print(x)
    
    



def create_richards_table():
    #create a table for the richards dataset
    '''
    This function will create a table for the richards dataset
    It takes no arguments
    '''

    print("Accessing richards table...")

    logging.debug("Creating Richards Table...")

    for i in tq(range(10), desc="Generating query....."):
        logging.info("Generating query.....")

        query= '''

        CREATE TABLE IF NOT EXISTS richards (

            timestamp VARCHAR(255),
            flow1 INT,
            occupancy1 FLOAT,
            flow2 INT,
            occupancy2 FLOAT,
            flow3 INT,
            occupancy3 FLOAT,
            totalflow INT,
            weekday INT,
            hour INT,
            minute INT,
            second INT

        )

        '''
    
    #execute query using the cursor object
    try:
        mycursor.execute(query)
    except Error as e:
        print(e)
        print("Run into error exiting the system..")
        logging.debug("Query failed with error {}".format(e))
        sys.exit(1)

    print(" Successfully executed query")
    logging.debug("Successfully run query")

    #check the query run
    print(mycursor)

    for i in tq(range(100),desc="Dispalying tables .... "):
        pass
    print("Confirming tables has been created")
    mycursor.execute(" SHOW TABLES ")

    #check table schema 
    for x in mycursor:
        print(x)

    print("Accessing table description") 

    mycursor.execute("desc richards")

    for x in mycursor:
        print(x)

    #close db connection

    print("Done closing database connection....")
    
    # mycursor.close()


def create_stations_summary_table():
    '''
    This function creates a summary table for stations as per the stations_summary dataset
    '''
    print("Accessing stations summary tables...")
    logging.debug("Creating stations summary tables... ")

    #write the query to create the table
    for i in tq(range(10),desc="Generating query"):
        pass
    logging.debug("Generating query...")
    query= '''
    
    CREATE TABLE IF NOT EXISTS summary_table (
    ID INT NOT NULL PRIMARY KEY,
    flow_99 INT,
    flow_max INT,
    flow_median INT,
    flow_total INT,
    n_obs INT
    
    
    )
    '''

    #execute query
    mycursor.execute(query)

    print("Successfully run query")
    logging.debug("Successfully run query")

    print(mycursor) 
    
    #confirm that the table has been created 
    for i in tq(range(10),desc="Displaying tables"):
        pass

    
    print("Confirming table has been created")
    mycursor.execute(" SHOW TABLES ")

    for x in mycursor:
        print (x)

    #access table schema
    print("Accessing table description")

    mycursor.execute(" desc summary_table")

    for x in mycursor:
        print(x)
    
    print("Done closing database connection....")
    logging.debug(" Finished closing db connection")
    # mycursor.close()

def create_median_table():
    '''
    This creates a table based off the I80_median dataset 
    '''
    print("Accessing I80 median tables....")
    logging.debug("Creating table based on I80_median")

    for i in tq(range(10),desc="Generating query"):
        pass
    logging.debug("Generating query...")

    query='''

    CREATE TABLE IF NOT EXISTS i80_median (
        ID INT NOT NULL PRIMARY KEY,
        weekday INT,
        hour INT,
        minute INT,
        Second INT,
        flow1 INT,
        occupancy1 FLOAT,
        mph1 INT,
        flow2 INT,
        occupancy2 FLOAT,
        mph2 INT,
        flow3 INT,
        occupancy3 FLOAT,
        mph3 INT,
        flow4 INT,
        occupancy4 FLOAT,
        mph4 INT,
        flow5 INT,
        occupancy5 FLOAT,
        mph5 INT,
        totalflow DOUBLE


    )
    
    
    
    '''

    #execute query
    mycursor.execute(query)

    print("Successfully run query")
    logging.debug("Successfully run query")

    print(mycursor) 
    
    #confirm that the table has been created 
    for i in tq(range(10),desc="Displaying tables"):
        pass

    
    print("Confirming table has been created")
    mycursor.execute(" SHOW TABLES ")

    for x in mycursor:
        print (x)

    #access table schema
    print("Accessing table description")

    mycursor.execute(" desc i80_median")

    for x in mycursor:
        print(x)
    
    print("Done closing database connection....")
    logging.debug(" Finished closing db connection")
    # mycursor.close()


if __name__ == "__main__":


    # create_stations_table()
    create_richards_table()
    create_stations_summary_table()
    create_median_table()
    mycursor.close()