import mysql.connector
from mysql.connector import Error
from tqdm import tqdm as tq

'''
This script creates a SQL database 

'''

def create_db():
    '''
    This function creates a SQL database

    '''
    print("Creating database...")
    
    try:
        database=mysql.connector.connect(
            host='localhost',
            user='root',
            password='root'
        )

        print(database)

        mycursor=database.cursor()
        mycursor.execute("CREATE DATABASE sensor_datawarehouse")

        print("Done... \n")

        print("Displaying databases...")

        mycursor.execute("SHOW DATABASES")
        dbs=[]
        for x in mycursor:
            dbs.append(x)

        print(dbs)

        mycursor.close()
    except Error as e:
        print(e)

if __name__ == "__main__":

    create_db()