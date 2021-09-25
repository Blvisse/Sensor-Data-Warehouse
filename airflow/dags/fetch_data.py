from airflow import DAG
from random import randint
from airflow.operators.python import PythonOperator, BranchPythonOperator 
from airflow.operators.bash import BashOperator
from datetime import datetime
import logging 
import pandas as pd

logging.basicConfig(filename='../logs/store.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.DEBUG)
print("Done Loading libraries")
logging.info("Done loading libraries")


def best_data(ti):
    # accuracies=ti.xcom_pull(task_ids=['data_get','data_get_2','data_get_3'])
    # best_accuracy=max(accuracies)
    # if(best_accuracy >8):
    #     return 'accurate'
    print("Datasets have been loaded into the system")


def store_summary_data():
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
    data=pd.read_csv('../../data/station_summary.csv')
    print(data)
    data=data[:10]
    data=data.fillna(-1)

    for row in data.itertuples():
        print(row)
        mycursor.execute(

        '''
        INSERT INTO summary_table (ID,flow_99,flow_max,flow_median,flow_total,n_obs)

        VALUES(%s,%s,%s,%s,%s,%s);
        
        
        ''',
        (row.ID, row.flow_99, row.flow_max, row.flow_median, row.flow_total,row.n_obs)
     )
    connection.commit()
    print ("successfully inserted data into the database")

def store_stations_data():
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
    data=pd.read_csv('../../data/I80_stations.csv')
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

    



with DAG("fetch_data_dag",start_date=datetime(2021,1,1),schedule_interval="@daily", catchup=False) as dag:

    fetch_data_1=PythonOperator(
        task_id="data_get",
        python_callable=store_stations_data,

    )
    fetch_data_2=PythonOperator(
        task_id="data_get_2",
        python_callable=store_summary_data,

    )
  

    best_data=BranchPythonOperator(
        task_id="select_model",
        python_callable=best_data
    )
    # accurate=BashOperator(
    #     task_id="accurate",
    #     bash_command="echo 'accurate'"
    # )
    # inacurate=BashOperator(
    #     task_id="inacurate",
    #     bash_command="echo 'inacurate'"
    # )

[fetch_data_1,fetch_data_2]>>best_data
# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.bash import BashOperator

# from random import randint
# from datetime import datetime

# def _choose_best_models(ti):
#     accuracies = ti.xcom_pull(task_ids=[
#         'training_model_A',
#         'training_model_B',
#         'training_model_C'
#     ])
#     best_accuracy = max(accuracies)
#     if (best_accuracy > 8):
#         return 'accurate'
#     return 'inaccurate'

# def _training_model():
#     return randint(1, 10)

# with DAG("fetch_dag", start_date=datetime(2021, 1, 1),
#     schedule_interval="@daily", catchup=False) as dag:

#         training_model_A = PythonOperator(
#             task_id="training_model_A",
#             python_callable=_training_model
#         )

#         training_model_B = PythonOperator(
#             task_id="training_model_B",
#             python_callable=_training_model
#         )

#         training_model_C = PythonOperator(
#             task_id="training_model_C",
#             python_callable=_training_model
#         )

#         choose_best_model = BranchPythonOperator(
#             task_id="choose_best_model",
#             python_callable=_choose_best_models
#         )

#         accurate = BashOperator(
#             task_id="accurate",
#             bash_command="echo 'accurate'"
#         )

#         inaccurate = BashOperator(
#             task_id="inaccurate",
#             bash_command="echo 'inaccurate'"
#         )

#         [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]
