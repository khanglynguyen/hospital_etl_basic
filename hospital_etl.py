import pendulum
import pandas as pd
import re
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
default_args = {
    'owner':'airflow',
}
@dag(
    default_args= default_args,
    schedule=None,
    start_date=pendulum.datetime(2025,5,27, tz="UTC"),
    catchup=False,
    tags=['etl','hospital'],
)

def taskflow_hospital_etl():
    @task()
    def read_csv():
        #For csv with no headers
        #df_no_header = pd.read_csv("hospital_data_analysis.csv", header=None)
        #df_no_header.columns = ['name', 'age', 'date', 'sickness'] #Assign headers
        #For csv with headers
        df = pd.read_csv("patient_data.csv")
        return df

    @task()
    def transform_df(df_file):
        patientid_column = 'Patient_ID' # already unique due to being generated as the primary key from the original database
        name_columns = ['First_Name', 'Last_Name'] # normalise name format
        date_columns = ['Date_of_Birth', 'Admission_Date'] # change to standard format
        diag_column = 'Primary_Diagnosis' # normalise name format
        bloodp_column = 'Blood_Pressure' # check for standard format ab/cd and ab > cd
        bodytemp_column = 'Body_Temperature' # check for standard format ab.c
        premed_column = 'Prescribed_Medication' # normalise name format, compare with another database
        roomnum_column = 'Room_Number' # change to num and should be in format abc
        # diagnosis and medication should have a separate database to double check the information
        pharmacy = [
            {
                "category":"medicine",
                "id":"M0001",
                "name":"Paracetamol",
                "stock":1000
            },
            {
                "category":"medicine",
                "id":"M0002",
                "name":"Ibuprofen",
                "stock":500
            },
            {
                "category":"medicine",
                "id":"M0003",
                "name":"Amoxicillin",
                "stock":600
            },
            {
                "categpry":"medicine",
                "id":"M0004",
                "name":"Insulin",
                "stock":700
            },
            {
                "category":"medicine",
                "id":"M0005",
                "name":"Lisinopril",
                "stock":800
            }
        ]

        #name
        for names in name_columns:
            df_file[names] = df_file[names].capitalize()
        
        #date
        for dates in date_columns:
            df_file[dates] = pd.to_datetime(df_file[dates], infer_datetime_format = True)
        
        #diagnosis
        df_file[diag_column] = df_file[diag_column].capitalize()

        #medicine
        df_file[premed_column] = df_file[premed_column].capitalize()
        Medicine = [med["name"] for med in pharmacy if med["category"] == "medicine"]
        for med in df_file[premed_column]:
            if med in Medicine:
                print("Clear")
            else:
                print(f"the medcine's name is logged wrongly: {med}") # simple solution with print, could use other types of solution

        #bloodp
        for bloddpressure in df_file[bloodp_column]:
            if re.fullmatch(r"^d{3}/\d{2}$",bloddpressure) or re.fullmatch(r"^d{2}/\d{2}$",bloddpressure):
                print("Clear")
            else:
                print(f"blood pressure is logged wrongly: {bloddpressure}")
        
        #bodytemp
        #roomnum
        return df_file
    
    @task()
    def load_data(returned_df):
        table_name = "Patient_information"
        postgres_hook = PostgresHook(postgres_conn_id="hospital_connection")
        engine = postgres_hook.get_sqlalchemy_engine()

        returned_df.to_sql(
            table_name,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000
        )
    
    read_data = read_csv()
    transformed_data = transform_df(read_data)
    load_data(transformed_data)

hospital_etl = taskflow_hospital_etl()
