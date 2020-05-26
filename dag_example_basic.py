# -*- coding: utf-8 -*-
"""
Created on Tue May 19 21:30:53 2020

@author: Ercan KARACELIK
"""

import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "me",
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 19),
    "email": ["alert@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@daily",
	"catchup":False
}



def insert_mytable():
    """ Inserts datas into the database """

    insert_query = """
        	INSERT INTO public.my_table
	        SELECT * FROM source_schema.my_table limit 5;
        """

    pg_hook = PostgresHook(postgres_conn_id="postgre_dwh_test")
    pg_hook.run(insert_query)
    
    
    
    
with DAG("insert_mytable", default_args=default_args) as dag:
    
    Task_I = PythonOperator(
        task_id="insert_mytable", python_callable=insert_mytable
    )

        
        

