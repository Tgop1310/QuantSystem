"""
This file shows a sanitized snippet of the sector extraction, transform, and loading in the ETL process for portfolio purposes.

This code demonstrates:
1.  Data extraction from a financial data API.
2.  Complex multi-layer data transformationusing Pandas.
3.  Loading processed data into a PostgreSQL database.
4.  Architecture for moving data from PostgreSQL to Google Cloud Storage (GCS) and BigQuery.
5.  Running in airflow

Note: Connection strings, credentials, and specific project paths have been replaced with placeholders.
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import your_DataSource_api as api
import pandas as pd
from sqlalchemy import create_engine
import io
from sqlalchemy import create_engine, text
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import numpy as np
from datetime import date, timedelta, datetime
from airflow.hooks.base import BaseHook

#Connection string
c = BaseHook.get_connection("pipeline_postgres")
engine = create_engine(f"postgresql+psycopg2://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}")

def sector_screener_Update():

    #Defined this part in docker-composse.yaml file
    output_folder = r"/opt/airflow/output/"

    sector_list = ['basic-materials', 'communication-services', 'consumer-cyclical', 'consumer-defensive', 'energy', 'financial-services', 'healthcare', 'industrials', 
                'real-estate', 'technology', 'utilities']
    df_total_industries_info = pd.DataFrame()

    for sector in sector_list:
        sec = api.Sector(sector)
        df_top_companies = sec.top_companies.reset_index()
        df_top_companies.drop(columns=['name'], inplace=True)
        df_top_companies.rename(columns={"symbol": f"Top_Companies", "market weight": f"Market_Weight"}, inplace=True)
        df_etf = pd.DataFrame.from_dict(sec.top_etfs, orient='index').reset_index()
        df_etf.drop(columns=[0], inplace=True)
        df_etf.rename(columns={"index": f"Top_ETFs"}, inplace=True)
        df_mutual_funds = pd.DataFrame.from_dict(sec.top_mutual_funds, orient='index').reset_index()
        df_mutual_funds.drop(columns=[0], inplace=True)
        df_mutual_funds.rename(columns={"index": f"Top_Mutual_Funds"}, inplace=True)
        df_sector_info = pd.concat([df_top_companies, df_etf, df_mutual_funds], axis=1)

        df = sec.industries.reset_index()
        df.rename(columns={"market weight": f"Market_Weight"}, inplace=True)
        df["Sector"] = sector
        list_industries = df['key'].to_list()
        print(list_industries)
        df.drop(columns=['symbol', 'name'], inplace=True)
        df_total_industries_info = pd.concat([df_total_industries_info, df], axis=0)
        df_all_industry_info = pd.DataFrame()

        for industry in list_industries:
            ind = api.Industry(industry)
  
            try:
                df_top_companies = ind.top_performing_companies.reset_index()
                df_top_companies.drop(columns=['name'], inplace=True)
                df_top_companies.rename(columns={"symbol": f"TPC_Symbol_{industry}", "ytd return": f"YTD_Return_{industry}", 
                                                " last price": f"Last_Price_{industry}", "target price": f"Target_Price_{industry}"}, inplace=True)
                df_top_growth_companies = ind.top_growth_companies.reset_index()
                df_top_growth_companies.drop(columns=['name'], inplace=True)
                df_top_growth_companies.rename(columns={"symbol": f"TGC_Symbol_{industry}", "ytd return": f"YTD_Return_Growth_{industry}",
                                                        " growth estimate": f"Growth_Estimate_{industry}"}, inplace=True)
                df_top_growth_companies.columns = df_top_growth_companies.columns.str.replace('-', '_')
                df_top_companies.columns = df_top_companies.columns.str.replace('-', '_')
                df_industry_info = pd.concat([df_top_companies, df_top_growth_companies], axis=1)
                df_all_industry_info = pd.concat([df_all_industry_info, df_industry_info], axis=1)
            except Exception as e:
                print(f"Error processing industry {industry}: {e}")
                continue

        df_all_info = pd.concat([df_sector_info, df_all_industry_info], axis=1)
        name = f"{sector}_Screener.csv"
        output_file = os.path.join(output_folder, name)
        df_all_info.to_csv(os.path.join(output_folder, f"{sector}_Screener.csv"), index=False)
        df_all_info.to_sql(f"{sector}_Screener", con=engine, if_exists='replace', index=False, schema='your_schema')
    
    df_total_industries_info.to_sql(f"All_Industries_info", con=engine, if_exists='replace', index=False, schema='your_schema')





def daily_sector_screener_google_GCS_BigQuery_update():
    """
    To push data from PostgreSQL to Google Cloud Storage then to Google BigQuery
    """

    sector_list = ['basic-materials_Screener', 'communication-services_Screener', 'consumer-cyclical_Screener', 'consumer-defensive_Screener', 
                   'energy_Screener', 'financial-services_Screener', 'healthcare_Screener', 'industrials_Screener', 'real-estate_Screener', 
                   'technology_Screener', 'utilities_Screener', 'All_Industries_info']
    for sector in sector_list:

        # Configs
        GCS_BUCKET = "your_bucket_name"
        GCS_PATH = f"your_folder_name/{sector}.csv"
        BQ_TABLE_ID = f"your_proj.your_dataset.{sector}"

        # Path to service account JSON key
        SERVICE_ACCOUNT_FILE = r"path_to_your_json_key"

        # Load credentials
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)

        def export_postgres_to_csv():
            engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
            with engine.connect() as conn:
                result = conn.execute(text(f'select * from stock_db."{sector}"'))
                columns = result.keys()

                output = io.StringIO()
                output.write(",".join(columns) + "\n")
                for row in result:
                    output.write(",".join(str(item) if item is not None else "" for item in row) + "\n")
                output.seek(0)
                return output

        def upload_to_gcs(csv_buffer):
            client = storage.Client(credentials=credentials)
            bucket = client.bucket(GCS_BUCKET)
            blob = bucket.blob(GCS_PATH)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
            print(f"Uploaded CSV to gs://{GCS_BUCKET}/{GCS_PATH}")

        def load_into_bigquery():
            client = bigquery.Client(credentials=credentials)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE"
            )
            uri = f"gs://{GCS_BUCKET}/{GCS_PATH}"
            load_job = client.load_table_from_uri(uri, BQ_TABLE_ID, job_config=job_config)
            load_job.result()
            table = client.get_table(BQ_TABLE_ID)
            print(f"Loaded {table.num_rows} rows into {BQ_TABLE_ID}")


        csv_data = export_postgres_to_csv()
        upload_to_gcs(csv_data)
        load_into_bigquery()



with DAG(
    dag_id='Daily_Sector_Performance_Update',
    start_date=datetime(2024, 1, 1),
    schedule= '20 20 * * 1-5',
    catchup=False
) as dag:

    task_sector_screener_Update = PythonOperator(
        task_id = 'sector_screener_Update',
        python_callable= sector_screener_Update
    )

    task_sector_screener_google_GCS_BigQuery_update = PythonOperator(
        task_id='Daily_Google_GCS_BigQuery_Update',
        python_callable= daily_sector_screener_google_GCS_BigQuery_update
    )

task_sector_screener_Update >> task_sector_screener_google_GCS_BigQuery_update



