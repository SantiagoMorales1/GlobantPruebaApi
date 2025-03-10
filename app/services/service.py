import csv
from os import replace
from pathlib import Path
import io
import numpy as np
import pandas as pd
import logging

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.identity import ManagedIdentityCredential

import pyodbc
from sqlalchemy import create_engine

from app.conn.conn import DRIVER, STORAGE_ACCOUNT, CONTAINER, SERVER, DATABASE, UID, PWD, ENV

#Conexion a Azure sotrage account

logger = logging.getLogger(__name__)

def get_conn_blob():

    #USING CON MANGMENT IDNETITY
    credential = DefaultAzureCredential()
    #credential = ManagedIdentityCredential()
    account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url, credential)
    
    return blob_service_client


def get_conn_sql_service(result=1):

    try:
      
        # Get Azure AD token
        if (ENV == 1):
            
            connString = f'Driver={DRIVER};Server={SERVER};Database={DATABASE};Authentication=ActiveDirectoryMsi;Encrypt=yes;'      
            conn = pyodbc.connect(connString)
          
        else:
            conn = pyodbc.connect(
                f"Driver={DRIVER};"
                f"Server={SERVER};"
                f"Database={DATABASE};"
                f"Uid={UID};"
                f"Pwd={PWD};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
        
        if result == 1:           
            conn.close()
            return {"status": "Connection OK"}            
        else:            
            return conn

    except Exception as e:        
        return {"error": f"Connection failed: {str(e)}"}


#Estructuras de archivos

COLUMN_MAPPINGS = {
    "departments.csv": ["id", "department"],
    "hired_employees.csv": ["id", "name", "datetime", "department_id", "job_id"],
    "jobs.csv": ["id", "job"],
}


#Leer localmente los archivos
def leer_csv_local(file_name: str):
    DATA_FOLDER = Path(__file__).parent.parent / "data"
    file_path = DATA_FOLDER / file_name
    
    if not file_path.exists():
        return {"error": f"Archivo {file_name} no encontrado"}

    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return list(reader)
 
#Leer de azur elos archivos
def leer_csv_azure (file_name: str, result=1):

    try:     
        blob_service_client = get_conn_blob()  
        container_client = blob_service_client.get_container_client(CONTAINER)
    
        blob_client = container_client.get_blob_client(file_name)
            
        if blob_client.exists():
            stream = blob_client.download_blob()
            csv_data = stream.readall()

            df = pd.read_csv(
                io.BytesIO(csv_data), 
                sep=",",
                encoding="utf-8",
                names= COLUMN_MAPPINGS.get(file_name, None)
            )

            df.replace({np.nan: None, np.inf: None, -np.inf: None}, inplace=True)

            print(f"Data CSV Leida para {file_name}")

            if result == 1:      
                return df.to_dict(orient="records")
            else:
                return df
        else: 
            if result == 1:      
                print("El archivo indicado no existe")
                return {"El archivo indicado no existe"}                
            else:
                print("El archivo indicado no existe")
                return pd.DataFrame()                   
                
                          
    except Exception as e:
        return {"error": str(e)}

def inferir_data_types(df, table_name):

    sql_types = []
    for col, dtype in df.dtypes.items():
        if "int" in str(dtype):
            sql_types.append(f"[{col}] INT")
        elif "float" in str(dtype):
            sql_types.append(f"[{col}] FLOAT")
        elif "bool" in str(dtype):
            sql_types.append(f"[{col}] BIT")
        else:
            sql_types.append(f"[{col}] NVARCHAR(255)")  # Default to text

    print(f"tipos de daots inferidos para {table_name}")    
    return ", ".join(sql_types)


def Valida_tabla (df, table_name):
    
    conn = get_conn_sql_service(0)
    cursor = conn.cursor()

    try:

        columns_def = inferir_data_types(df, table_name)

        create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
            BEGIN
                CREATE TABLE {table_name} (                    
                    {columns_def}
                )
            END
        """

        cursor.execute(create_table_sql)
        conn.commit()
        
        print(f"Tabla '{table_name}' creada o ya existe")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cursor.close()
        conn.close()


def Insertar_csv_azure (file_name: str):

    #Obtener la data
    df = leer_csv_azure (file_name, 0)
    
    if df.empty:
        return("El archivo indicado no existe")
    else:
        table_name = file_name.split(".")[0]
    
        #Valida tabla si no la crea
        #Valida_tabla(df, table_name)

        #Insertar los datos
        try:

            #columns = ", ".join([f"[{col}]" for col in df.columns])
            #values = ", ".join(["?" for _ in df.columns])
            #sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

            #conn = get_conn_sql_service(0)
            #cursor = conn.cursor()

            #data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
            #cursor.executemany(sql_query, data_tuples)

            #conn.commit()
            #print(f"{len(data_tuples)} Registros insertados")
        
            #cursor.execute(bulk_sql)
            #conn.commit()
                       
            engine = create_engine(f"mssql+pyodbc://{UID}:{PWD}@{SERVER}/{DATABASE}?driver=ODBC+Driver+18+for+SQL+Server")
            df.to_sql(table_name, engine, if_exists="append", index=False)
            inserted_rows = len(df)

            print (f"Registros insertados {inserted_rows}")
            return f"Registros insertados {inserted_rows}"
        
        except Exception as e:
            print(f"Error {e}")

        #finally:
            #cursor.close()
            #conn.close()


def get_view_hired_employees_by_quarter_service():
    
    conn = get_conn_sql_service(0)
    cursor = conn.cursor()
    
    query = "SELECT * FROM HIRED_EMPLOYES_PER_QUARTER ORDER BY department, job"
    cursor.execute(query)
    
    # Fetch all data
    columns = [column[0] for column in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()

    print("Query Ok")
    return data


def get_view_rank_employees_hired_by_mean_service ():
    
    conn = get_conn_sql_service(0)
    cursor = conn.cursor()
    
    query = "SELECT * FROM RANK_EMPLOYES_HIRED_MEAN ORDER BY hired DESC"
    cursor.execute(query)
    
    # Fetch all data
    columns = [column[0] for column in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()

    print("Query Ok")
    return data