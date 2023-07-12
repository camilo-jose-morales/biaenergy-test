import json
import os

import pandas as pd
import psycopg2
import requests
from envyaml import EnvYAML
from fastapi import BackgroundTasks, FastAPI, File, UploadFile
from sqlalchemy import create_engine

# ENVIRONMENT VARIABLES CONTAINED IN THE ./APP1/APP/PARAMS.YAML FILE
os.chdir('./app')
PARAMS = EnvYAML('params.yaml')


app = FastAPI()
conn = None

@app.get("/")
def Hello_world():
    return {'message': 'Hello world!'}


@app.post("/load_csv")
async def upload_csv(background_tasks: BackgroundTasks, csv_file: UploadFile = File(...)):
    # READ CSV
    try:
        dftotal = pd.read_csv(csv_file.file)
    except Exception as error:
        print({'error reading csv file': f'{error}'})
        return {'error reading csv file': f'{error}'}

    # TEST WITH LOCAL CSV
    # dftotal = pd.read_csv("postcodesgeo.csv")
    
    # REMOVE DUPLICATES FROM INPUT DATA
    df0 = dftotal.drop_duplicates(subset=['lat','lon'])
    
    # REMOVE EXISTING POINTS FROM POINTS TO BE PROCESSED
    existing = read_existing_records()
    df_to_api = df0[~df0.set_index(['lat','lon']).index.isin(existing)]

    records = df_to_api.to_dict('records')
    
    # ADD BACKGROUND TASK SO THE APP1 RETURNS A HTTP RESPONSE BEFORE PROCESSING THE DATA DUE TO THE LONG TIME IT TAKES
    background_tasks.add_task(process_data, records)
    return {
        'msg': f'reverse geocoding {len(records)} points to get their respective postcodes.',
        'total_received': len(dftotal),
        'duplicated_records_removed': len(dftotal) - len(df0),
        'existing_records_in_db_removed': len(df0) - len(df_to_api),
        'sent_to_api': len(records)
    }


def process_data(records):
    # RECEIVE RESPONSE FROM APP2
    df = call_app2(records)

    # FILTER SUCESSFUL REVERSE GEOCODING TO UPLOAD TO POSTGRES
    df1 = df[~df.postcode.isnull()]

    # ADDS ID COLUMN TO THE DF
    max_id = get_max_id()
    df1 = df1.reset_index(drop=True)
    df1['id'] = df1.index + max_id + 1

    # APPEND DF WITH POSTCODES TO POSTGRES
    try:
        conn_string = f"postgresql://{PARAMS['user']}:{PARAMS['password']}@{PARAMS['host']}/{PARAMS['dbname']}"
        db = create_engine(conn_string)
        conn = db.connect()
        df1.to_sql('geo', con=conn, if_exists='append', index=False)
    except Exception as error:
        print(error)
    finally:
        conn.close()

    # Notify user of mistakes in reverse geocoding
    dfnone = df[df.postcode.isnull()]
    write_notification(dfnone)


def call_app2(records):
    url = 'http://0.0.0.0:80/postcode'
    myobj = {'data': json.dumps(records)}

    x = requests.post(url, json = myobj)

    return pd.DataFrame.from_dict(json.loads(x.json())['result'])
    

def read_existing_records():
    existing = []
    try:
        with psycopg2.connect(
            host=PARAMS['host'],
            dbname=PARAMS['dbname'],
            user=PARAMS['user'],
            password=PARAMS['password'],
            port=PARAMS['port']
        ) as conn:
            with conn.cursor() as cur:
                # CREATE THE TABLE IN POSTGRES IF NOT EXISTS
                with open('create_table.sql', 'r') as f:
                    query = f.read()
                cur.execute(query)

                # GET THE CURRENT RECORDS IN POSTGRES SO THEY WON'T BE PROCESSED TWICE
                with open('select_current_records.sql', 'r') as f:
                    query = f.read()
                cur.execute(query)
                for record in cur.fetchall():
                    existing.append(record)
    except Exception as error:
        print(error)
    finally:
        if conn:
            conn.close()
    
    return existing


def get_max_id():
    max_id = 0
    try:
        with psycopg2.connect(
            host=PARAMS['host'],
            dbname=PARAMS['dbname'],
            user=PARAMS['user'],
            password=PARAMS['password'],
            port=PARAMS['port']
        ) as conn:
            with conn.cursor() as cur:
                # GET THE MAX ID FROM POSTGRES
                cur.execute('SELECT MAX(id) FROM geo')
                max_id = cur.fetchone()[0]
    except Exception as error:
        print(error)
    finally:
        if conn:
            conn.close()
    
    return max_id if max_id else 0


def write_notification(dfnone):
    content = ''
    for index, row in dfnone.iterrows():
        content += '\n' + f'point with latitude: {row.lat} and longitude: {row.lon} had an unexpected error when reverse geocoding with the (https://postcodes.io/) API'

    with open("notifications.txt", mode="w") as file:
        file.write(content)
    print(content)