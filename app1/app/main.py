import json
import os

import pandas as pd
import psycopg2
import requests
from envyaml import EnvYAML
from fastapi import FastAPI, File, Request, UploadFile
from sqlalchemy import create_engine

PARAMS = EnvYAML('params.yaml')


app = FastAPI()
conn = None

@app.get("/")
def Hello_world():
    return {'message': 'Hello world!'}


@app.post("/load")
async def upload_csv(csv_file: UploadFile = File(...)):
    # READ CSV
    df0 = pd.read_csv(csv_file.file)
    
    # TEST WITH LOCAL CSV
    df0 = pd.read_csv("postcodesgeo.csv")
    
    # REMOVE DUPLICATES FROM INPUT DATA
    df0 = df0.drop_duplicates(subset=['lat','lon'])
    
    # REMOVE EXISTING POINTS FROM POINTS TO BE PROCESSED
    max_id, existing = read_existing_records()
    df0 = df0[~df0.set_index(['lat','lon']).index.isin(existing)]
    df0 = df0.reset_index(drop=True)
    df0['id'] = df0.index + max_id + 1
    records = df0.to_dict('records')
    
    call_app2(records)

    return {'msg': f'reverse geocoding {len(records)} points to get their respective postcodes'}


@app.post("/upload_to_postgres")
async def upload_to_postgres(data: Request):
    # READ REQUEST FROM APP2
    req_info = await data.json()
    request = json.loads(req_info['data'])
    df = pd.DataFrame.from_dict(json.loads(request.json())['result'])

    # APPEND DF WITH POSTCODES TO POSTGRES
    conn_string = f"postgresql://{PARAMS['user']}:{PARAMS['password']}@{PARAMS['host']}/{PARAMS['dbname']}"
    db = create_engine(conn_string)
    conn = db.connect()
    df.to_sql('geo', con=conn, if_exists='append', index=False)
    
    return {}


def call_app2(records):
    # url = 'http://0.0.0.0:80/postcode'
    # myobj = {'data': json.dumps(records)}
    # try:
    #     x = requests.post(url, json = myobj, timeout=1)
    # except requests.exceptions.ReadTimeout:
    #     pass
    # print(f'sent {len(records)} records to app2')
    url = 'http://0.0.0.0:80/postcode'
    myobj = {'data': json.dumps(records)}
    x = requests.post(url, json = myobj, timeout=1)

    df = pd.DataFrame.from_dict(json.loads(x.json())['result'])
    


def read_existing_records():
    max_id = 0
    existing = []

    try:
        with psycopg2.connect(
            host=PARAMS['host'],
            dbname=PARAMS['dbname'],
            user=PARAMS['user'],
            password=PARAMS['password'],
            port=PARAMS['port']
        ) as conn:
            # CREATE THE TABLE IN POSTGRES IF NOT EXISTS
            with open('create_table.sql', 'r') as f:
                query = f.read()
            cur = conn.cursor()
            cur.execute(query)

            # GET THE CURRENT RECORDS IN POSTGRES SO THEY WON'T BE PROCESSED TWICE
            with open('select_current_records.sql', 'r') as f:
                query = f.read()
            cur = conn.cursor()
            cur.execute(query)
            for record in cur.fetchall():
                existing.append(record)

            # GET THE MAX ID FROM POSTGRES
            cur = conn.cursor()
            cur.execute('SELECT MAX(id) FROM geo')
            max_id = cur.fetchone()[0]
        
    except Exception as error:
        print(error)
    finally:
        if conn:
            conn.close()
    
    return max_id if max_id else 0, existing


