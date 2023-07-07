from fastapi import FastAPI, File, UploadFile
import pandas as pd
import psycopg2
import json
import requests

app = FastAPI()
conn = None
cur = None

@app.post("/load")
async def upload_csv(csv_file: UploadFile = File(...)):
    
    records = pd.read_csv(csv_file.file).to_dict('records')
    records = pd.read_csv("postcodesgeo.csv").to_dict('records')
    
    df = call_app2(records)

    # conn = build_pg_connection()
    # try:
        # df = pd.read_csv("postcodesgeo.csv")
    #     with open('create_table.sql', 'r') as f:
    #         query = f.read()
    
    #     cur = conn.cursor()
    #     cur.execute(query)

        
    
    #     conn.commit()
    # except Exception as error:
    #     print(error)
    # finally:
    #     if cur:
    #         cur.close()
    #     if conn:
    #         conn.close()
    
    return {}


def build_pg_connection():
    return psycopg2.connect(
        host= 'localhost',
        dbname= 'biaenergy',
        user= 'postgres',
        password= 'admin',
        port= 5432
    )


def call_app2(records):
    url = 'http://0.0.0.0:5000/postcode'
    myobj = {'data': json.dumps(records)}

    x = requests.post(url, json = myobj)

    return pd.DataFrame.from_dict(json.loads(x.json()['result']))