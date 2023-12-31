import json
from concurrent.futures import ThreadPoolExecutor

import requests
import uvicorn
from fastapi import FastAPI, Request

app = FastAPI()


@app.get("/")
def helloworld():
    return {'message': 'Hello World'}


@app.post("/postcode")
async def upload_csv(data: Request):
    req_info = await data.json()
    records = json.loads(req_info['data'])
    data = get_postcodes(records)

    return json.dumps({'result': data})


def get_postcodes(records):
    def call_postcodes_api(data):
        print(data[0] * 100 / len(records))
        r = requests.post(url, headers=headers, json=data[1])
        return [
            {   
                'lat': x['query']['latitude'],
                'lon': x['query']['longitude'],
                'postcode': x['result'][0]['postcode']
            }
            if x['result']
            else {
                'lat': x['query']['latitude'],
                'lon': x['query']['longitude'],
                'postcode': None
            }
            for x in r.json()['result']
        ]

    url = 'https://api.postcodes.io/postcodes'
    headers = {'Content-Type': 'application/json'}
    data_list = get_chunks_of_data(records)

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(
            call_postcodes_api, x) for x in data_list]
    return [x for future in futures for x in future.result() if x]



def get_chunks_of_data(records):
    data_list = []
    chunks = [records[x:x+100] for x in range(0, len(records), 100)]
    for chunk in chunks:
        data_list.append(
            {"geolocations": [{
                "longitude": x['lon'],
                "latitude": x['lat']
            } for x in chunk]}
        )
    return list(enumerate(data_list))


# if __name__ == '__main__':
#     uvicorn.run(app, port=80, reload=True, server_header=False)