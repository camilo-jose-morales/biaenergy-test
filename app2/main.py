from fastapi import FastAPI, Request
import uvicorn
import json
import requests

app2 = FastAPI()


@app2.post("/postcode")
async def upload_csv(data: Request):
    req_info = await data.json()
    records = json.loads(req_info['data'])
    data = call_postcodes(records)

    return {'result': json.dumps(data)}


def call_postcodes(records):
    url = 'https://api.postcodes.io/postcodes'
    headers = {'Content-Type': 'application/json'}
    chunks = [records[x:x+100] for x in range(0, len(records), 100)]
    answer = []
    progress = 0

    for chunk in chunks:
        print(progress/len(records) * 100)     
        data = {"geolocations": [{
            "longitude": x['lon'],
            "latitude": x['lat']
        } for x in chunk]}

        r = requests.post(url, headers=headers, json=data)
        answer += [
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
        progress+=len(chunk)
        
    return answer


if __name__ == '__main__':
    uvicorn.run("main:app2", host='0.0.0.0', port=5000, reload=True, server_header=False)