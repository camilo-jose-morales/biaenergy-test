# biaenergy-test

## Deploy app1:

```console
1. cd ./app1

2. docker build -t app1-image .

3. docker run -d --name app1-container -p 5000:5000 app1-image
```

## Deploy app2:

```console
1. cd ./app2

2. docker build -t app2-image .

3. docker run -d --name app2-container -p 80:80 app2-image
```