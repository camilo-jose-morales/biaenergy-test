# biaenergy-test

## Deploy proyect:

```console
# Create a Docker custom network so containers can communicate between each other

docker network create bianetwork

# Deploy postgres container
1. cd ./postgres
2. docker build -t postgres-image .
3. docker run -d --name postgres-container -p 8080:5432 --network bianetwork postgres-image

# Deploy App 1
1. cd ./app1
2. docker build -t app1-image .
3. docker run -d --name app1-container -p 8000:8000 --network bianetwork app1-image

# Deploy app 2
1. cd ./app2
2. docker build -t app2-image .
3. docker run -d --name app2-container -p 80:80 --network bianetwork app2-image
```