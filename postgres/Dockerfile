FROM postgres:15

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=admin

COPY *.sql /docker-entrypoint-initdb.d/

