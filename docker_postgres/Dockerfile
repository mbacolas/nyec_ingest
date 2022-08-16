FROM postgres:latest

RUN apt-get update
COPY ./db_init/*.sql /docker-entrypoint-initdb.d/
