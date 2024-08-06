# syntax=docker/dockerfile:1
FROM apache/airflow:2.8.4 as base

USER root
# install java runtime 
# and install docker-compose as both are needed by 
# apache beam pipelines (launches docker containers to run JVM)
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get install -y docker-compose

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt

