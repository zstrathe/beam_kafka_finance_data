# syntax=docker/dockerfile:1
FROM apache/airflow:2.8.4 as base

USER root

# install java runtime for apache beam
# first dl software-properties-common to get add-apt-repository,
# then add debian unstable source list
# then install openjdk-11 
# alternatively "apt-get install -y default-jre" might work just as well, it just installs 
# Java17 and I'm testing if Java11 fixes a problem...
RUN apt-get update && \
    apt-get -y --no-install-recommends install software-properties-common && \
    add-apt-repository "deb http://httpredir.debian.org/debian sid main" && \
    apt-get update && \
    apt-get install -y openjdk-11-jdk

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt


