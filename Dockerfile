FROM ubuntu:18.04

RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install \
        curl \
        mongodb-clients \
        python-dev \
        python-setuptools \
        python-pip \
    && pip install tox \
    && mkdir /shardmonster

WORKDIR /shardmonster

COPY . .
