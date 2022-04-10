# FROM python:3.9.10

# RUN apt update

# RUN pip install --upgrade pip
FROM prefecthq/prefect:1.2.0-python3.9

RUN pip install google-cloud-storage \
    numpy \
    pandas \
    Quandl \
    requests \
    dulwich

ARG SLACK_URL

ENV SLACK_URL=$SLACK_URL

