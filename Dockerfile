FROM python:3.9.10

RUN apt update

RUN pip install --upgrade pip

WORKDIR /app

ADD requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

ADD . /app

ARG SLACK_URL

ENV SLACK_URL=$SLACK_URL
