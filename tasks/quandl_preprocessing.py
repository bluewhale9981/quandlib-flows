"""
This is the script to fetch data from QuanDL to local machine.
"""
from __future__ import absolute_import

import os
import time
import typing
import configparser

import prefect
from prefect import task
import pandas as pd
from fredapi import Fred  # type: ignore

from tasks.base import BaseHandler
from utils.storages import GoogleCloudStorage
from utils.slackbot import Slack


logger = prefect.context.get('logger')


class DownloadFredData(BaseHandler):

    # config_file: str = 'quandlib-flows/tasks/configs/fred_download.cfg'
    config_file: str = 'configs/quandl_preprocessing.cfg'

    def __init__(self) -> None:
        super().__init__()

        # Get config from file.
        self.config: configparser.ConfigParser = self.get_config()

        self.tickers: typing.List[str] = self.config['quandl']['tickers'].split('\n')

        # Create storage.
        self.storage = GoogleCloudStorage(bucket_name=os.getenv('BUCKET_NAME'))

        # Create Slack instace to send messages.
        self.slack = Slack(title='FRED download')

        self.__pre_start()

    def __pre_start(self) -> None:
        self.remove_previous()

        self.create_folder(os.path.join('data'))
        self.create_folder(os.path.join('data', 'quandl'))
        self.create_folder(os.path.join('data', 'quandl', 'latest'))

        self.__download_previous_quandl_latest()



    def __download_previous_quandl_latest(self) -> None:
        ticker: str

        for ticker in self.tickers:
            latest_ticker_blob = f'shared/data/quandl/latest/{ticker}.pkl'
            latest_ticker_local_path = os.path.join(
                'data',
                'quandl',
                'latest',
                f'{ticker}.pkl',
            )
            if self.storage.is_file_exists(latest_ticker_blob):
                self.storage.download_a_file(
                    source_blob_name=latest_ticker_blob,
                    destination_file_name=latest_ticker_local_path,
                )