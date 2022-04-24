"""
This is the script to fetch data from QuanDL to local machine.
"""
from __future__ import absolute_import

import os
import typing
import configparser

import prefect
from prefect import task
import pandas as pd

from tasks.base import BaseHandler
from utils.storages import GoogleCloudStorage
from utils.slackbot import Slack


logger = prefect.context.get('logger')


class QuandlCombineRawTickers(BaseHandler):

    config_file: str = 'quandlib-flows/tasks/configs/quandl_combine_raw.cfg'

    def __init__(self) -> None:
        super().__init__()

        # Get config from file.
        self.config: configparser.ConfigParser = self.get_config()

        self.tickers: typing.List[str] = self.config['quandl']['tickers'].split('\n')

        # Create storage.
        self.storage = GoogleCloudStorage(bucket_name=os.getenv('BUCKET_NAME'))

        # Create Slack instace to send messages.
        self.slack = Slack(title='QuanDL combine raw tickers')

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
            logger.info(f'Download ticker: {ticker}')

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

    def __load_all_tickers(self) -> typing.List[pd.DataFrame]:
        all_ticker_list: typing.List[pd.DataFrame] = []
        ticker: str

        for ticker in self.tickers:
            try:
                latest_ticker_file: str = os.path.join(
                    'data',
                    'quandl',
                    'latest',
                    f'{ticker}.pkl',
                )
                latest_ticker_df: pd.DataFrame = pd.read_pickle(latest_ticker_file)
                all_ticker_list.append(latest_ticker_df)
            except Exception as ex:
                logger.error(ex)
                self.slack.send(message=f'Unable to load ticker: {ticker}')

        return all_ticker_list

    def __merge_all_tickers(self, all_ticker_list: typing.List[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(all_ticker_list)

    def __upload_raw_quandl(self, all_tickers_df: pd.DataFrame) -> None:
        today = self.get_today()
        local_path: str = os.path.join(
            'data',
            'quandl',
            'quandl_raw.pickle',
        )
        latest_blob: str = 'shared/data/quandl/raw.pickle'
        historical_blob: str = f'shared/data/quandl/archives/{today}/raw.pickle'

        logger.info(f'Upload ticker - shape: {all_tickers_df.shape}')

        # Save to file first.
        all_tickers_df.to_pickle(local_path)

        # Upload to cloud
        self.storage.upload_a_file(source_file=local_path, destination_blob=latest_blob)
        self.storage.upload_a_file(source_file=local_path, destination_blob=historical_blob)

    def run(self) -> None:
        self.slack.send('Start')

        # Download all latest tickers from Storage.
        self.__download_previous_quandl_latest()

        # Load all tickers which have been downloaded.
        all_ticker_list: typing.List[pd.DataFrame] = self.__load_all_tickers()

        # Merge all the tickers data.
        all_tickers_df: pd.DataFrame = self.__merge_all_tickers(all_ticker_list)

        # Upload to Storge.
        self.__upload_raw_quandl(all_tickers_df)

        self.slack.send('Finished')


@task
def run_quandl_combine_raw():
    handler = QuandlCombineRawTickers()
    handler.run()
