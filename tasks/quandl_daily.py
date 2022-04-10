from __future__ import absolute_import

import os
import time
import uuid
import typing
import shutil
import prefect
import configparser
from datetime import datetime

import quandl
import pandas as pd
from prefect import task

from utils.storages import GoogleCloudStorage
from utils.slackbot import Slack


logger = prefect.context.get('logger')


class QuandlPremium:

    def __init__(self):
        # Create slack instace.
        id = str(uuid.uuid4())
        self.slack = Slack(title=f'Quanld Daily - {id}')

        # Get config from file.
        self.config: configparser.ConfigParser = self.__get_config()
        api_key: str = self.config['quandl']['api_key']
        quandl.ApiConfig.api_key = api_key

        self.tickers: typing.List[str] = self.config['quandl']['tickers'].split('\n')

        self.storage = GoogleCloudStorage(bucket_name=self.config['storage']['bucket_name'])

    def __get_config(self) -> configparser.ConfigParser:
        self.run_time: str = os.getenv('RUN_TIME', '')
        config_file: str = f'quandlib-flows/tasks/configs/quandl_daily_{self.run_time}.cfg'
        cwd: str = os.getcwd()

        if not os.path.isfile(config_file):
            raise Exception(f'Config file not found: {cwd}')

        config = configparser.ConfigParser()
        config.read(config_file)

        return config

    def __pre_start(self) -> None:
        '''
        Setup the data folders and sync from GCS before starting.
        '''
        self.__remove_previous()

        self.__create_folder(os.path.join('data'))
        self.__create_folder(os.path.join('data', 'latest'))
        self.__create_folder(os.path.join('data', 'tickers'))
        self.__create_folder(os.path.join('data', 'merged'))

        self.__download_previous_latest()

    @staticmethod
    def __remove_previous() -> None:
        if os.path.isdir('data'):
            shutil.rmtree('data')

    @staticmethod
    def __create_folder(folder_path: str) -> None:
        if not os.path.isdir(folder_path):
            os.makedirs(folder_path)

    def __download_previous_latest(self) -> None:
        ticker: str

        for ticker in self.tickers:
            latest_ticker_blob = f'shared/data/quandl/latest/{ticker}.pkl'
            latest_ticker_local_path = os.path.join(
                'data',
                'latest',
                f'{ticker}.pkl',
            )
            if self.storage.is_file_exists(latest_ticker_blob):
                self.storage.download_a_file(
                    source_blob_name=latest_ticker_blob,
                    destination_file_name=latest_ticker_local_path,
                )

    def __download_all(self) -> typing.Dict[str, pd.DataFrame]:
        ticker: str
        tickers: typing.List[str] = self.tickers
        downloaded_data: typing.Dict[str, pd.DataFrame] = {}  # type: ignore
        failed_tickers: typing.List[str] = []

        for ticker in tickers:
            data: typing.Optional[pd.DataFrame] = self.__download_by_ticker(ticker=ticker)

            if data is not None:
                self.__save_ticker_data(ticker=ticker, data=data)
                downloaded_data[ticker] = data
            else:
                failed_tickers.append(ticker)

        logger.info(f'Failed Tickers: {failed_tickers}')

        return downloaded_data

    def __download_by_ticker(self, ticker: str) -> typing.Optional[pd.DataFrame]:
        prefix = self.config['quandl']['table_name']
        slice_method: str = self.config['quandl']['splice_code']
        start_date: str = self.config['default']['start_date']
        end_date: str = self.__get_end_date()
        data: pd.DataFrame

        try:
            logger.info(f'Getting ticker: {ticker}')
            data = quandl.get_table(
                prefix,
                date={
                    'gte': start_date,
                    'lte': end_date
                },
                quandl_code=f'{ticker}_{slice_method}',
            )
            logger.info(f'Done: {ticker} - {data.shape}')

            # Delay the call by a second.
            time.sleep(1)
        except Exception as ex:
            logger.info(ex)
            data = None

        return data

    def __save_ticker_data(self, ticker: str, data: pd.DataFrame) -> None:
        file_name: str = self.__get_ticker_file_name(ticker=ticker)
        # today: str = datetime.now().strftime('%Y%m%d')
        file_path: str = os.path.join('data', 'tickers', file_name)

        data.to_pickle(file_path)

    def __merge_with_the_latest(self, downloaded_data: typing.Dict[str, pd.DataFrame]):
        ticker: str
        data: pd.DataFrame

        for ticker, data in downloaded_data.items():
            # Load latest.
            latest_file_path: str = os.path.join(
                'data',
                'latest',
                f'{ticker}.pkl',
            )

            if os.path.isfile(latest_file_path):
                latest_data: pd.DataFrame = pd.read_pickle(latest_file_path)
                merged_data: pd.DataFrame = pd.concat([data, latest_data]) \
                    .drop_duplicates() \
                    .reset_index(drop=True)
                merged_file_path = os.path.join(
                    'data',
                    'merged',
                    f'{ticker}.pkl',
                )
                merged_data.to_pickle(merged_file_path)
                logger.info(f'Merged latest for ticker: {ticker} - {merged_data.shape}')
            else:
                # Upload the current file to latest.
                self.__write_to_latest(ticker=ticker, data=data)

    def __write_to_latest(self, ticker: str, data: pd.DataFrame) -> None:
        latest_file_path: str = os.path.join(
            'data',
            'latest',
            f'{ticker}.pkl',
        )
        data.to_pickle(latest_file_path)

    def __upload_latest_tickers(
        self,
        downloaded_data: typing.Dict[str, pd.DataFrame]
    ) -> None:
        for ticker, _ in downloaded_data.items():
            self.__upload_latest_ticker(ticker=ticker)

    def __upload_latest_ticker(self, ticker: str) -> None:
        local_file: str = os.path.join(
            'data',
            'latest',
            f'{ticker}.pkl',
        )
        blob_file: str = f'shared/data/quandl/latest/{ticker}.pkl'

        self.storage.upload_a_file(source_file=local_file, destination_blob=blob_file)
        logger.info(f'Uploaded: {blob_file}')

    def __upload_downloaded_tickers(
        self,
        downloaded_data: typing.Dict[str, pd.DataFrame]
    ) -> None:
        for ticker, _ in downloaded_data.items():
            self.__upload_downloaded_ticker(ticker=ticker)


    def __upload_downloaded_ticker(self, ticker: str) -> None:
        local_file: str = os.path.join(
            'data',
            'tickers',
            f'{ticker}.pkl',
        )
        today: str = self.__get_today()
        blob_file: str = f'shared/data/quandl/archives/{today}/{ticker}.pkl'

        self.storage.upload_a_file(source_file=local_file, destination_blob=blob_file)
        logger.info(f'Uploaded: {blob_file}')


    @staticmethod
    def __get_ticker_file_name(ticker: str) -> str:
        return f'{ticker}.pkl'

    def __get_end_date(self) -> str:
        '''
        Return today as format: YYYY-MM-DD.
        '''
        if 'end_date' not in self.config['default']:
            return datetime.now().strftime('%Y-%m-%d')
        return self.config['default']['end_date']

    def __get_today(self, output_format: typing.Optional[str] = None) -> str:
        if output_format is None:
            output_format = '%Y-%m-%d'

        if 'end_date' not in self.config['default']:
            return datetime.now().strftime(output_format)
        return self.config['default']['end_date']

    def run(self):
        self.slack.send(f'Start Quandl daily - run time: {self.run_time}')

        # Prepare folders and data.
        self.__pre_start()

        # Download newest data.
        downloaded_data = self.__download_all()

        # Upload downloaded files.
        self.__upload_downloaded_tickers(downloaded_data)

        # Merge with old data.
        self.__merge_with_the_latest(downloaded_data)

        # Upload latest tickers.
        self.__upload_latest_tickers(downloaded_data)

        logger.info('DONE!')
        self.slack.send(message='Task finished!')


def run_quandl_daily():
    quandl_daily = QuandlPremium()
    quandl_daily.run()
