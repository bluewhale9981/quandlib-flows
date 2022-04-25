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


class QuandlPreprocessing(BaseHandler):

    config_file: str = 'quandlib-flows/tasks/configs/quandl_preprocessing.cfg'
    # config_file: str = 'tasks/configs/quandl_preprocessing.cfg'

    def __init__(self) -> None:
        super().__init__()

        # Get config from file.
        self.config: configparser.ConfigParser = self.get_config()

        self.tickers: typing.List[str] = self.config['quandl']['tickers'].split('\n')

        # Create storage.
        self.storage = GoogleCloudStorage(bucket_name=os.getenv('BUCKET_NAME'))

        # Create Slack instace to send messages.
        self.slack = Slack(title='Quandl Preprocessing')

        self.__pre_start()

    def __pre_start(self) -> None:
        self.remove_previous()

        self.create_folder(os.path.join('data'))
        self.create_folder(os.path.join('data', 'quandl'))

        self.__download_previous_quandl_latest()

    def __download_previous_quandl_latest(self) -> str:
        quandl_latest_blob: str = 'shared/data/quandl/raw.pickle'
        quandl_raw_path: str = os.path.join(
            'data',
            'quandl',
            'raw.pickle',
        )
        self.storage.download_a_file(quandl_latest_blob, quandl_raw_path)

        return quandl_raw_path

    def __load_quandl_raw(self, quandl_raw_path: str) -> typing.Optional[pd.DataFrame]:
        quandl_raw: pd.DataFrame

        try:
            quandl_raw = pd.read_pickle(quandl_raw_path)
        except Exception as ex:
            logger.error('Unable to load QuanDL raw.')
            logger.error(ex)

        return quandl_raw

    @staticmethod
    def __create_new_columns(quandl_df: pd.DataFrame) -> pd.DataFrame:
        logger.info('Adding new columns')

        # Create year column.
        quandl_df['year'] = quandl_df['date'].dt.year

        # Create ticker column.
        quandl_df['ticker'] = quandl_df['exchange'] \
            + '_' \
            + quandl_df['symbol'] \
            + quandl_df['depth'].astype(str) \
            + '_' \
            + quandl_df['method']

        return quandl_df

    @staticmethod
    def __verify_data(quandl_df: pd.DataFrame):
        logger.info('Verify QuanDL premium data.')
        columns = [
            'exchange',
            'symbol',
            'depth',
            'method',
            'date',
            'open',
            'high',
            'low',
            'settle',
            'volume',
            'prev_day_open_interest',
            'year',
            'ticker',
        ]
        if set(quandl_df.columns) != set(columns):
            logger.error(quandl_df.columns)
            raise Exception('Missing some columns.')

    @staticmethod
    def __check_missing_data(data: pd.DataFrame):
        """
        We expected there is no NaN values on premium data.
        """
        logger.debug('Check missing data of QuanDL premium.')
        if data.isna().any().any():
            raise Exception('There is NaN value in data.')

    def __remove_unused_comlumns(self, quandl_df: pd.DataFrame) -> pd.DataFrame:
        unused_columns: typing.List[str] = self.config['quandl']['unused_columns'].split('\n')
        logger.info(f'Remove unused columns: {unused_columns}')

        quandl_df = quandl_df.drop(unused_columns, axis=1)

        return quandl_df

    def __upload_quandl_processed(self, quandl_df: pd.DataFrame):
        logger.info(f'Upload Quandl processed - shape: {quandl_df.shape}')

        today: str = self.get_today()
        local_path: str = os.path.join(
            'data',
            'quandl',
            'processed.pickle',
        )
        latest_blob: str = 'shared/data/quandl/processed.pickle'
        historical_blob: str = f'shared/data/quandl/archives/{today}/processed.pickle'

        # Save file to disk.
        quandl_df.to_pickle(local_path)

        # Upload to Storage.
        self.storage.upload_a_file(local_path, latest_blob)
        self.storage.upload_a_file(local_path, historical_blob)

    def run(self) -> None:
        self.slack.send('Start')

        quandl_df: pd.DataFrame
        is_success: bool = True

        quandl_raw_path = self.__download_previous_quandl_latest()
        quandl_raw: typing.Optional[pd.DataFrame] = self.__load_quandl_raw(quandl_raw_path)

        if quandl_raw is not None:
            logger.info(f'Quandl raw - shape: {quandl_raw.shape}')
            quandl_df = self.__remove_unused_comlumns(quandl_raw)
            quandl_df = self.__create_new_columns(quandl_df)
            self.__verify_data(quandl_df)
            self.__check_missing_data(quandl_df)
            self.__upload_quandl_processed(quandl_df)
        else:
            is_success = False

        self.slack.send(f'Finished - Success: {str(is_success)}')


@task
def run_quandl_preprocessing() -> None:
    handler = QuandlPreprocessing()
    handler.run()
