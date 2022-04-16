"""
This is the script to preprocess the data for FRED.
"""
from __future__ import absolute_import

import os
import pickle
import typing
import configparser

import prefect
import pandas as pd

import sys
from pathlib import Path  # if you haven't already done so
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

# Additionally remove the current file's directory from sys.path
try:
    sys.path.remove(str(parent))
except ValueError: # Already removed
    pass

from tasks.base import BaseHandler
from utils.storages import GoogleCloudStorage
from utils.slackbot import Slack


logger = prefect.context.get('logger')


class PreprocessingFredDataConfig:

    config_file: str
    recent_date: str
    first_date: str
    filter_before_date: str
    proxy_min_factor: float
    proxy_min_days: int
    update_realtime_proxy: bool

    def __init__(self):
        self.fields: typing.Dict[str, typing.Any] = {
            'update_realtime_proxy': bool,
            'recent_date': str,
            'first_date': str,
            'filter_before_date': str,
            'proxy_min_factor': float,
            'proxy_min_days': int,
        }
        self.__read_from_config_file()

    def __read_from_config_file(self):
        self.config_file: str = 'tasks/configs/preprocess_fred.cfg'
        self.config: configparser.ConfigParser = self.__get_config()
        name: str

        for name, value_type in self.fields.items():
            if value_type == str:
                setattr(self, name, self.config['default'][name])
            elif value_type == float:
                setattr(self, name, self.config.getfloat('default', name))
            elif value_type == int:
                setattr(self, name, self.config.getint('default', name))
            elif value_type == bool:
                setattr(self, name, self.config.getboolean('default', name))

    def __get_config(self) -> configparser.ConfigParser:
        assert self.config_file is not None
        config_file: str = self.config_file
        cwd: str = os.getcwd()

        if not os.path.isfile(config_file):
            raise Exception(f'Config file not found: {cwd}')

        config = configparser.ConfigParser()
        config.read(config_file)

        return config


class PreprocessingFredData(BaseHandler):

    def __init__(
        self,
    ) -> None:
        logger.info('Initialize.')

        # Get config.
        self.config = PreprocessingFredDataConfig()

        # Create storage.
        self.storage = GoogleCloudStorage(bucket_name=os.getenv('BUCKET_NAME'))

        # Create Slack instace to send messages.
        self.slack = Slack(title='FRED download')

        self.prestart()

    def __load_fred_raw(self) -> pd.DataFrame:
        """Load necessary data"""
        downloaded_file: str = self.__download_fred_raw()

        df: pd.DataFrame = pd.read_pickle(downloaded_file)  # type: ignore

        return df

    def __download_fred_raw(self) -> str:
        latest_blob_file: str = f'shared/data/fred/latest/fred.pkl'
        fred_raw_path: str = os.path.join('data', 'fred_raw.pkl')
        self.storage.download_a_file(source_blob_name=latest_blob_file, destination_file_name=fred_raw_path)

        return fred_raw_path

    def __process(
        self,
        fred_raw_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        1. Filter out certain data:
            A. data with date before 1990
            B. data that was discontinued
        2. create realtime_start proxy based on *recent* data.
        Also create last_time_proxy when another datapoint takes precedence
        3. Filter out anything except first report
        4. diff everything by code
        """
        logger.info('Start filter by date')
        logger.info(f'fred_raw_df: {fred_raw_df.shape}')

        # 1.A filter out data based on date
        fred_raw_df['realtime_start'] = pd.to_datetime(fred_raw_df['realtime_start'])
        fred_raw_df['date'] = pd.to_datetime(fred_raw_df['date'])
        logger.info(f'fred_raw_df: {fred_raw_df.shape}')

        # filter out data that doesn't start before cutoff date
        min_dates = fred_raw_df.groupby('code')['date'].min()
        mask = min_dates < self.config.first_date
        logger.info(
            f'{mask.sum()} started before '
            f'{self.config.first_date} {(~mask).sum()} started after'
        )
        min_date_codes = min_dates[mask].index
        fred_processed_df: pd.DataFrame = fred_raw_df[fred_raw_df.code.isin(min_date_codes)]

        # filter out data before cutoff date
        mask = fred_processed_df['date'] >= self.config.filter_before_date
        logger.info(
            f'filtering out {(~mask).sum()} '
            'records before filter_before_date, keeping '
            f'{mask.sum()} after'
        )
        fred_processed_df = fred_processed_df[fred_processed_df['date'] >= self.config.first_date]
        logger.info(f'fred_processed_df data shape: {fred_processed_df.shape}')

        # 1.B filter discontinued data
        max_dates = fred_raw_df.groupby('code')['realtime_start'].max()
        mask = max_dates > self.config.recent_date
        logger.info(
            f'{mask.sum()} had realtime_start after'
            f'recent_date {(~mask).sum()} ended before'
        )
        max_date_codes = max_dates[mask].index
        fred_processed_df = fred_processed_df[fred_processed_df.code.isin(max_date_codes)]

        # 2. Create realtime_start proxies.
        # get number of days from date to the report
        fred_processed_df.loc[:, 'days_to_report'] = fred_processed_df['realtime_start'] - fred_processed_df['date']
        fred_processed_df.loc[:, 'days_to_report'] = fred_processed_df['days_to_report'].dt.days

        # get 'next realtime start'
        fred_processed_df.loc[:, 'next_realtime_start'] = fred_processed_df.groupby(
            ['code', 'date'])['realtime_start'].shift(-1)

        # boolean if this was the first reported for that code, date
        fred_processed_df.loc[:, 'first_reported'] = fred_processed_df.groupby(
            ['code', 'date'])['realtime_start'].transform(
                lambda x: x == x.min()
        )

        # 3. filter out all except first reported
        fred_processed_df = fred_processed_df[fred_processed_df['first_reported']]
        days_to_report_proxies: pd.DataFrame = self.__get_days_to_report_proxies(fred_processed_df=fred_processed_df)

        # Map report proxy by code.
        fred_processed_df['days_to_report_proxy'] = fred_processed_df.code.map(days_to_report_proxies)
        min_factor = self.config.proxy_min_factor
        min_days = self.config.proxy_min_days
        mask = (
            fred_processed_df['days_to_report'] >
            (fred_processed_df['days_to_report_proxy'] * min_factor + min_days)
        ) & fred_processed_df['first_reported']

        logger.info(f'Updating: {mask.sum()} rows')

        # Duplicate realtime start column.
        fred_processed_df['realtime_start_est'] = fred_processed_df['realtime_start']
        fred_processed_df.loc[mask, 'realtime_start_est'] = fred_processed_df.loc[mask, 'date'] + \
            pd.to_timedelta(
                fred_processed_df.loc[mask, 'days_to_report_proxy'], unit='days'
            )

        # 4. diff everything
        fred_processed_df = fred_processed_df.sort_values('date')
        fred_processed_df['value'] = pd.to_numeric(fred_processed_df['value'], errors='coerce')
        fred_processed_df.index = range(fred_processed_df.shape[0])
        fred_processed_df['diffed_value'] = fred_processed_df.groupby('code')['value'].diff()

        # ceiling the realtime_start_est
        fred_processed_df['realtime_start_est'] = fred_processed_df['realtime_start_est'].dt.ceil('D')

        # For each fred_processed_dfing date create a dataframe with 'up to date' data
        #  based on realtime_start proxy and last_time_index
        fred_processed_df = fred_processed_df.pivot_table(
            index='realtime_start_est',
            columns='code',
            values=['diffed_value', 'value']
        )

        return fred_processed_df

    def __get_days_to_report_proxies(self, fred_processed_df: pd.DataFrame) -> pd.DataFrame:
        mask = (fred_processed_df['date'] > self.config.recent_date)
        days_to_report_proxies: pd.DataFrame = fred_processed_df[mask] \
            .groupby('code')['days_to_report'].quantile(.75)

        return days_to_report_proxies

    def __upload_processed_data(self, fred_processed_df: pd.DataFrame) -> None:
        # Save file to disk.
        local_file: str = os.path.join(
            'data',
            'fred_preprocessed.pkl',
        )
        blob_file: str = 'shared/data/fred/latest/fred_preprocessed.pkl'
        fred_processed_df.to_pickle(local_file)

        self.storage.upload_a_file(
            source_file=local_file,
            destination_blob=blob_file,
        )

    def run(self) -> None:
        logger.info('Start FRED preprocessing')
        fred_raw_df: pd.DataFrame = self.__load_fred_raw()
        fred_processed_df: pd.DataFrame = self.__process(fred_raw_df)

        logger.info('Finished processing')
        logger.info(f'fred_processed_df: {fred_processed_df.shape}')

        self.__upload_processed_data(fred_processed_df)


@task
def run_preprocess_fred():
    p = PreprocessingFredData()
    p.run()
