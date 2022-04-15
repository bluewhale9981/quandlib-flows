"""
This is the script to fetch data from FRED to local machine.
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


class FredDataDownload(BaseHandler):
    exceed_point_number_message: str = 'This exceeds the maximum number of vintage dates allowed'
    earliest_realtime_start = '2000-01-01'

    def __init__(self) -> None:
        logger.info('Init fred_download_data script')
        # Set config file path.
        self.config_file: str = 'tasks/configs/fred_download.cfg'
        self.__prestart()

        # Get the Fred API key from the environment
        self.api_key: typing.Optional[str] = os.getenv('FRED_API_KEY', default=None)

        assert self.api_key is not None

        # Initialize fred instance
        self.fred: Fred = Fred(api_key=self.api_key)

        # Create storage.
        self.storage = GoogleCloudStorage(bucket_name=self.config['storage']['bucket_name'])

        # Create Slack instace to send messages.
        self.slack = Slack(title='FRED download')

        self.fred_codes: typing.List[str] = self.config['fred']['fred_codes'].split('\n')


    def __prestart(self) -> None:
        self.config: configparser.ConfigParser = self.get_config()
        data_path = 'data'

        if not os.path.isdir(data_path):
            self.create_folder(data_path)

    def __download_all_fred_info(self) -> pd.DataFrame:
        """
        This is a private method for getting code info using fred instance.
        """
        df_code_info: pd.DataFrame

        code_info_list: typing.List[pd.Series[typing.Any]] = []
        bad_codes: typing.List[str] = []
        for code in self.fred_codes:
            logger.info(f'Getting fred code Info: {code}')
            try:
                code_info: typing.Optional[pd.Series[typing.Any]] = self.fred.get_series_info(code)  # type: ignore
                if code_info is not None:
                    code_info_list.append(code_info)
            except Exception as ex:
                logger.error(ex)
                bad_codes.append(code)
                logger.info(f'Code is failed to fetch: {code}')

            # Wait a bit for the next iteration.
            time.sleep(1)

        logger.info(f'Unable to download: {bad_codes}')

        # Convert the data to dataframe.
        df_code_info = pd.concat(code_info_list, axis=1).T  # type: ignore

        return df_code_info

    def __download_all_fred_codes(self) -> pd.DataFrame:
        """
        This is a private method to download the fred codes.
        """
        all_codes: pd.DataFrame
        data_series: typing.Dict[str, pd.DataFrame] = {}
        failed_codes: typing.Set[str] = set()
        i = 0

        for code in self.fred_codes:
            data = self.__download_fred_code(code=code)
            if data is not None:
                data_series[code] = data
            else:
                failed_codes.add(code)

            # logger about the progress.
            i = i + 1
            if i % 10 == 0:
                logger.info(f'fred code downloaded: {i}')

        logger.info(f"{str(len(failed_codes))} codes failed to download data")
        logger.info(failed_codes)

        for k, v in data_series.items():
            v.loc[:, "code"] = k
        all_codes = pd.concat(list(data_series.values()), axis=0)

        return all_codes

    def __download_fred_code(self, code: str) -> typing.Optional[pd.DataFrame]:
        data: typing.Optional[pd.DataFrame] = None

        logger.info(f"Getting FRED code Data: {code}")
        try:
            data = self.fred.get_series_all_releases(code, realtime_start=self.earliest_realtime_start)
        except Exception as ex:
            logger.error(f"Failed to download data: {code}")
            logger.error(ex)

            if self.exceed_point_number_message in str(ex):
                logger.info(f'Trying with multiple parts: {code}')
                data = self.__download_fred_code_by_part(code=code)

        return data

    def __download_fred_code_by_part(self, code: str) -> typing.Optional[pd.DataFrame]:
        '''
        Break the request into 2 or more to get over the exceed limit of data points.
        https://api.stlouisfed.org/fred/series/observations?series_id=BAMLH0A1HYBB&realtime_start=2000-01-01&realtime_end=9999-12-31&api_key=xxx
        '''
        break_point: str = '2020-01-01'
        data: typing.Optional[pd.DataFrame] = None

        try:
            part_one: pd.DataFrame = self.fred.get_series_all_releases(code, realtime_end=break_point)
            part_two: pd.DataFrame = self.fred.get_series_all_releases(code, realtime_start=break_point)
            data = pd.concat([part_one, part_two])

            logger.info(f'{code}: Getting by multipart OK!')
        except Exception as ex:
            logger.error(f'Unable to get ticker by part: {code}')
            logger.error(ex)

        return data

    @staticmethod
    def __validate_fred_data(df: pd.DataFrame) -> None:
        # It should have 4 columns.
        assert list(df.columns).sort() == [  # type: ignore
            'realtime_start', 'date', 'value', 'code'
        ].sort()

        # Check type of each columns
        assert str(df.dtypes.realtime_start) == 'datetime64[ns]'  # type: ignore
        assert str(df.dtypes.date) == 'datetime64[ns]'  # type: ignore
        assert str(df.dtypes.value) == 'object'  # type: ignore
        assert str(df.dtypes.code) == 'object'  # type: ignore

    @staticmethod
    def __validate_fred_info_data(df: pd.DataFrame) -> None:
        """
        Validate file `data/fred/download/fred_data_info.pkl`
        """
        required_columns = [
            'id',
            'realtime_start',
            'realtime_end',
            'title',
            'observation_start',
            'observation_end',
            'frequency',
            'frequency_short',
            'units',
            'units_short',
            'seasonal_adjustment',
            'seasonal_adjustment_short',
            'last_updated',
            'popularity',
            'notes',
        ]
        # Validate columns
        assert list(df.columns).sort() == required_columns.sort()  # type: ignore

        # Validate type of each columns
        for column in required_columns:
            assert str(df.dtypes[column]) == 'object'  # type: ignore

    def __upload_fred_data(self, df: pd.DataFrame) -> None:
        self.__upload(file_name='fred.pkl', df=df)

    def __upload_fred_info_data(self, df: pd.DataFrame) -> None:
        self.__upload(file_name='fred_info.pkl', df=df)

    def __upload(self, file_name: str, df: pd.DataFrame) -> None:
        local_file: str = os.path.join(
            'data',
            f'{file_name}',
        )
        latest_blob_file: str = f'shared/data/fred/latest/{file_name}'
        archive_blob_file: str = f'shared/data/fred/archives/{self.get_today}/{file_name}'

        # Save file to local first.
        df.to_pickle(local_file)

        # Upload to latest.
        self.storage.upload_a_file(
            source_file=local_file,
            destination_blob=latest_blob_file,
        )

        # Upload one for archive.
        self.storage.upload_a_file(
            source_file=local_file,
            destination_blob=archive_blob_file,
        )

    def run(self) -> None:
        """
        The main method to run.
        """
        logger.info('Start running the Fred download.')
        self.slack.send(f'Start with: {len(self.fred_codes)} codes')

        # Download FRED data.
        fred_df: pd.DataFrame = self.__download_all_fred_codes()
        self.__validate_fred_data(df=fred_df)
        self.__upload_fred_data(df=fred_df)

        # Download FRED info data.
        fred_info_df: pd.DataFrame = self.__download_all_fred_info()
        self.__validate_fred_info_data(df=fred_info_df)
        self.__upload_fred_info_data(df=fred_df)

        self.slack.send(f'Task finished!')


@task
def run_fred_download():
    fred = FredDataDownload()
    fred.run()
