import os
import typing
import configparser
import shutil
from datetime import datetime


class BaseConfiguration:
    config_file: str
    field_map: typing.Dict[str, typing.Any]

class BaseHandler:
    config_file: str

    def prestart(self) -> None:
        data_path = 'data'

        if not os.path.isdir(data_path):
            self.create_folder(data_path)

    def get_config(self) -> configparser.ConfigParser:
        assert self.config_file is not None
        config_file: str = self.config_file
        cwd: str = os.getcwd()

        if not os.path.isfile(config_file):
            raise Exception(f'Config file not found: {cwd}')

        config = configparser.ConfigParser()
        config.read(config_file)

        return config

    def get_today(self, output_format: typing.Optional[str] = None) -> str:
        if output_format is None:
            output_format = '%Y-%m-%d'
        today: str

        try:
            today = self.config['default']['end_date']
        except:
            today = datetime.now().strftime(output_format)

        return today

    @staticmethod
    def get_config_key(config: configparser.ConfigParser, key: str, default: typing.Any) -> typing.Any:
        if key in config:
            return config[key]

        return default

    @staticmethod
    def remove_previous() -> None:
        if os.path.isdir('data'):
            shutil.rmtree('data')

    @staticmethod
    def create_folder(folder_path: str) -> None:
        if not os.path.isdir(folder_path):
            os.makedirs(folder_path)
