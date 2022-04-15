import os
import typing
import configparser
import shutil


class BaseHandler:
    config_file: str

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

        if 'end_date' not in self.config['default']:
            return datetime.now().strftime(output_format)
        return self.config['default']['end_date']

    @staticmethod
    def remove_previous() -> None:
        if os.path.isdir('data'):
            shutil.rmtree('data')

    @staticmethod
    def create_folder(folder_path: str) -> None:
        if not os.path.isdir(folder_path):
            os.makedirs(folder_path)
