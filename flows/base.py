import os
import typing

from prefect.storage import Git
from prefect.run_configs import LocalRun


SLACK_URL: typing.Optional[str] = os.getenv('SLACK_URL')
GOOGLE_APPLICATION_CREDENTIALS: typing.Optional[str] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
FRED_API_KEY: typing.Optional[str] = os.getenv('FRED_API_KEY')

if SLACK_URL is None:
    raise Exception('SLACK_URL is not set.')

if GOOGLE_APPLICATION_CREDENTIALS is None:
    raise Exception('GOOGLE_APPLICATION_CREDENTIALS is not set.')


class BaseFlow:
    @staticmethod
    def get_local_run() -> LocalRun:
        return LocalRun(
            labels=['quandl'],
            env={
                    'FRED_API_KEY': FRED_API_KEY,
                    'SLACK_URL': SLACK_URL,
                    'GOOGLE_APPLICATION_CREDENTIALS': GOOGLE_APPLICATION_CREDENTIALS,
                }
        )

    @staticmethod
    def get_storage(flow_file: str) -> Git:
        return Git(repo='bluewhale9981/quandlib-flows', flow_path=f'flows/{flow_file}')
