import os
import typing

from prefect import Flow
from prefect.run_configs import LocalRun
from prefect.storage import Git
from dotenv import load_dotenv

load_dotenv('.env')

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


from tasks.download_fred import run_download_fred
from tasks.preprocess_fred import run_preprocess_fred


SLACK_URL: typing.Optional[str] = os.getenv('SLACK_URL')
GOOGLE_APPLICATION_CREDENTIALS: typing.Optional[str] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
FRED_API_KEY: typing.Optional[str] = os.getenv('FRED_API_KEY')

if SLACK_URL is None:
    raise Exception('SLACK_URL is not set.')

if GOOGLE_APPLICATION_CREDENTIALS is None:
    raise Exception('GOOGLE_APPLICATION_CREDENTIALS is not set.')


base_env = {
    'FRED_API_KEY': FRED_API_KEY,
    'SLACK_URL': SLACK_URL,
    'GOOGLE_APPLICATION_CREDENTIALS': GOOGLE_APPLICATION_CREDENTIALS,
}

with Flow(
    'fred_download',
    run_config=LocalRun(
        labels=['quandl'],
        env=base_env,
    ),
    storage=Git(repo='bluewhale9981/quandlib-flows', flow_path='flows/main.py'),
) as flow_download_fred:
    run_download_fred()


with Flow(
    'fred_preprocessing',
    run_config=LocalRun(
        labels=['quandl'],
        env=base_env,
    ),
    storage=Git(repo='bluewhale9981/quandlib-flows', flow_path='flows/main.py'),
) as flow_preprocess_fred:
    run_preprocess_fred()


if __name__ == '__main__':
    flow_download_fred.register(project_name='quandlib', labels=['quandl'])
    flow_preprocess_fred.register(project_name='quandlib', labels=['quandl'])
