import os
import typing

from prefect import Flow
from prefect.run_configs import LocalRun
from prefect.storage import Git

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


SLACK_URL: typing.Optional[str] = os.getenv('SLACK_URL')
GOOGLE_APPLICATION_CREDENTIALS: typing.Optional[str] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

if SLACK_URL is None:
    raise Exception('SLACK_URL is not set.')

if GOOGLE_APPLICATION_CREDENTIALS is None:
    raise Exception('GOOGLE_APPLICATION_CREDENTIALS is not set.')


with Flow(
    'download_fred',
    run_config=LocalRun(
        labels=['quandl'],
        env={
                'SLACK_URL': SLACK_URL,
                'GOOGLE_APPLICATION_CREDENTIALS': GOOGLE_APPLICATION_CREDENTIALS,
            }
        ),
    storage=Git(repo='bluewhale9981/quandlib-flows', flow_path='flows/download_fred.py'),
) as flow:
    run_download_fred()


if __name__ == '__main__':
    flow.register(project_name='quandlib', labels=['quandl'])
