import os
import typing

from prefect import Flow
from prefect.run_configs import LocalRun
from prefect.storage import Git
from prefect.schedules import CronSchedule

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


from tasks.data_fetching.quandl_daily import run_quandl_daily


SLACK_URL: typing.Optional[str] = os.getenv('SLACK_URL')
GOOGLE_APPLICATION_CREDENTIALS: typing.Optional[str] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

if SLACK_URL is None:
    raise Exception('SLACK_URL is not set.')

if GOOGLE_APPLICATION_CREDENTIALS is None:
    raise Exception('GOOGLE_APPLICATION_CREDENTIALS is not set.')


with Flow(
    'quandl_daily_230pm',
    run_config=LocalRun(
        labels=['quandl'],
        env={
                'RUN_TIME': '230pm',
                'SLACK_URL': SLACK_URL,
                'GOOGLE_APPLICATION_CREDENTIALS': GOOGLE_APPLICATION_CREDENTIALS,
            }
        ),
    storage=Git(repo='bluewhale9981/quandlib-flows', flow_path='flows/quandl_daily_230pm.py'),
    schedule=CronSchedule('45 19 * * 1-5'),
) as flow:
    run_quandl_daily()


if __name__ == '__main__':
    flow.register(project_name='quandlib', labels=['quandl'])
