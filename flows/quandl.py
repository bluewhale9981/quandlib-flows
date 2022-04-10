import os
from prefect import Flow, task
from prefect.run_configs import LocalRun
from prefect.storage import Git
# from prefect.schedules import CronSchedule

from prestart import pre_start

pre_start()

from tasks.quandl_daily import run_quandl_daily

RUN_TIME = '230pm'


SLACK_URL: str = os.getenv('SLACK_URL')

if SLACK_URL is None:
    raise Exception('SLACK_URL is not set.')


run_config = LocalRun(
    env={
        'SLACK_URL': SLACK_URL,
        'RUN_TIME': RUN_TIME
    },
)
storage = Git(
    repo='bluewhale9981/quanlib-flows',
    flow_path='flows/quandl.py'
)

with Flow(
    f'quandlib_daily_{RUN_TIME}',
    run_config=run_config,
    storage=storage,
) as flow:
    run_quandl_daily(run_time=RUN_TIME)

flow.register(project_name='quandlib', labels=['quandl'])
