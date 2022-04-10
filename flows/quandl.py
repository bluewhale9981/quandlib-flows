import os
from prefect import Flow, task
from prefect.run_configs import LocalRun
from prefect.storage import Git
# from prefect.schedules import CronSchedule

from prestart import pre_start

pre_start()

# from tasks.quandl_daily import run_quandl_daily


@task
def do_something():
    print('hello')


SLACK_URL: str = os.getenv('SLACK_URL')

if SLACK_URL is None:
    raise Exception('SLACK_URL is not set.')

def register_quandl_daily(run_time: str) -> None:
    run_config = LocalRun(
        env={
            'SLACK_URL': SLACK_URL,
            'RUN_TIME': run_time
        },
    )
    storage = Git(
        repo='bluewhale9981/quanlib-flows',
        flow_path='flows/quandl.py'
    )

    with Flow(
        f'quandlib_daily_{run_time}',
        run_config=run_config,
        storage=storage,
    ) as flow:
        # run_quandl_daily(run_time=run_time)
        do_something()

    flow.register(project_name='quandlib', labels=['quandl'])


if __name__ == '__main__':
    register_quandl_daily('230pm')
    register_quandl_daily('430am')
