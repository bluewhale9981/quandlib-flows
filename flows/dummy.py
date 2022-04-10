from prefect import task, Flow
from prefect.run_configs import LocalRun
from prefect.storage import Git


@task
def do_something(name: str):
    print('hello')
    print(name)


with Flow(
    'test_dummy',
    run_config=LocalRun(labels=['quandl'], env={'some_env': 'some_value'}),
    storage=Git(repo='bluewhale9981/quandlib-flows', flow_path='flows/dummy.py')
) as flow:
    do_something('Jones')


flow.register(project_name='quandlib', labels=['quandl'])
