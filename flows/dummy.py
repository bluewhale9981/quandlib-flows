from prefect import task, Flow
from prefect.run_configs import LocalRun
from prefect.storage import Git


@task
def do_something():
    print('hello')


flow = Flow(
    'test_dummy',
    run_config=LocalRun(labels=['quandl']),
    tasks=[do_something],
    storage=Git(repo='bluewhale9981/quandlib-flows', flow_path='flows/dummy.py')
)


flow.register(project_name='quandlib', labels=['quandl'])
