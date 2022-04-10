from prefect import task, Flow
from prefect.run_configs import LocalRun
from prefect.storage import Git

import sys
from pathlib import Path # if you haven't already done so
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

# Additionally remove the current file's directory from sys.path
try:
    sys.path.remove(str(parent))
except ValueError: # Already removed
    pass


@task
def do_something(name: str):
    print('hello')
    print(name)


with Flow(
    'test_dummy_1',
    run_config=LocalRun(labels=['quandl'], env={'some_env': 'some_value'}),
    storage=Git(repo='bluewhale9981/quandlib-flows', flow_path='flows/dummy.py')
) as flow:
    do_something('Jones')


if __name__ == '__main__':
    flow.register(project_name='quandlib', labels=['quandl'])
