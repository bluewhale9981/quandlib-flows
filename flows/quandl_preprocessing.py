from prefect import Flow

import sys
from pathlib import Path  # if you haven't already done so
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))


from tasks.preprocessing.quandl_preprocessing import run_quandl_preprocessing
from flows.base import BaseFlow


with Flow(
    'quandl_preprocessing',
    run_config=BaseFlow.get_local_run(),
    storage=BaseFlow.get_storage(flow_file='quandl_preprocessing.py'),
) as flow:
    run_quandl_preprocessing()


if __name__ == '__main__':
    flow.register(project_name='quandlib', labels=['quandl'])
