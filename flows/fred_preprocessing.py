from prefect import Flow

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


from tasks.fred_preprocessing import run_preprocess_fred
from flows.base import BaseFlow


with Flow(
    'fred_preprocessing',
    run_config=BaseFlow.get_local_run(),
    storage=BaseFlow.get_storage(flow_file='pred_preprocessing.py'),
) as flow:
    run_preprocess_fred()


if __name__ == '__main__':
    flow.register(project_name='quandlib', labels=['quandl'])
