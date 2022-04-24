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


from tasks.quandl_combine_raw import run_quandl_combine_raw
from flows.base import BaseFlow


with Flow(
    'quandl_combine_raw',
    run_config=BaseFlow.get_local_run(),
    storage=BaseFlow.get_storage(flow_file='quandl_combine_raw.py'),
) as flow:
    run_quandl_combine_raw()


if __name__ == '__main__':
    flow.register(project_name='quandlib', labels=['quandl'])
