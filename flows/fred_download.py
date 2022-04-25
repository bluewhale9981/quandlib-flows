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


from tasks.data_fetching.fred_download import run_download_fred
from flows.base import BaseFlow


with Flow(
    'fred_download',
    run_config=BaseFlow.get_local_run(),
    storage=BaseFlow.get_storage(flow_file='fred_download.py'),
) as flow:
    run_download_fred()


if __name__ == '__main__':
    flow.register(project_name='quandlib', labels=['quandl'])
