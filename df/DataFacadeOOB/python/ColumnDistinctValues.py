

from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, df, col):
        distinct_values = list(dict.fromkeys(df[col].tolist()))

        return {
            "DistinctValues": distinct_values
        }
