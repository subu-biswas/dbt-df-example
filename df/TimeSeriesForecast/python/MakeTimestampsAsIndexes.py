import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):

    def execute(self, df: pd.DataFrame, timestamp_column):
        df = df.set_index(timestamp_column)
        return df
