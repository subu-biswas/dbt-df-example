"""
Inputs:
    - data: Source data in the form of dataframe
Output:
    - Data in the form of dataframe without the duplicate rows
"""
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame):
        data.drop_duplicates(keep='first', inplace=True)

        return data
