"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names which you want to duplicate
Output:
    - Data in the form of dataframe with the columns to be duplicated
"""
import pandas as pd

from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        for col in columns:
            data[col + '_' + 'duplicate'] = data[col]

        return data
