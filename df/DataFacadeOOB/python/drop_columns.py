"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names which you want to delete
Output:
    - Data in the form of dataframe without the columns to be deleted
"""
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        data.drop(columns, axis=1, inplace=True)

        return data
