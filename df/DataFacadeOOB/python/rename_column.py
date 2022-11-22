"""
Inputs:
    - data: Source data in the form of dataframe
    - columns:  column name which you want to rename
    - new_name: new name for the column
Output:
    - Data in the form of dataframe with the renamed columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, column, new_name):
        data.rename({column: new_name}, axis=1, inplace=True)

        return data
