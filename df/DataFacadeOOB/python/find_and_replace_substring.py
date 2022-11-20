"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - find_str: String pattern to find
    - replace_str: String pattern to replace
Output:
    - Data in the form of dataframe with formatted string columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, find_str=" ", replace_str=" ", columns=None):
        try:
            if columns is None:
                data = data.replace(to_replace=find_str, value=replace_str, regex=True)
            else:
                for i in range(len(columns)):
                    data[columns[i] + '_replaced'] = data[columns[i]].replace(to_replace=find_str, value=replace_str,
                                                                              regex=True)

        except Exception as e:
            raise type(e)(e)

        return data
