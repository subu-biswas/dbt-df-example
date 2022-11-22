"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - starts_with: Substring which is expected to be at the start of the string (default '')
Output:
    - Data in the form of dataframe with the indicator column showing if the condition is satisfied in that row string columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, starts_with='', columns=None):
        try:
            for i in range(len(columns)):
                data[columns[i]] = data[columns[i]].astype(str)
                data[columns[i] + '_valid_startswith'] = data[columns[i]].str.startswith(starts_with)

        except Exception as e:
            raise type(e)(e)

        return data
