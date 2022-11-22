"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - ends_with: Substring which is expected to be at the end of the string (default '')
Output:
    - Data in the form of dataframe with the indicator column showing if the condition is satisfied in that row string columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, ends_with='', columns=None):
        try:
            for i in range(len(columns)):
                data[columns[i]] = data[columns[i]].astype(str)
                data[columns[i] + '_valid_endswith'] = data[columns[i]].str.endswith(ends_with)

        except Exception as e:
            raise type(e)(e)

        return data

