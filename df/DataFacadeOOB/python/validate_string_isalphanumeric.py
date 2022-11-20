"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
Output:
    - Data in the form of dataframe with the indicator column showing if the string is alphanumeric
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns=None):
        try:
            for i in range(len(columns)):
                data[columns[i]] = data[columns[i]].astype(str)
                data[columns[i] + '_valid_isalphanum'] = data[columns[i]].str.isalnum()

        except Exception as e:
            raise type(e)(e)

        return data
