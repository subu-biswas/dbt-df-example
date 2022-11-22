"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - prefix: String to be added as prefix
Output:
    - Data in the form of dataframe with formatted string columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], prefix):
        try:
            for i in columns:
                data[i] = prefix + data[i]

        except Exception as e:
            raise type(e)(e)

        return data
