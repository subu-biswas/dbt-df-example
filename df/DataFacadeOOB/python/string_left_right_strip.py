"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - characters: String specifying the characters to be removed
Output:
    - Data in the form of dataframe with formatted string columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], characters):
        try:
            for i in columns:
                data[i] = data[i].str.strip(characters)

        except Exception as e:
            type(e)(e)

        return data
