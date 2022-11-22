"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of  columns (with vector/list values) which will be exploded
Output:
    - Data in the form of dataframe with the new exploded columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        try:
            for col in columns:
                data = data.explode(col)

            data = data.reset_index().drop('index', axis=1)

        except Exception as e:
            raise type(e)(e)

        return data
