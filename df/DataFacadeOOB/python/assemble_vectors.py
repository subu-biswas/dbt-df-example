"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of  columns (with vector values) which will be assembled
Output:
    - Data in the form of dataframe with the new assembled columns
"""
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        try:
            data['assembled_column'] = data[columns].values.tolist()
        except Exception as e:
            raise type(e)(e)

        return data
