"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - delimiter: Delimiter based on which the column will be split (default value is ",")
Output:
    - Data in the form of dataframe with formatted string columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], delimiter=","):
        for col in columns:
            length = data[col].str.count(delimiter).max()
            cols = [col + '_' + str(i) for i in range(0, length + 1)]
            temp_df = data[col].str.split(',', expand=True)
            temp_df.columns = cols

            data = data.join(temp_df)

        return data
