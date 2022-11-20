"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - width: Width of the field to pad the string value to
Output:
    - Data in the form of dataframe with formatted string columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], width):
        try:
            for i in columns:
                data[i] = data[i].str.pad(width, side='left', fillchar='0')
                if sum(data[i].str.len() > width) > 0:
                    data[i] = data[i].str.slice(start=-width)

        except Exception as e:
            type(e)(e)


        return data
