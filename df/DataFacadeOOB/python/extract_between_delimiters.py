"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - left_delimiter: Left Delimiter (default is " ")
    - right_delimiter: Right Delimiter (default is " ")

Output:
    - Data in the form of dataframe added with new extracted substring columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], left_delimiter=None, right_delimiter=None):
        def extract_between(s, first=left_delimiter, last=right_delimiter):
            try:
                start = s.index(first) + len(first)
                end = s.index(last, start)
                return s[start:end]

            except ValueError:
                return "None"

        for i in columns:
            data['extract' + '_' + i] = data[i].apply(extract_between)

        return data
