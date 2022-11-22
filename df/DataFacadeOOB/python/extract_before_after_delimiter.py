"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - delimiter: Delimiter specified by the user (default is None)
    - position: Specifying the position of the substring in the string with respect to the delimiter (before/after(default)

Output:
    - Data in the form of dataframe added with new extracted substring columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], delimiter=None, position='After'):
        def extract(s, deli=delimiter, pos=position):
            try:
                if pos == 'Before':
                    start = 0
                    end = s.index(deli, start)
                    return s[start:end]
                elif pos == 'After':
                    start = s.index(deli) + len(deli)
                    end = len(s)
                    return s[start:end]

            except ValueError:
                return "None"

        for i in columns:
            data['extract' + '_' + i] = data[i].apply(extract)

        return data
