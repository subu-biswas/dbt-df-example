"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
    - delimiter: Delimiter specified by the user (default is '')
    - position: Specifying the position of the substring in the string with respect to the delimiter (before/after(default)
    - substring: Substring to be added between the delimiters

Output:
    - Data in the form of dataframe added with new added substring columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], substring, delimiter='', position='After'):
        def add_between(s, deli=delimiter, pos=position, string=substring):
            try:
                if pos == 'Before':
                    start = 0
                    end = s.index(deli, start)
                    return string + s[end:]
                elif pos == 'After':
                    start = s.index(deli) + len(deli)
                    return s[:start] + string

            except ValueError:
                return "None"

        for i in columns:
            data['new' + '_' + i] = data[i].apply(add_between)

        return data
