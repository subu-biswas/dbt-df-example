"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names which you want to concatenate
    - delimiter: Delimiter to be used for concatenation (default ",")
Output:
    - Data in the form of dataframe with new concatenate column
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], delimiter=","):

        data['new_concatenated_col'] = data[columns].apply(lambda x: delimiter.join(x.dropna().astype(str).values),
                                                           axis=1)

        return data
