"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names which you want to apply ordinal encoding
Output:
    - Data in the form of dataframe with the ordinal encoded columns
"""
import numpy as np
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        from sklearn.preprocessing import OrdinalEncoder

        try:
            encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=np.nan).fit(data[columns])
            new_names = [column + '_' + 'ordinal_encode' for column in columns]
            data[new_names] = pd.DataFrame(encoder.transform(data[columns]))

        except Exception as e:
            raise type(e)(e)

        return data
