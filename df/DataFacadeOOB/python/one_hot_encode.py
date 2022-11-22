"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names which you want to apply one-hot encoding
    - drop_columns: Binary parameter to chose to drop the columns after encoding or not (True/False(default))
Output:
    - Data in the form of dataframe with the one-hot encoded columns
"""
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], drop_columns=False):

        try:
            encoder = OneHotEncoder(sparse=False, handle_unknown='ignore').fit(data[columns])

            encoded_cols = list(encoder.get_feature_names_out(columns))

            data[encoded_cols] = encoder.transform(data[columns])
            if drop_columns:
                data = data.drop(columns, axis=1)

        except Exception as e:
            raise type(e)(e)

        return data
