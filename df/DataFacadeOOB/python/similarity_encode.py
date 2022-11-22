"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names which you want to apply similarity encoding
Output:
    - Data in the form of dataframe with the new columns consist of dense numerical vectors
"""
import pandas as pd
from dirty_cat import SimilarityEncoder
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        try:
            for col in columns:
                encoder = SimilarityEncoder(similarity='ngram')
                data['new' + '_' + col] = encoder.fit_transform(data[[col]]).tolist()

        except Exception as e:
            raise type(e)(e)

        return data
