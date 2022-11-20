"""
Inputs:
    - data: Source data in the form of dataframe
    - column: Column name for which you want to handel outlier
    - lower_threshold: Value of the lower limit
    - upper_threshold: Value of the upper limit
    - fix_method: Method used to handle outliers when they are detected ('clip'/'remove'(default)/'invalidate')
Output:
    - Data in the form of dataframe with transformed column
"""
import numpy as np
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, column, lower_threshold, upper_threshold, fix_method='remove'):
        q1 = lower_threshold
        q2 = upper_threshold
        try:
            if fix_method == 'clip':
                data[column] = data[column].apply(lambda x: q2 if x > q2 else (q1 if x < q1 else x))

            elif fix_method == 'remove':
                data.drop(np.where((data[column] > q2) | (data[column] < q1))[0].tolist(), axis=0, inplace=True)
                data = data.reset_index().drop('index', axis=1)

            else:
                data[column] = data[column].apply(lambda x: np.nan if x > q2 else (np.nan if x < q1 else x))

        except Exception as e:
            raise ValueError(e)

        return data
