"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of  columns (with vector values) which will be flattened
Output:
    - Data in the form of dataframe with the new columns with flattened vectors
"""
import numpy as np
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        try:
            for col in columns:
                data['flattened' + '_' + col] = data[col]

                for i in range(0, len(data[col])):
                    data[col][i] = np.array(data[col][i])

                    data['flattened' + '_' + col][i] = data[col][i].flatten('F')

        except Exception as e:
            raise type(e)(e)

        return data
