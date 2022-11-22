"""
Inputs:
    - data: Source data in the form of dataframe
    - column: Column name for which you want to handel outlier
    - value: String value to be replaced with the outliers detected
    - threshold_type: Threshold methods (Absolute threshold,Fraction threshold, Max common categories)
    - threshold_value: Threshold value (integer value for Absolute threshold, Max common categories and decimal value for Fraction threshold)
Output:
    - Data in the form of dataframe with transformed column
"""
import pandas as pd
import math
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, column, value, threshold_type=None, threshold_value=0):
        try:
            if threshold_type == 'Absolute threshold':
                if threshold_value.is_integer():
                    table = data[column].value_counts(sort=True)
                    table = pd.DataFrame(table[table <= threshold_value]).reset_index()
                    data[column] = data[column].apply(lambda x: value if sum(x == table['index']) >= 1 else x)

                else:
                    raise ValueError('The threshold_value must be integer')

            elif threshold_type == 'Fraction threshold':
                if (threshold_value > 0) and (threshold_value < 1):
                    table = data[column].value_counts(sort=True)
                    threshold_value = math.floor(threshold_value*len(data[column]))
                    table = pd.DataFrame(table[table <= threshold_value]).reset_index()
                    data[column] = data[column].apply(lambda x: value if sum(x == table['index']) >= 1 else x)

                else:
                    raise ValueError('The threshold_value must be between 0 and 1')

            elif threshold_type == 'Max common categories':
                if threshold_value.is_integer():
                    table = data[column].value_counts(sort=True)
                    table = pd.DataFrame(table[int(threshold_value)-1:]).reset_index()
                    data[column] = data[column].apply(lambda x: value if sum(x == table['index']) >= 1 else x)

                else:
                    raise ValueError('The threshold_value must be integer')

        except Exception as e:
            raise type(e)(e)

        return data
