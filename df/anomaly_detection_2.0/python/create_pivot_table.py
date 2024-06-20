import traceback
import numpy as np
import pandas as pd
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def __init__(self):
        self.value_type_to_string = {
            'Count': 'count',
            'Count Percentage': 'count_percentage',
            'Unique Count': 'nunique',
            'Average': 'mean',
            'Maximum': 'max',
            'Minimum': 'min',
            'Median': 'median',
            'Total': 'sum',
            '80 Percentile': 'percentile',
            '90 Percentile': 'percentile',
            '95 Percentile': 'percentile'
        }

    def execute(self, data: pd.DataFrame, rows, columns, values, sql_filter='optional', value_type=None):
        value_type = value_type.strip()

        def data_filter(df: pd.DataFrame, sql):
            try:
                df = df.query(f'{sql}')
            except Exception as e:
                print("Malformed SQL. Filtering aborted")
                print(e)
            return df

        def count_percentage(x):
            return 100 * x.count() / len(data[values])

        try:
            data = data_filter(data, sql_filter)
            operation_type = self.value_type_to_string.get(value_type)
            if operation_type == 'count_percentage':
                table = pd.pivot_table(data, values=values, index=[rows],
                                       columns=[columns], aggfunc=count_percentage).reset_index()

            elif operation_type == 'percentile':
                percentile = int(value_type[0:1])
                table = pd.pivot_table(data, values=values, index=[rows],
                                       columns=[columns], aggfunc=lambda x: np.percentile(x, percentile)).reset_index()

            else:
                table = pd.pivot_table(data, values=values, index=[rows],
                                       columns=[columns], aggfunc=operation_type).reset_index()

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return table