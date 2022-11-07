"""
Inputs:
    - data: Source data in the form of dataframe
    - axis: Primary variable of X-axis
    - legend: Secondary variable of X-axis
    - value: Y-axis variable
    - sql_filter: SQL queries for filter
    - value_type: Summary Statistic Type ("Count","Count Percentage","Unique Count","Average","Maximum","Minimum","Median","Total")
Output:
    - Data in the form of dataframe with summary table and plot
"""
import traceback
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
            'Total': 'sum'
        }

    def execute(self, data: pd.DataFrame, axis, legend, value, sql_filter, value_type=None):
        value_type = value_type.strip()

        def data_filter(df: pd.DataFrame, sql):
            try:
                df = df.query(f'{sql}')
            except Exception as e:
                print("Malformed SQL. Filtering aborted")
                print(e)
            return df

        def count_percentage(x):
            return 100 * x.count() / len(data[value])

        try:
            data = data_filter(data, sql_filter)
            operation_type = self.value_type_to_string.get(value_type)
            if operation_type == 'count_percentage':
                table = pd.pivot_table(data, values=value, index=[axis],
                                       columns=[legend], aggfunc=count_percentage).fillna(0).reset_index()

                df_plot.clubbed_histogram(value_type+' Of ' + value + ' For different' + legend + ' And ' + axis,
                                          table.columns[0], table.columns[1:], table)
            else:
                table = pd.pivot_table(data, values=value, index=[axis],
                                       columns=[legend], aggfunc=operation_type).fillna(0).reset_index()

                df_plot.clubbed_histogram(value_type + ' Of ' + value + ' For different' + legend + ' And ' + axis,
                                          table.columns[0], list(table.columns[1:]), table)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return table
