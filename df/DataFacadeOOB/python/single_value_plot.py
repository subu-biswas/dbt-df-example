"""
Inputs:
    - data: Source data in the form of dataframe
    - variable_name: Variable for which user want summary value
    - sql_filter: SQL queries for filter
    - single_value_name: Name of the single value user wants
    - value_type: Summary Statistic Type ("Count","Unique Count","Average","Maximum","Minimum","Median","Total")
Output:
    - Data in the form of dataframe with summary table and plot
"""
import traceback
import pandas as pd
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, variable_name, sql_filter, single_value_name='optional', value_type=None):
        value_type = value_type.strip()
        if single_value_name == 'optional':
            plot_name = value_type + ' Of ' + variable_name
        else:
            plot_name = single_value_name

        def data_filter(df: pd.DataFrame, sql):
            try:
                df = df.query(f'{sql}')
            except Exception as e:
                print("Malformed SQL. Filtering aborted")
                print(e)
            return df

        try:
            data = data_filter(data, sql_filter)
            if value_type == 'Count':
                value = data[variable_name].count()

            elif value_type == 'Unique Count':
                value = data[variable_name].nunique()

            elif value_type == 'Average':
                value = data[variable_name].mean().round(2)

            elif value_type == 'Maximum':
                value = data[variable_name].max()

            elif value_type == 'Minimum':
                value = data[variable_name].min()

            elif value_type == 'Median':
                value = data[variable_name].median()

            else:
                value = data[variable_name].sum()

            value = plot_name + ' Is ' + str(value)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return value
