"""
Inputs:
    - data: Source data in the form of dataframe
    - axis: X-axis variable
    -number_of_class: Number of class user wants to specify
    - sql_filter: SQL queries for filter
    - plot_type: Frequency plot type ("Bar Chart","Line Chart","Histogram")
Output:
    - Data in the form of dataframe with summary table and plot
"""
import traceback
import pandas as pd
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, axis, sql_filter, number_of_class=5, plot_type=None):
        plot_type = plot_type.strip()

        def data_filter(df: pd.DataFrame, sql):
            try:
                df = df.query(f'{sql}')
            except Exception as e:
                print("Malformed SQL. Filtering aborted")
                print(e)
            return df
        try:
            data = data_filter(data, sql_filter)
            table = pd.DataFrame(data[axis].value_counts(bins=number_of_class, sort=False)).reset_index()
            table.columns = [axis, 'Frequency']
            table[axis] = table[axis].astype(str)
            if plot_type == "Bar Chart":
                df_plot.bar_chart('Frequency Distribution Of ' + axis, table.columns[0], table.columns[1], table)

            else:
                df_plot.line_chart('Frequency Distribution Of ' + axis, table.columns[0], table.columns[1], table)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return table
