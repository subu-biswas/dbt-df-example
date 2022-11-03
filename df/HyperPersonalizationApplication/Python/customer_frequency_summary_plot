"""
Inputs:
    - data: Source data in the form of dataframe
    - axis: X-axis variable
    - legend: Secondary variable
    - number_of_class: Number of class user wants to specify
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
    def execute(self, data: pd.DataFrame, axis, legend, sql_filter, number_of_class=5):

        def data_filter(df: pd.DataFrame, sql):
            try:
                df = df.query(f'{sql}')
            except Exception as e:
                print("Malformed SQL. Filtering aborted")
                print(e)
            return df

        def value_to_interval(x, classes, interval_column):
            for i in range(classes):
                if x in interval_column[i]:
                    y = interval_column[i]
                else:
                    pass
            return y

        try:
            data = data_filter(data, sql_filter)
            table = pd.DataFrame(data[axis].value_counts(bins=number_of_class, sort=False)).reset_index()
            data[axis] = data[axis].apply(value_to_interval, args=(number_of_class, table['index']))
            data[axis] = data[axis].astype(str)
            table = pd.pivot_table(data, index=axis, values=data.columns[0],
                                   columns=[legend], aggfunc='count').fillna(0).reset_index()
            table[table.columns[0]] = table[table.columns[0]].astype(str)
            df_plot.clubbed_histogram('Frequency Distribution' + ' Of ' + axis + ' For different' + legend,
                                      table.columns[0], list(table.columns[1:]), table)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise
        return table
