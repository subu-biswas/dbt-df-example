
'''
each function will be wrapped inside a single class. It should have a self attribute
The execute function must have a pandas dataframe as a parameter which will be replaced by the table you select
For eg. the below code calculates the sum of a column in a table and multiplies it with a constant k
To plot charts you can use the df_plot class. The options for df_plot are:
 - df_plot.bar_chart(name, x, y, data)
 - df_plot.scatter_chart(name, x, y, data)
 - df_plot.pie_chart(name, legends, y, data)
 - df_plot.line_chart(name, x, y, data)
 - df_plot.single_value(name, value, variation=None)
 - df_plot.segment_line_chart(name, x, y, segments, data)

For eg to plot column1 against column2 as a line chart for dataframe df use:
- df_plot.line_chart("Chart Name", column1, column2, df)
'''
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, df, column, k):
        return df[column].sum()*k
