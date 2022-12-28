
'''
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
import pandas as pd
from dft import df_plot

df = df_helper.get_table(parameter_name="input_table", parameter_display_name="Input Table", parameter_description="Input Raw Table")

column_x = df_helper.get_column(parameter_name="column_x",parameter_display_name="Column X", parameter_description="First Column")
column_y = df_helper.get_column(parameter_name="column_y",parameter_display_name="Column Y", parameter_description="Second Column")
#filter_column = df_helper.get_column(parameter_name="filter_column",parameter_display_name="Filter Column", parameter_description="Column to filter the data")
sql_filter = df_helper.get_string(parameter_name="sql_query", parameter_display_name="Type the filtering query", parameter_description="Sql query to filter the data")

def data_filter(df: pd.DataFrame, sql):
    try:
        df = df.query(f'{sql}')
    except Exception as e:
        print("Malformed SQL. Filtering aborted")
        print(e)
    return df


df = data_filter(df, sql_filter)

correlation = df[column_x].corr(df[column_y])

df_plot.single_value('The Correlation Between '+ column_x + ' & '+ column_y, correlation)

df_plot.scatter_chart('The Correlation Between ', column_x, column_y, df)

string = 'The Correlation Between '+ column_x + ' & '+ column_y+ ' is ' + str(correlation)

# Make sure to publish the data so that it become available in the UI or for other actions.
df_helper.publish(df)

