
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

df = df_helper.get_table(parameter_name="input_table", parameter_display_name="Input Table", parameter_description="Input Raw Table")
time_columns = df_helper.get_column(parameter_name="timestamp_column",parameter_display_name="Timestamp Column", parameter_description="Timestamp Column")
#
# Write your logic 
#

new_df = df.head(1000)

# Make sure to publish the data so that it become available in the UI or for other actions.
df_helper.publish(new_df)

