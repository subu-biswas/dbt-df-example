df_helper.install(['pandas']) 
import pandas as pd
import numpy as np

df = df_helper.get_table(parameter_name="input_table", parameter_display_name="Input Table", parameter_description="Input Raw Table") 
datetime_column =  df_helper.get_column(parameter_name="datetime_column", parameter_display_name="Chose Time Stamp Column", parameter_description="Timestamp Column")
aggregate_method = df_helper.get_string(parameter_name="aggregate_method", parameter_display_name="Aggregation Method", parameter_description="Aggregation Method")
metric_column = df_helper.get_column(parameter_name="metric_column", parameter_display_name="Chose Metric Column", parameter_description="Metric Column")
window= df_helper.get_string(parameter_name="window", parameter_display_name="Window (in days)", parameter_description="Window")
threshold_alert = df_helper.get_integer(parameter_name="threshold_alert", parameter_display_name="Threshold Alert", parameter_description="Threshold Alert")
email = df_helper.get_string(parameter_name="email", parameter_display_name="Enter Emails", parameter_description="Enter the emails to get the alert")

# importing pandas library

value_type_to_string = {'Average': 'mean',
                        'Median': 'median',
                        '80 Percentile': 'percentile',
                        '90 Percentile': 'percentile',
                        '95 Percentile': 'percentile'}
method = value_type_to_string.get(aggregate_method)
tab = df.groupby(datetime_column).agg({metric_column:'median'})
target = tab[-1:]

if method == 'mean':
    tab = tab.rolling(int(window)).apply(lambda x: sum(x)/len(x))
    value = tab[-2:-1]
elif method == 'percentile':
    percentile = int(aggregate_method[0:1])
    tab = tab.rolling(int(window)).apply(lambda x: np.percentile(x,percentile))
    value = tab[-2:-1]
else:
    tab = tab.rolling(int(window)).apply(lambda x: np.percentile(x,50))
    value = tab[-2:-1]

variation = abs(float(target.values) - float(value.values))/float(target.values)*100

if variation >= threshold_alert:
    s = "Variation Reached " + str(threshold_alert) + "% " + "Threshold. "
    s1 = "Today Value: "+ str(float(target.values)) + ',' + "Aggregated Value:" + str(float(value.values))
    df_helper.send_email(email, "Alert", s+s1, [], [])
    s= s+s1

else:
    s = "Variation Do Not Reached Threshold. "
    s1 = "Today Value: "+ str(float(target.values)) + ',' + "Aggregated Value:" + str(float(value.values))
    s= s+s1

df_helper.publish(s)