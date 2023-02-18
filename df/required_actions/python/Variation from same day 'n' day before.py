df_helper.install(['pandas']) 
import pandas as pd

df = df_helper.get_table(parameter_name="input_table", parameter_display_name="Input Table", parameter_description="Input Raw Table")
datetime_column =  df_helper.get_column(parameter_name="datetime_column", parameter_display_name="Chose Time Stamp Column", parameter_description="Timestamp Column")
metric_column = df_helper.get_column(parameter_name="metric_column", parameter_display_name="Chose Metric Column", parameter_description="Metric Column")
window= df_helper.get_integer(parameter_name="window", parameter_display_name="Window (in days)", parameter_description="Window")
threshold_alert = df_helper.get_integer(parameter_name="threshold_alert", parameter_display_name="Threshold Alert", parameter_description="Threshold Alert")
email = df_helper.get_string(parameter_name="email", parameter_display_name="Enter Emails", parameter_description="Enter the emails to get the alert")

tab = df.groupby(datetime_column).agg({metric_column:'median'})
target = tab[metric_column][:1]
value = tab[metric_column][(int(window)-1):int(window)]

variation =  abs(float(target.values) - float(value.values))

if variation >= threshold_alert:
    s = "Variation Reached " + str(threshold_alert) + "% " + "Threshold. "
    s1 = "Today Value: "+ str(int(target.values)) + ',' + "Value:" + str(int(value.values))
    df_helper.send_email(email, "Alert", s+s1, [], [])
    s= s+s1

else:
    s = "Variation Do Not Reached Threshold. "
    s1 = "Today Value: "+ str(int(target.values)) + ',' + "Value:" + str(int(value.values))
    s= s+s1

df_helper.publish(s)