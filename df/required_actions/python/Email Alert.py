import pandas as pd

df = df_helper.get_table(parameter_name="input_table", parameter_display_name="Input Table", parameter_description="Input Raw Table")
email = df_helper.get_string(parameter_name="email", parameter_display_name="Enter Emails", parameter_description="Enter the emails to get the report")
file_paths = []
df.to_csv("/tmp/report.csv")
file_paths.append("/tmp/report.csv")
df_helper.send_email(email, "Daily Attachment Rate Report", "Hello, Please find attached the Attachment Rate Report for today.", [], file_paths)
#df_helper.send_slack("C01NSTT6AA3", "Daily Attachment Rate Report", [], file_paths)

df_helper.publish(df)