import pandas as pd 
from prophet import Prophet
import numpy as np
from dft import df_plot

df = df_helper.get_table(parameter_name="input_table", parameter_display_name="Select The Table", parameter_description="Input table.")
target_column = df_helper.get_column(parameter_name="target_column", parameter_display_name="Select The Time Series Column", parameter_description="Please select the time series column on which forecast Will be generated.")
dimension_col = df_helper.get_column("dimension_col","Groupby","Plese specify it if want forecast based on a specific category else not required.")
forecast_period = df_helper.get_integer("forcast_period", "Forecast Period", "Please select the forecast period")
frequency = df_helper.get_string("fequency", "Forequency Of The Forecast Period", "Please select frquecy of the forecast period")
filter_query = df_helper.get_string("filter_query", "Specify Any SQL Query For Filtering", "Please specify any sql query for filtering.")
threshold_value = df_helper.get_decimal("threhold_value", "Specify The Threshold Value If Any", "Please specify the threshold value if you have any")


# CONFIGS
num_row_threshold = 100
num_cat_threshold = 5
trend_slope = 0.05

metadata = df_helper.get_table_metadata("input_table")

table_tags = metadata.Tags
def data_filter(df: pd.DataFrame, sql):
    try:
        df = df.query(f'{sql}')
    except Exception as e:
        print("Malformed SQL. Filtering aborted")
        print(e)
    return df
frequency_option_to_string = {
            'Months': 'MS',
            'Calendar Days': 'D',
            'Weeks': 'W',
            'Business Days': 'B',
            'Hours': 'H',
            'Minutes': 'T',
            'Seconds': 'S',
            'Years': 'Y'
        }
frequency_string = frequency
frequency = frequency_option_to_string.get(frequency)
print(frequency)
df = data_filter(df, filter_query)
if dimension_col == None:
    pass
else:
    if df[dimension_col].nunique() <= 5:
        pass
    else:
        raise "Number of categories should not be greater than 5 , please use SQL query for filterin the data"
if "TimeSeriesTable" in table_tags:
    for c in metadata.Columns:
        c_tag = c.Tags
        if 'TimestampColumn' in c_tag:
            timestamp_column = c.Name
            break
    df[timestamp_column]=pd.to_datetime(df[timestamp_column])
    ts_df = df.set_index(timestamp_column).resample(frequency).median()

elif "TimeAndValueTable" in table_tags:
    aggregate_by = 'median'
    timestamp_columns = []
    for c in metadata.Columns:
        c_tag = c.Tags
        if "TimeAndValue" in c_tag:
            timestamp_columns.append(c.Name)
    timestamps = []
    for column in timestamp_columns:
            if "/" in column:
                timestamps.append(column.split('/')[1])
            else:
                timestamps.append(column)

    date_col_dict = dict((i, i.split('/')[1]) for i in df.columns if "/" in i)
    ts_df = df.rename(columns=date_col_dict)

    grouped_by = ts_df.groupby(dimension_col).agg({timestamp: aggregate_by for timestamp in timestamps}).reset_index()
    ts_df = pd.melt(grouped_by, id_vars=[dimension_col], value_vars=timestamps)

    ts_df = pd.pivot(ts_df, values='value', index='variable', columns=dimension_col).reset_index()
    ts_df['variable'] = pd.to_datetime(ts_df['variable'])
    ts_df = ts_df.rename({'variable': timestamp_column}, axis=1)
    ts_df.set_index('variable').resample(frequency).median()
elif "TimeSeriesTableWithDimensions" in table_tags:
    for c in metadata.Columns:
        c_tag = c.Tags
        if "TimestampColumn" in c_tag:
            timestamp_column = c.Name
            break
    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
    ts_df = df.pivot(columns=dimension_col, values=target_column)
    ts_df[timestamp_column] = df[timestamp_column]
    ts_df = ts_df.groupby([timestamp_column]).median().reset_index()
    ts_df = ts_df.set_index(timestamp_column).resample(frequency).median()
    
else:
    raise "Forecast not possible for this table"

#Forecast Generation
if len(df[target_column]) < num_row_threshold:
    raise "Number of datapoint is very low for generating forecast"
trimmed_df = ts_df.tail(min(3000, len(df[target_column])))
primary_dimension_values = ts_df.columns.tolist()
forecast_df1 = pd.DataFrame()
for primary_dimension_value in primary_dimension_values:
     # ----fitting prophet model
     print('Please Wait. Fitting Timeseries Model For '+primary_dimension_value)
     time_series = trimmed_df[primary_dimension_value].copy()
     time_series = pd.DataFrame(time_series.reset_index())
     timestamp_column = time_series.columns[0]
     target_column1 =  time_series.columns[1]
     time_series.rename(columns={timestamp_column: 'ds', target_column1: 'y'}, inplace=True)
     model = Prophet(interval_width=0.95)

     # Training the model
     model.fit(time_series)

     print('Generating Forecast')
     future = model.make_future_dataframe(periods=forecast_period, freq=frequency)
     forecast = model.predict(future)[['ds', 'yhat']][-forecast_period:]
     forecast.rename(columns={'yhat': 'y'}, inplace=True)
     forecast_df = time_series.append(forecast)
     forecast_df.rename(columns={'ds': timestamp_column, 'y': target_column1}, inplace=True)
     forecast_df = forecast_df.set_index(timestamp_column)

     forecast_df1 = forecast_df1.append(forecast_df, ignore_index=True)
     
    # Infering Trend
     coefficients = np.polyfit(np.array(forecast['y']), np.array(range(1,forecast_period + 1)), 1)
     slope = coefficients[0]

     if forecast_period > 5:
         if slope > trend_slope:
             print("The forecasted time series has an upward trend\n")
         elif slope < -trend_slope:
             print("The forecasted time series has a downward trend\n")
         else:
             print("The forecasted time series has no trend")
     else:
        pass
     
     if threshold_value == None:
         pass
     else:
        if time_series['y'].tail(1).item() > threshold_value:
            for i,j in zip(forecast['y'],range(1,forecast_period + 1)):
                if i < threshold_value:
                    if dimension_col == None:
                        s = "1. The " + target_column + " value will drop below " + str(threshold_value) + ".\n"
                        s = s + "2. The value will drop below threshold within " + str(j) + " " + frequency_string + "."
                        print(s)
                        break
                    else:
                        s = "1. The " + target_column + " value will drop below " + str(threshold_value) +  " for " + primary_dimension_value + ".\n"
                        s = s + "2. The value will drop below threshold witin " + str(j)  + " " + frequency_string + "."
                        print(s)
                        break
                else:
                    pass
        else:
            for i,j in zip(forecast['y'],range(1,forecast_period + 1)):
                if i > threshold_value:
                    if dimension_col == None:
                        s = "1. The " + target_column + " value will jump above " + str(threshold_value) + ".\n"
                        s = s + "2. The value will jump above threshold within " + str(j)+ " " + frequency_string + "."
                        print(s)
                        break
                    else:
                        s = "1. The " + target_column + " value will jump above " + str(threshold_value) +  " for " + primary_dimension_value + ".\n"
                        s = s + "2. The value will jump above threshold within " + str(j) + " " + frequency_string + "."
                        print(s)
                        break
                    
                else:
                    pass
    

df_plot.time_series_forecast("Time Series Forecast", forecast_period, forecast_df1[-30+forecast_period:])


df_helper.publish(forecast_df1)