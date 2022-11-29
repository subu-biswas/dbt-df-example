import pandas as pd
from dateutil.relativedelta import relativedelta
from pmdarima import auto_arima
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
import numpy as np
#import keras
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler
from prophet import Prophet
import matplotlib.pyplot as plt


class ExecutionHandler(BaseExecutionHandler):

    def execute(self, df: pd.DataFrame, observation_period: int, forecast_period: int, timestamp_column, target_column, threshold_percentage, covariate_columns=[]):
    
        if covariate_columns[0] not in df.columns:
            covariate_columns.clear()
        else:
            pass

        frequency_type = pd.infer_freq(df[timestamp_column])
        df.sort_values(by=timestamp_column, ascending=True)
        start_date = df[timestamp_column][0]
        df = df.set_index(timestamp_column)
        # df = df.asfreq(frequency_type)
        # ts_length = len(df[target_column])
        target_value = df[target_column][df.shape[0] - 1]

        column_means = df.mean()
        df = df.fillna(column_means)

        trimmed_df = df.tail(min(3000, len(df)))

        # -----When target column do not depend on covariates
        if len(covariate_columns) == 0:
            time_series = trimmed_df[target_column].copy()
            # ----fitting arima model
            print('Please Wait. Fitting Timeseries Model ')
            forecast_model = auto_arima(time_series, start_p=0, start_q=0,
                                        test='adf',  # use adftest to find optimal 'd'
                                        max_p=3, max_q=3,  # maximum p and q
                                        start_Q=0,
                                        max_Q=5,
                                        m=12,  # frequency of series
                                        d=None,  # let model determine 'd'
                                        seasonal=True,  # No Seasonality
                                        start_P=0,
                                        D=0,
                                        trace=False,
                                        error_action='ignore',
                                        suppress_warnings=True,
                                        stepwise=True)
            print('Generating Forecast')
            forecast = pd.Series(forecast_model.predict(forecast_period))
            # -----real time trend generation
            time_window = 7
            print('Please Wait. Generating Trend')
            trimmed_df['trend'] = trimmed_df[target_column].rolling(time_window).mean()
            trimmed_df.dropna(inplace=True)
            trimmed_df['Absolute Difference From Trend In %'] = 100 * abs(
                trimmed_df[target_column] - trimmed_df['trend']) / trimmed_df['trend']
            trimmed_df['Is Significant (Threshold Is ' + str(threshold_percentage) + '%)'] = trimmed_df[
                                                                                                 'Absolute Difference From Trend In %'] > threshold_percentage
            trimmed_df.rename(columns={target_column: 'Actual Values', 'trend': 'Trend Values'}, inplace=True)
            trimmed_df = trimmed_df[['Actual Values', 'Trend Values', 'Absolute Difference From Trend In %',
                                     'Is Significant (Threshold Is ' + str(threshold_percentage) + '%)']].reset_index()

        # -----When target column depend on covariates
        else:
            data = pd.DataFrame(df[covariate_columns].copy())
            for i in data.select_dtypes('object').columns:
                le = LabelEncoder().fit(data[i])
                data[i] = le.transform(data[i])
            for col in data.select_dtypes('float', 'int').columns:
                scaler = MinMaxScaler().fit(data[[col]])
                data[col] = scaler.transform(data[[col]])
            data[target_column] = df[target_column]

            trimmed_df = data.tail(min(3000, len(data))).reset_index()
            # -----Fitting Prophet model
            trimmed_df.rename(columns={timestamp_column: 'ds', target_column: 'y'}, inplace=True)
            model1 = Prophet(interval_width=0.95)

            # Training the model
            print('Please Wait Fitting Time Series Model')
            model1.fit(trimmed_df)

            print('Generating Forecast')
            future = model1.make_future_dataframe(periods=forecast_period)
            forecast = model1.predict(future)['yhat'][-forecast_period:]
            # forecast = forecast[data.shape[0]:]

            # -----real time trend generation
            # -----Not considering the values in the observation period in training
            trimmed_df = data.tail(min(3000, len(data))).reset_index()
            trimmed_df = trimmed_df[:-observation_period]

            trimmed_df.rename(columns={timestamp_column: 'ds', target_column: 'y'}, inplace=True)
            print('Please Wait. Generating Trend')
            model2 = Prophet(interval_width=0.95)
            model2.fit(trimmed_df)
            trend_df = model2.make_future_dataframe(periods=observation_period)
            predict = model2.predict(trend_df)
            trend = predict['yhat']
            ds = predict['ds']

            trimmed_df = trimmed_df[-len(trend):].reset_index()
            trimmed_df['trend'] = trend
            trimmed_df[timestamp_column] = ds

            # -----Generating output df
            trimmed_df['Absolute Difference From Trend In %'] = 100 * abs(trimmed_df['y'] - trimmed_df['trend'])/trimmed_df['trend']
            trimmed_df['Is Significant (Threshold Is ' + str(threshold_percentage) + '%)'] = trimmed_df[
                                                                                                 'Absolute Difference From Trend In %'] > threshold_percentage
            trimmed_df.rename(columns={'y': 'Actual Values', 'trend': 'Trend Values' }, inplace=True)
            trimmed_df = trimmed_df[[timestamp_column, 'Actual Values', 'Trend Values', 'Absolute Difference From Trend In %',
                                     'Is Significant (Threshold Is ' + str(threshold_percentage) + '%)']]

        if sum(trimmed_df['Is Significant (Threshold Is ' + str(threshold_percentage) + '%)'][
               -observation_period:]) > 0:
            print('Real Time Value Significantly Deviating From Trend Values')
            fig = trimmed_df[['Actual Values', 'Trend Values']][-30:].plot(marker='.', figsize=(12, 8))
            fig.get_figure().savefig("trend_output.jpg", bbox_inches="tight",
                                     pad_inches=0.1, transparent=True)
            trimmed_df = trimmed_df[-100:]

        else:
            pass

        timeseries = list(df[target_column][-100:])
        
        if sum(abs(forecast - target_value) / 100 > threshold_percentage / 100) > 0:

            print('The Value Reached The Threshold')

            #  time_stamp = pd.date_range(start=start_date, periods=ts_length+forecast_period, freq=frequency_type)

            # upper_anomaly = np.ma.masked_greater_equal(list(forecast), target_value+(target_value*threshold_percentage/100))
            # lower_anomaly = np.ma.masked_less_equal(list(forecast),target_value-(target_value*threshold_percentage/100))
            # plot the full timeseries
            plt.figure(figsize=(12, 8))
            plt.plot(range(0, 30), timeseries[-30:], '.-b', label='Actual Values')

            # plotting the anomaly
            plt.plot(range(30, 30 + len(forecast)), list(forecast), 'rx', linewidth=2, markersize=10,
                     label='Forecasted Values')

            # adding the threshold value
            plt.axhline(target_value + (target_value * threshold_percentage / 100), color='orange', linestyle='--',
                        label=str(threshold_percentage) + '% Threshold')
            plt.axhline(target_value - (target_value * threshold_percentage / 100), color='orange', linestyle='--')
            # plt.show()
            plt.legend()
            plt.savefig("output.jpg", bbox_inches="tight",
                        pad_inches=0.1, transparent=True)

        else:

            pass

        # ----Generating Output df
        timeseries = timeseries + list(forecast)
        is_forecast = [False] * 100 + [True] * forecast_period
        is_significant = [np.nan] * 100 + list(abs(forecast - target_value) / 100 > threshold_percentage / 100)
        output_df = pd.DataFrame()
        output_df['Values'] = timeseries
        output_df['Is Forecasted'] = is_forecast
        output_df['Is Significant (Threshold Is ' + str(threshold_percentage) + '%)'] = is_significant
        output_df = output_df.reset_index()

        return trimmed_df