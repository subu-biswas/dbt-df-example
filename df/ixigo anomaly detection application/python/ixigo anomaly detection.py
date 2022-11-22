import pandas as pd
from dateutil.relativedelta import relativedelta
from pmdarima import auto_arima
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
import numpy as np
import keras
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class NNMultistepModel():

    def __init__(
            self,
            X, Y, n_outputs, n_lag, n_ft, n_layer, batch,
            epochs, lr, Xval=None, Yval=None, mask_value=-999.0,
            min_delta=0.001, patience=5):
        lstm_input = keras.Input(shape=(n_lag, n_ft))

        # Series signal
        lstm_layer = keras.layers.LSTM(n_layer, activation='relu')(lstm_input)

        x = keras.layers.Dense(n_outputs)(lstm_layer)

        self.model = keras.Model(inputs=lstm_input, outputs=x)
        self.batch = batch
        self.epochs = epochs
        self.n_layer = n_layer
        self.lr = lr
        self.Xval = Xval
        self.Yval = Yval
        self.X = X
        self.Y = Y
        self.mask_value = mask_value
        self.min_delta = min_delta
        self.patience = patience

    def trainCallback(self):
        return keras.callbacks.EarlyStopping(monitor='loss', patience=self.patience, min_delta=self.min_delta)

    def train(self):
        # Getting the untrained model
        empty_model = self.model

        # Initiating the optimizer
        optimizer = keras.optimizers.Adam(learning_rate=self.lr)

        # Compiling the model
        empty_model.compile(loss=keras.losses.MeanAbsoluteError(), optimizer=optimizer)

        if (self.Xval is not None) & (self.Yval is not None):
            history = empty_model.fit(
                self.X,
                self.Y,
                epochs=self.epochs,
                batch_size=self.batch,
                validation_data=(self.Xval, self.Yval),
                shuffle=False,
                callbacks=[self.trainCallback()]
            )
        else:
            history = empty_model.fit(
                self.X,
                self.Y,
                epochs=self.epochs,
                batch_size=self.batch,
                shuffle=False,
                callbacks=[self.trainCallback()]
            )

        # Saving to original model attribute in the class
        self.model = empty_model

        # Returning the training history
        return history

    def predict(self, X):
        return self.model.predict(X)


class ExecutionHandler(BaseExecutionHandler):

    def execute(self, df: pd.DataFrame, forecast_period: int, timestamp_column, target_column, threshold_percentage,
                covariate_columns=[]):

        def create_X_Y(ts: np.array, lag=1, n_ahead=1, target_index=0) -> tuple:
            """
            A method to create X and Y matrix from a time series array for the training of
            deep learning models
            """
            # Extracting the number of features that are passed from the array
            n_features = ts.shape[1]

            # Creating placeholder lists
            X, Y = [], []

            if len(ts) - lag <= 0:
                X.append(ts)
            else:
                for i in range(len(ts) - lag - n_ahead):
                    Y.append(ts[(i + lag):(i + lag + n_ahead), target_index])
                    X.append(ts[i:(i + lag)])

            X, Y = np.array(X), np.array(Y)

            # Reshaping the X array to an RNN input shape
            X = np.reshape(X, (X.shape[0], lag, n_features))

            return X, Y

        frequency_type = pd.infer_freq(df[timestamp_column])
        df.sort_values(by=timestamp_column, ascending=True)
        df = df.set_index(timestamp_column)
        target_value = df[target_column][df.shape[0] - 1]
        # df = df.asfreq(frequency_type)

        column_means = df.mean()
        df = df.fillna(column_means)

        trimmed_df = df.tail(min(3000, len(df)))
        # -----When target column do not depend on covariates
        if len(covariate_columns) == 0:
            time_series = trimmed_df[target_column].copy()
            # ----fitting arima model
            print('Please Wait. Fitting Timesereies Model ')
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
            forecast = pd.Series(forecast_model.predict(forecast_period))
        # -----When target column depend on co variates
        else:
            data = pd.DataFrame(df[covariate_columns].copy())
            for i in data.select_dtypes('object').columns:
                le = LabelEncoder().fit(data[i])
                data[i] = le.transform(data[i])
            for col in data.select_dtypes('float', 'int').columns:
                scaler = MinMaxScaler().fit(data[[col]])
                data[col] = scaler.transform(data[[col]])
            data[target_column] = df[target_column]

            # Creating the X and Y for training
            lag = 100
            # Steps ahead to forecast
            n_ahead = forecast_period
            Xtrain, Ytrain = create_X_Y(data.values, lag=lag, n_ahead=n_ahead, target_index=data.shape[1] - 1)

            # -----Fitting LSTM model
            # Epochs for training
            epochs = 10
            # Batch size
            batch_size = 512
            # Learning rate
            lr = 0.001
            # Number of neurons in LSTM layer
            n_layer = 10
            n_ft = Xtrain.shape[2]
            # Creating the model object
            model = NNMultistepModel(X=Xtrain, Y=Ytrain, n_outputs=n_ahead,
                                     n_lag=lag, n_ft=n_ft, n_layer=n_layer,
                                     batch=batch_size, epochs=epochs,
                                     lr=lr, Xval=Xtrain, Yval=Ytrain, )

            # Training the model
            print('Generating Forecast')
            history = model.train()

            print('Generating Forecast')
            forecast = [x[0] for x in model.predict(Xtrain)]
            forecast = pd.Series(forecast[-forecast_period:]
            # forecast = forecast[data.shape[0]:]
        if sum(abs(forecast - target_value) / 100 > threshold_percentage / 100) > 0:

            print('The Value Reached The Threshold')

        else:

            pass

        return forecast
