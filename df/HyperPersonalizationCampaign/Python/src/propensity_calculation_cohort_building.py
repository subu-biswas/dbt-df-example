"""
Inputs:
    - source_data: Original fintech dcata in the form of dataframe
    - transformed_data: Transformed Fintech data  in the form of dataframe with the target label
    - CustomerID: Chose unique identification column for customer
    - target_column: Chose the target column for calculating propensity
    - method: Classification model to be used("Decision Tree Classifier,Logistic Regression(default),Random Forest Classifier,XGBoost Classifier")
Outputs:
    - Propensity values
"""
import pandas as pd
import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder
import dirty_cat


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, source_data, transformed_data, target_column, CustomerID, propensity_lower_limit, propensity_upper_limit,method='Logistic Regression'):
        try:
            data = transformed_data.drop([target_column, CustomerID], axis=1)
            data = self.numeric_column_scale(data)
            data = self.categorical_column_encode(data)
            X_train = data
            y_train = transformed_data[target_column]
            if method == 'Logistic Regression':
                model = LogisticRegression(multi_class='multinomial', solver='sag', penalty='l2', random_state=100,
                                           C=0.8, max_iter=1000)
                model.fit(X_train, y_train)
                cm = pd.crosstab(y_train, model.predict(X_train), rownames=['Actual'], colnames=['Predicted']
                                 , margins=True)  # Confusion Matrix

                # probabilities of each member of the segmentation
                model_prediction = pd.DataFrame(model.predict_proba(X_train))
                columns = ['Propensity(' + str(i) + ')' for i in range(y_train.min(), y_train.max() + 1)]
                model_prediction.columns = columns
                model_prediction['Predicted'] = model.predict(X_train)

            elif method == 'Decision Tree Classifier':
                model = DecisionTreeClassifier(max_depth=5, max_leaf_nodes=5,
                                               random_state=100)
                model.fit(X_train, y_train)
                cm = pd.crosstab(y_train, model.predict(X_train), rownames=['Actual'], colnames=['Predicted']
                                 , margins=True)  # Confusion Matrix
                model_prediction = pd.DataFrame(model.predict_proba(X_train))
                columns = ['Propensity(' + str(i) + ')' for i in range(y_train.min(), y_train.max() + 1)]
                model_prediction.columns = columns
                model_prediction['Predicted'] = model.predict(X_train)

            elif method == 'Random Forest Classifier':
                model = RandomForestClassifier(n_jobs=-1, random_state=42, max_depth=5,
                                               max_leaf_nodes=5)
                model.fit(X_train, y_train)
                cm = pd.crosstab(y_train, model.predict(X_train), rownames=['Actual'], colnames=['Predicted']
                                 , margins=True)  # Confusion Matrix
                model_prediction = pd.DataFrame(model.predict_proba(X_train))
                columns = ['Propensity(' + str(i) + ')' for i in range(y_train.min(), y_train.max() + 1)]
                model_prediction.columns = columns
                model_prediction['Predicted'] = model.predict(X_train)

            else:
                model = XGBClassifier(random_state=100, n_jobs=-1, n_estimators=10, max_depth=5,
                                      use_label_encoder=False,
                                      learning_rate=1, eval_metric='logloss')
                model.fit(X_train, y_train)
                cm = pd.crosstab(y_train, model.predict(X_train), rownames=['Actual'], colnames=['Predicted']
                                 , margins=True)  # Confusion Matrix
                model_prediction = pd.DataFrame(model.predict_proba(X_train))
                columns = ['Propensity(' + str(i) + ')' for i in range(y_train.min(), y_train.max() + 1)]
                model_prediction.columns = columns
                model_prediction['Predicted'] = model.predict(X_train)
            source_data['Propensity'] = model_prediction['Propensity(1)']*100
            source_data = source_data[source_data['Propensity'] > propensity_lower_limit]
            source_data = source_data[source_data['Propensity'] <= propensity_upper_limit]
            print(source_data)

        except Exception as e:
            raise type(e)(e)

        return source_data

    def categorical_column_encode(self, data: pd.DataFrame):
        columns = list(data.select_dtypes(include=['object']).columns)
        distinct_values = dict(map(lambda x: (x, len(pd.unique(data[x]))), columns))
        least_distinct_columns = list(dict(filter(lambda x: x[1] <= 5, distinct_values.items())).keys())
        mid_distinct_columns = list(dict(filter(lambda x: 5 < x[1] <= 10, distinct_values.items())).keys())
        most_distinct_values = list(dict(filter(lambda x: 10 < x[1], distinct_values.items())).keys())

        if len(least_distinct_columns):
            data = self.one_hot_encode(data, least_distinct_columns, drop_columns=True)
        if len(mid_distinct_columns):
            data = self.original_encode(data, mid_distinct_columns)
        if len(most_distinct_values):
            data = self.similarity_encode(data, most_distinct_values)

        return data

    def one_hot_encode(self, data: pd.DataFrame, columns, drop_columns):
        try:
            encoder = OneHotEncoder(sparse=False, handle_unknown='ignore').fit(data[columns])

            encoded_cols = list(encoder.get_feature_names_out(columns))

            data[encoded_cols] = encoder.transform(data[columns])
            if drop_columns:
                data = data.drop(columns, axis=1)

            for col in columns:
                print(col + " is " + "encoded")
            return data

        except Exception as e:
            raise type(e)(e)

    def original_encode(self, data: pd.DataFrame, columns):
        try:
            encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=np.nan).fit(data[columns])
            new_names = [column + '_' + 'encode' for column in columns]
            data[new_names] = pd.DataFrame(encoder.transform(data[columns]))
            data.drop(columns, axis=1, inplace=True)
            for col in columns:
                print(col + " is " + "encoded")
            return data
        except Exception as e:
            raise type(e)(e)

    def similarity_encode(self, data: pd.DataFrame, columns):
        try:
            for col in columns:
                encoder = dirty_cat.SimilarityEncoder(similarity='ngram')
                encoded_df = pd.DataFrame(encoder.fit_transform(data[[col]]))
                encoded_df.columns = [col + '_' + str(i) for i in encoded_df.columns]
                data = pd.concat([data, encoded_df], axis=1)
                data.drop(col, axis=1, inplace=True)
                print(col + " is " + "encoded")

            return data
        except Exception as e:
            raise type(e)(e)

    def numeric_column_scale(self, data: pd.DataFrame):
        from sklearn.preprocessing import MinMaxScaler
        columns = list(data.select_dtypes(include=['int', 'float']).columns)
        for col in columns:
            if data[col].dtype == 'float':
                scaler = MinMaxScaler().fit(data[[col]])
                data[col] = scaler.transform(data[[col]])

                print(col + ' Is Normalized')
            else:
                if data[col].nunique() / len(data[col]) > 0.5:
                    scaler = MinMaxScaler().fit(data[[col]])
                    data[col] = scaler.transform(data[[col]]).round(2)
                    print(col + ' Is Normalized')
                else:
                    dummies = pd.get_dummies(data[col])
                    dummies.columns = [col + '_' + str(i) for i in list(dummies.columns)]
                    data = pd.concat([data, dummies], axis=1)
                    data = data.drop(col, axis=1)
                    print(col + ' Is Normalized')

        return data
