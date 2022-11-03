"""
Inputs:
    - data: Transformed data  in the form of dataframe
    - segment_column: Name of the column to be used as segment
    - CustomerID: Unique id column to identify each customer
    - propensity_data: Propensity source data in the form of dataframe
Output:
    - Data in the form of dataframe with means of each features for all the segments and whole population
"""
import pandas as pd
import numpy as np
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data, CustomerID, propensity_data, propensity_column, lower_range_discount, upper_range_discount, segment_column='segment'):
        try:
            selected_user = data[CustomerID]
            propensity_data.set_index("user", inplace=True)
            propensity_data = propensity_data.loc[selected_user].reset_index()
            df = pd.DataFrame()
            df[propensity_column] = propensity_data[propensity_column]
            Segment = data[segment_column]
            df[segment_column] = Segment
            df_segment_propensity = df.groupby([segment_column]).median().reset_index()
            max_propensity = propensity_data[propensity_column].max()
            min_propensity = propensity_data[propensity_column].min()
            mid_propensity = (max_propensity+min_propensity)/2
            mid_discount = (lower_range_discount+upper_range_discount)/2
            df_segment_propensity['Discount'] = df_segment_propensity[propensity_column].map(
                lambda x: np.random.uniform(lower_range_discount, mid_discount, 1)[0] if x > mid_propensity else
                np.random.uniform(mid_discount, upper_range_discount, 1)[0])
            df_segment_propensity['Discount'] = df_segment_propensity['Discount'].apply(np.floor)
            discount_df = pd.DataFrame()
            discount_df[CustomerID] = data[CustomerID]
            discount_df[segment_column] = data[segment_column]
            discount_df['Discount(in %)'] = data[segment_column].map(lambda x: df_segment_propensity['Discount'][x])
            discount_df[data.drop(segment_column, axis=1).columns[1:]] = data[data.drop(segment_column, axis=1).columns[1:]]

        except Exception as e:
            raise type(e)(e)

        return discount_df
