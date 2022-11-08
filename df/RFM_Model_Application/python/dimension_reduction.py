"""
Action for calculating first two principal component of the RFM table and add the value of those component to a new dataframe
Inputs:
    - segment_data: Dataframe with RFM values and segments
    - customer_id_column: Customer identification column of the segment_data
    - segment_column: Segment labels columns of segment_data
Output:
    - Modified Dataframe with RFM values and  the first two principal components values
"""""
import traceback
import pandas as pd
from dft import df_plot
from sklearn.preprocessing import StandardScaler
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, segment_data, customer_id_column, segment_column):
        from sklearn.decomposition import PCA
        try:
            pca = PCA(2)  # calculating the first two principal component
            scaler = StandardScaler()
            scaler.fit(segment_data.drop([customer_id_column, segment_column], axis=1))
            rfm_data_normalised = scaler.transform(segment_data.drop([customer_id_column, segment_column], axis=1))
            df = pca.fit_transform(rfm_data_normalised)
            PC1 = segment_data.columns[pca.components_[0].argmax()] + " (Feature 1)"
            PC2 = segment_data.columns[pca.components_[1].argmax()] + " (Feature 2)"
            d = pd.concat([segment_data, pd.DataFrame(df)], axis=1)
            d.rename(columns={0: PC1, 1: PC2}, inplace=True)
            df_plot.scatter_chart('Scatter Plot', PC1, PC2, d)
            print('The Scatter Plot Is Produced Using The Following Dimensions ')
            print(segment_data.columns[pca.components_[0].argmax()]+' and '+segment_data.columns[pca.components_[1].argmax()])
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise
        return d

