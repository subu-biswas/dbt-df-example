"""
Action for calculating the ML segments
Inputs:
    - rfm_table: Dataframe with RFM values
    - number_of_segments: Number of segments (integer) user wants
Output:
    - Modified data with split column in the form of dataframe
"""
import traceback
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, rfm_table, id_customer, number_of_segments):
        try:
            table_tmp = rfm_table.drop(id_customer, axis=1)
            kmeans = KMeans(n_clusters=number_of_segments, random_state=1)  # creating the Kmeans model
            scaler = StandardScaler()
            scaler.fit(table_tmp)
            rfm_data_normalised = scaler.transform(table_tmp)  # data normalization
            kmeans.fit(rfm_data_normalised)
            cluster_labels = kmeans.labels_ + 1
            rfm_table['Segment'] = cluster_labels

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return rfm_table
