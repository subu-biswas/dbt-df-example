"""
Action to calculate the propensity of conversion of one or multiple points from one segment to other segment
Inputs:
    - Data: Dataframe with RFM values and first two principal component values
    -Selected_Segment: The segment for which the user wants to see the  propensity of conversion of the selected points (integer)
    -Targeted_Segment: The segment in which the points to be converted(integer)
    -Number_Of_Points: Number of points with high propensity user want (integer)
Output:
    - Dataframe with propensity value of the points which have high propensity
"""
import math
import traceback
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, Data, Selected_Segment, Targeted_Segment, Number_Of_Points):
        def select_points(data, select_segment, target_segment, no_of_points):
            t1 = point_prob(data, select_segment, target_segment)
            t1 = t1.sort_values('Propensity', ascending=False).head(no_of_points)
            t1 = t1[['id_customer', 'Propensity', 'Recency', 'Frequency', 'Monetary', 'T', 'Segment']]
            return t1

        def dist(data, select_segment, point1,
                 point2):  # Function for calculating the Euclidean distance between the points
            k = select_segment
            d = data.loc[data['Segment'] == k]
            l = sum(data['Segment'] == k)
            tmp = []
            for i in range(l):
                PC1 = d.columns[len(d.columns) - 2]
                PC2 = d.columns[len(d.columns) - 1]
                p1 = d.iloc[[i]][PC1]
                p2 = d.iloc[[i]][PC2]
                # Calculating the Euclidean distance between two points
                distance = math.sqrt(((p1 - float(point1)) ** 2) + ((p2 - float(point2)) ** 2))
                tmp.append(distance)

            return min(tmp)

        def point_prob(data, select_segment, target_segment):  # Function for calculating the Propensity
            k = select_segment
            d1 = data.loc[data['Segment'] == k]
            d2 = data.loc[data['Segment'] == select_segment]
            l = len(data['Segment'])
            tmp = []
            for i in range(l):
                PC1 = data.columns[len(data.columns) - 2]
                PC2 = data.columns[len(data.columns) - 1]
                point1 = data.iloc[[i]][PC1]
                point2 = data.iloc[[i]][PC2]
                tmp.append(dist(data, target_segment, point1, point2))

            tmp = [1 - (i - min(tmp)) / (max(tmp) - min(tmp)) for i in tmp]  # Propensity calculation

            data['Propensity'] = tmp
            d2 = data.loc[data['Segment'] == select_segment]

            return d2
        try:
         propensity_table = select_points(Data, Selected_Segment, Targeted_Segment, Number_Of_Points)
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return propensity_table
