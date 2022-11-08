"""
Action for getting the summary of the RFM values for each segment
Inputs:
    - ml_segment_data: Dataframe with RFM values and ML segments
    - recency_column: Recency column of the ml_segment_data
    - frequency_column: Frequency column of the ml_segment_data
    - monetary_value_column: Monetary Value column of the ml_segment_data
    - segment_column: Column with respect to which user want the segmentation summary
Output:
    - Dataframe with summary statistics of the RFM values foe each segments
"""
import traceback
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, ml_segment_data, recency_column, frequency_column, monetary_value_column, segment_column):
        try:
            ml_segment_data[segment_column] = ['Segment ' + str(i) for i in ml_segment_data[segment_column]]
            table = ml_segment_data.groupby([segment_column]).agg({
                recency_column: 'mean',
                frequency_column: 'mean',
                monetary_value_column: ['mean', 'count']
            }).round(0)
            table = table.reset_index()
            table.columns = ['Segment', 'Recency(mean)', 'Frequency(mean)', 'MonetaryValue(mean)',
                             'Segment count']
            df_plot.pie_chart("Segment Count", 'Segment', 'Segment count', table)
            df_plot.bar_chart("Average Recency For Different Segments", 'Segment', 'Recency(mean)', table)
            df_plot.bar_chart("Average Frequency For Different Segments", 'Segment', 'Frequency(mean)', table)
            df_plot.line_chart("Average Monetary Value For Different Segments", 'Segment', 'MonetaryValue(mean)', table)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return table
