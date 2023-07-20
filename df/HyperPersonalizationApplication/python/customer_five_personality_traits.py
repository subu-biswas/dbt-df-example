"""
Inputs:
    - data: Transformed data  in the form of dataframe
    - segment_column: Name of the column to be used as segment
    - CustomerID: Unique id column to identify each customer
    - psychographic_data: Psychographic source data in the form of dataframe
Output:
    - Data in the form of dataframe with means of each features for all the segments and whole population
"""
import pandas as pd
import numpy as np
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data, CustomerID, psychographic_data, segment_column='segment'):
        try:
            psychographic_data = psychographic_data.drop(CustomerID, axis=1)
            traits = ['EXT', 'EST', 'AGR', 'CSN', 'OPN']
            trait_labels = ['Extroversion', 'Neuroticism', 'Agreeableness', 'Conscientiousness', 'Openness']
            df = pd.DataFrame()
            for trait in traits:
                trait_cols = sorted([col for col in psychographic_data.columns if trait in col])
                df[trait] = psychographic_data[trait_cols].median(axis=1)

            df = df.rename(columns={k: v for k, v in zip(traits, trait_labels)})
            df[segment_column] = data[segment_column]
            df_segment_traits = df.groupby([segment_column]).median().reset_index()
            df_segment_traits_copy = df_segment_traits.copy()
            df_segment_traits_copy['Extroversion'] = df_segment_traits_copy['Extroversion'].map(
                lambda x: 'Solitary/Reserved' if x <= 2 else ('Neutral' if x == 3 else 'Outgoing/Energetic'))
            df_segment_traits_copy['Neuroticism'] = df_segment_traits_copy['Neuroticism'].map(
                lambda x: 'Sensitive/Nervous' if x <= 2 else ('Neutral' if x == 3 else 'Resilient/Confident'))
            df_segment_traits_copy['Agreeableness'] = df_segment_traits_copy['Agreeableness'].map(
                lambda x: 'Critical/Rational' if x <= 2 else ('Neutral' if x == 3 else 'Friendly/Compassionate'))
            df_segment_traits_copy['Conscientiousness'] = df_segment_traits_copy['Conscientiousness'].map(
                lambda x: 'Extravagant/Careless' if x <= 2 else ('Neutral' if x == 3 else 'Efficient/Organized'))
            df_segment_traits_copy['Openness'] = df_segment_traits_copy['Openness'].map(
                lambda x: 'Consistent/Cautious' if x <= 2 else ('Neutral' if x == 3 else 'Inventive/Curious'))
            df_plot.radar_chart('Customer Five Traits', segment_column, list(df_segment_traits.columns[1:]), df_segment_traits)

        except Exception as e:
            raise type(e)(e)

        return df_segment_traits_copy
