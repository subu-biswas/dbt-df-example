"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of  columns (with vector values) which will be assembled
Output:
    - Data in the form of dataframe with the new assembled columns
"""
from dft import df_plot
import traceback
from pandasql import sqldf


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data, dim_columns, agg_function, target_col, filter_query="1=1"):
        try:
            if filter_query is None:
                filter_query = "1=1"
            dim_lengths = len(dim_columns)
            if dim_lengths == 0:
                raise ValueError("Dimensions columns cannot be empty!")
            col_query = dim_columns[0]
            for i in range(1, dim_lengths-1):
                col_query = col_query + ", "
                col_query = col_query + dim_columns[i]
            agg_col_name = f"{agg_function}__{target_col}"
            sql_template = f"""
                SELECT {agg_function.strip()}({target_col}) as {agg_col_name}, {col_query}
                FROM data 
                WHERE {filter_query} 
                GROUP BY {col_query}          
            """
            result = sqldf(sql_template)
            print(f"Rendered query: {sql_template}")
            if dim_lengths == 1:
                chart_name = f"{target_col} Trend"
                df_plot.bar_chart(chart_name, dim_columns[0], agg_col_name, data=result)
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return result
