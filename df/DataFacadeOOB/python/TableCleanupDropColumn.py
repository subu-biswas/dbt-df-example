from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, df, column_to_remove_name):
        final_df = df.drop(column_to_remove_name, 1)
        return final_df
    