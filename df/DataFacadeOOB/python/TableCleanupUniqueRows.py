from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, df):
        final_df = df.drop_duplicates()
        return final_df
    