

from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def __init__(self, mode="platform"):
        self.mode = mode

    def execute(self, df):
        duplicate_columns = df.T.duplicated().tolist()
        selected_columns = [not i for i in duplicate_columns]
        df = df.loc[:, selected_columns]
        return df
