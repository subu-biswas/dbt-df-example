"""
Inputs:
    - credentials: User Need To Provide The Google Drive Service Account API Credentials
    - spreadsheet_id: User Need To Provide The Google Spreed Sheet ID
    - sheet_name: User Need To Provide The Sheet Name Of The Google Spreed Sheet Want
Output:
    - Data in the form of dataframe
"""
import traceback
import pygsheets
from google.oauth2.service_account import Credentials
import json
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, spreadsheet_id, sheet_name):
        import pandas as pd
        credentials = {
            "type": "service_account",
            "project_id": "upbeat-sunspot-344107",
            "private_key_id": "a3634813e1ac40c3069d17f756820b72b14f43c3",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCIEHPGDu7Xt1oH\nEo8yt5jiGNJZbTRlYyLAZvYbTDUBhozpGAs3wTzB0vsVAbbGNArpQnSzuQS4nRbL\nU4PIKBzyk2okol/kLJYdf63/CBz1NaOY7XtJja48/zLZwKC9auUsGs5jVnd+nbB5\nw0XLCRDk7TKuuTWzC4WYKbKvsG5WUG0y0dY3v6PSmGWqiB+equsW4BE7ipem89Cp\nyMd7sh8JBi091GpzKJhUHPp2OIuOjskHPSt5TiIB2zZ85RMeVWasqjS7ERw9sdso\n/Y4WCMOeKGXb2HRut2b2Ao3BcYUABBXGAlw+2trgSaIs4bjYN6Qol++H/G3IYoxV\n3F9ZEV0JAgMBAAECggEACSqslhUW978BSHUyYw0V9nOMJlyyWTtI0dQN9BBaCNiH\nAUQuq0qwPHGpm0Ryix9h9c2Vna8TF5nzuW6OtAspyhju33l2AuITGfkqYIJsKwP6\nWdj0A6RRLwhVZXCyE5jSSDqlGXGvmr7mbwmeF9vsQLQPRFCzgrS9wx7aDDaTdGqd\nDkdARCglGANYDMjpH5A0akinfjF2EAvWlwYClUihFnFvQnv8s6M6GcmW3CrWgvY6\nm2idxJLspSe+d+7oNvVFeg/YMc6Kat+sKUMrvu4D6gunzjiZytA8kY4bpCs5KObr\n7hcEhtXC+2zRouWM1mvYwmfRag5YWl9Kut+yKGRStQKBgQC9Vd37fD33RBeUM/FT\n7kk2IAo75AcP/V5yaVx9IHkoajmV+HCqLjQMvGqj0o+hX/mwJiTWeDVfCFnwUcqm\nqv5AhQaXD8gJrBP0pbjBR8GOxkBK+QnNxtDkuILsui9VA2y2W/vI0OOrL1KxSvju\nzTkFhn/s/qeeKZ+8jVOOQjNIbQKBgQC3+OGswkuixxibw/d/olTOsaGP474/7NjV\n44C8rXS/xCbthbSB8ntLIapi8gJtRhgdjaIVl1fNmFIsBfHXy6Gv7VIb4ng2ZDZq\nxVkBgSm3u02162EcIUvSmWq+mNeDObaA8OCxcXTp9V1U4w7iMZH3sRAXareDo1yv\n2vKZ24K9jQKBgG5zoUQMfrm452h1xNsJr3v7xPybUeNqE6b7ABGe2A9TqLRsco1U\n1th3Ml7PfyrNKoQwPF1BUyFVZeJkVKxWJGzPLcECW9gIorud0eIvUiNQVEFodues\njEBhwz8GfoZBsTHRB1lnQumiecMj0YS+A/3NMOO4y1/hOGZuq8fZpsEhAoGAPEfe\nml7nraSTGabyl+224CswBxfWpNeUjTEIY8pqMNOy9a0T4mhzocCcNeOai/eluzlH\nXM/EQ2eftTEVd2IPzrdyahSg0yGu+vlDGs0ZwEwtQGip3y/BynXelik6pJLqjAFh\nfoLwjiCJvmDP5ancbE6mOSYMj6OdoTmauHNGuZkCgYBbzdqOGoucSCMenIMR1WyY\naIjfC+KVMO5ZFzz1Y2Dh7tIRN9EZZ0JFG0UXOwVU2bk2ur/B/fIoOE3xMsACvbYo\n4edtW6bYJPCOPErrdBO//hwgvHChsLzH8caPpLgyMY1Tw2GV58TFi7+IS9267aXE\ndmaZjYsHtDzbgkrn7HuOvA==\n-----END PRIVATE KEY-----\n",
            "client_email": "new-account@upbeat-sunspot-344107.iam.gserviceaccount.com",
            "client_id": "101687609443439823974",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/new-account%40upbeat-sunspot-344107.iam.gserviceaccount.com"
        }
        credentials = json.dumps(credentials)
        credentials = json.loads(credentials)
        SCOPES = ('https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive')
        my_credentials = Credentials.from_service_account_info(credentials, scopes=SCOPES)
        google_credentials = pygsheets.authorize(custom_credentials=my_credentials)
        google_sheet = google_credentials.open_by_key(spreadsheet_id)
        try:
            worksheet = google_sheet.worksheet_by_title(sheet_name)
            df_records = worksheet.get_all_records()
            df = pd.DataFrame.from_dict(df_records)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return df

ext=ExecutionHandler()

ext.execute("14ww2MBA2L5j5FJRYyRXgNilm5-JExVPMaDYIj296jFw", "test sheet")