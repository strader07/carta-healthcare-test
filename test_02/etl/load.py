import pandas as pd
import numpy as np
from carta_interview import Datasets, get_data_file

from sqlalchemy import create_engine

class DataLoader(object):
    """Load data into postgres"""
    def __init__(self):
        self.psql_host = "localhost"
        self.psql_db = "test_02"
        self.psql_user = ""
        self.psql_pwd = ""

    def load_data(self):
        patient_extract1 = get_data_file(Datasets.PATIENT_EXTRACT1)
        patient_extract2 = get_data_file(Datasets.PATIENT_EXTRACT2)

        # Read data from excel files
        df1 = pd.read_excel(patient_extract1)
        df2 = pd.read_excel(patient_extract2)

        df = pd.concat([df1, df2])
        df["Update"] = pd.to_datetime(df["Update D/T"], format="%m/%d/%Y %I:%M %p")
        df = df.sort_values(["MRN", "Update"], ascending=[True, True])
        df = df.drop(["Update"], axis=1)
        df = df.drop_duplicates(subset=["MRN"], keep="last").reset_index(drop=True)

        ## Implement load into postgres
        conn_url = f'postgresql+psycopg2://postgres@{self.psql_host}/{self.psql_db}'
        conn = create_engine(conn_url)
        df.to_sql("patients", conn, if_exists='replace', index=False, method='multi')