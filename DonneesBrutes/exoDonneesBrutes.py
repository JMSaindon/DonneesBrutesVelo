import os
import sys
import shutil
import pandas as pd
from dateutil import parser
import traceback
import zipfile

from joblib.memory import Memory

import station_name

with zipfile.ZipFile('brut.zip') as myzip:
    with myzip.open('brut/weather_bicincitta_parma.csv') as myfile:
        weather = pd.read_csv(myfile, nrows=1000)
    with myzip.open('brut/status_bicincitta_parma.csv') as myfile:
        byke = pd.read_csv(myfile, sep=';', header=None, names=["Timestamp","Station","Status","Bikes","Slots"], nrows=10000)
    with myzip.open('brut/bicincitta_parma_summary.csv') as myfile:
        stations = pd.read_csv(myfile)


def parse_date(df):
    def valid_datetime(date):
        try:
            return parser.parse(date)
        except ValueError:
            print("Invalid date:", date)
            return None
    df['Timestamp'] = df['Timestamp'].apply(valid_datetime)
    return df

def clean_byke_data(byke_df):
    byke_df = byke_df[byke_df["Timestamp"] != None]
    byke_df = byke_df[byke_df["Status"] == 1]
    return byke_df

byke = parse_date(byke)
byke = clean_byke_data(byke)
# print(byke.head(10))

# normalize names
byke['Station'] = byke['Station'].apply(lambda name: station_name.names[name])


for cle, df in byke.groupby('Station'):
    # print(cle, df)
    df = df.set_index('Timestamp').resample('10min', label='right', closed='right').last().dropna().reset_index()
    # print(df.head(5))
