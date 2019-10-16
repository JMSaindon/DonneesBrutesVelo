import os
import sys
import shutil
import pandas as pd
from dateutil import parser
import traceback
import zipfile

from joblib.memory import Memory

with zipfile.ZipFile('brut.zip') as myzip:
    with myzip.open('brut/weather_bicincitta_parma.csv') as myfile:
        weather = pd.read_csv(myfile, nrows=1000)
    with myzip.open('brut/status_bicincitta_parma.csv') as myfile:
        velo = pd.read_csv(myfile, nrows=1000)
    with myzip.open('brut/bicincitta_parma_summary.csv') as myfile:
        stations = pd.read_csv(myfile)



