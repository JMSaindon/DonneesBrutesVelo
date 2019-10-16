import os
import sys
import shutil
import pandas as pd
from dateutil import parser
import traceback
import zipfile
from tqdm import tqdm

from joblib.memory import Memory

with zipfile.ZipFile('brut.zip') as myzip:
    with myzip.open('brut/weather_bicincitta_parma.csv') as myfile:
        weather = pd.read_csv(myfile, nrows=1000, delimiter=';', header=None, names=["Timestamp", "Status", "Clouds", "Humidity", "Pressure", "Rain", "WindGust", "WindVarEnd", "WindVarBeg", "WindDeg", "WindSpeed", "Snow", "TemperatureMax", "TemperatureMin", "TemperatureTemp"])
    with myzip.open('brut/status_bicincitta_parma.csv') as myfile:
        velo = pd.read_csv(myfile, nrows=1000)
    with myzip.open('brut/bicincitta_parma_summary.csv') as myfile:
        stations = pd.read_csv(myfile, delimiter=';')

# drop useless columns
weather = weather.drop(columns = ['Clouds', 'WindGust', 'WindVarEnd', 'WindVarBeg', 'TemperatureMax', 'TemperatureMin'])

def valid_datetime(d):
    try:
        return parser.parse(d)
    except:
        return None

# parse datetimes
weather['Timestamp'] = weather['Timestamp'].apply(valid_datetime)

# print(weather.head(10))
# print(weather.columns)

weather = weather.set_index('Timestamp').resample('10min', label='right', closed='right').last().dropna().reset_index()

# print(weather.head(10))
# print(weather.columns)


# merging step
pathStations = "./Stations"
if not os.path.exists(pathStations):
    os.mkdir(pathStations)

# pas testé vu que pas de liste de dataframe par station (pas encore)
Stations = []

# for station in Stations:
#     pathSpecificStation = pathStations + '/' + station[0]['Station'][4:].replace(' ', '_')
#     if not os.path.exists(pathSpecificStation):
#         os.mkdir(pathSpecificStation)
#         station.to_csv(pathSpecificStation + '/1', compression='gzip') # a modifier car on doit créer différents fichiers pour es série temporelles




