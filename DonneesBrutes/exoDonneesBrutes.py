import os
import sys
import shutil
import pandas as pd
from dateutil import parser
import traceback
import zipfile
from tqdm import tqdm

from joblib.memory import Memory

import station_name

with zipfile.ZipFile('brut.zip') as myzip:
    with myzip.open('brut/weather_bicincitta_parma.csv') as myfile:
        weather = pd.read_csv(myfile, delimiter=';', header=None, names=["Timestamp", "Status", "Clouds", "Humidity", "Pressure", "Rain", "WindGust", "WindVarEnd", "WindVarBeg", "WindDeg", "WindSpeed", "Snow", "TemperatureMax", "TemperatureMin", "TemperatureTemp"])
    with myzip.open('brut/status_bicincitta_parma.csv') as myfile:
        bike = pd.read_csv(myfile, sep=';', header=None, names=["Timestamp","Station","Status","Bikes","Slots"])
    with myzip.open('brut/bicincitta_parma_summary.csv') as myfile:
        stations = pd.read_csv(myfile, delimiter=';')

def parse_date(df):
    def valid_datetime(date):
        try:
            return parser.parse(date)
        except ValueError:
            print("Invalid date:", date)
            return None
    df['Timestamp'] = df['Timestamp'].apply(valid_datetime)
    return df

def clean_timestamp(df):
    df = df[df["Timestamp"] != None]
    return df

def clean_bike_data(bike_df):
    bike_df = clean_timestamp(bike_df)
    bike_df = bike_df[bike_df["Status"] == 1]
    return bike_df

# ///////////// Weather ///////////////////

# drop useless columns
weather = weather.drop(columns = ['Clouds', 'WindGust', 'WindVarEnd', 'WindVarBeg', 'TemperatureMax', 'TemperatureMin'])

# parse datetimes
weather = parse_date(weather)
weather = clean_timestamp(weather)
# print(weather.head(10))

weather = weather.set_index('Timestamp').resample('10min', label='right', closed='right').last().dropna().reset_index()
# print(weather.head(10))

# ////////////// Bike ////////////////////

bike = parse_date(bike)
bike = clean_bike_data(bike)
# print(bike.head(10))

# normalize names
bike['Station'] = bike['Station'].apply(lambda name: station_name.names[name])
bike = bike.drop(columns = ['Status'])
bike['Total'] = bike['Bikes'] + bike['Slots']
Stations = []

for cle, df in bike.groupby('Station'):
    # print(cle, df)
    df = df.set_index('Timestamp').resample('10min', label='right', closed='right').last().dropna().reset_index()
    df = df.merge(weather, on='Timestamp')
    Stations.append(df)
    # print(df.head(5))

print('Creation des dossiers et fichiers: ')

# merging step
pathStations = "./Stations"
if not os.path.exists(pathStations):
    os.mkdir(pathStations)

lenStations = len(Stations)
countStation = 0

for station in Stations:
    countStation += 1
    print('Station ' + str(countStation) + '/' + str(lenStations) + ' :')

    pathSpecificStation = pathStations + '/' + station['Station'][0].replace('. ', '_')

    if not os.path.exists(pathSpecificStation):
        os.mkdir(pathSpecificStation)

    counter = 1
    previousTime = station['Timestamp'][0]
    interStation = pd.DataFrame(columns=['Timestamp', 'Station', 'Bikes', 'Slots', 'Total', 'Status', 'Humidity', 'Pressure', 'Rain', 'WindDeg', 'WindSpeed', 'Snow', 'TemperatureTemp'])
    # print('je passe là')

    for t in tqdm(range(len(station['Timestamp']))):

        diff = station['Timestamp'][t] - previousTime
        diffMin = diff.seconds / 60.
        # print(diffMin)

        if(diffMin - 10 > 0.001):
            interStation.to_csv(pathSpecificStation + '/' + str(counter), compression='gzip')
            counter += 1
            interStation = pd.DataFrame(columns=['Timestamp', 'Station', 'Bikes', 'Slots', 'Total', 'Status', 'Humidity', 'Pressure', 'Rain', 'WindDeg', 'WindSpeed', 'Snow', 'TemperatureTemp'])

        interStation = interStation.append(station.loc[t])
        previousTime = station['Timestamp'][t]

    interStation.to_csv(pathSpecificStation + '/' + str(counter) + '.csv.gz', compression='gzip')
