import os
import sys
import shutil
import pandas as pd
from dateutil import parser
import traceback
import zipfile

from joblib.memory import Memory

with zipfile.ZipFile('spam.zip') as myzip:
    with myzip.open('eggs.txt') as myfile:
        print(myfile.read())