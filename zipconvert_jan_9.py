#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  2 12:48:58 2019

@author: lefebvre1217
"""

import pandas as pd 
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from uszipcode import SearchEngine
#import uszipcode as uz

import os
#os.chdir('lefebvre1217/desktop/provider_network/pregnancy_data/')

dZip = pd.read_csv('~/desktop/provider_network/pregnancy_data/dZip.txt')
del df

colnames = ('zip')
df = pd.read_table('~/desktop/provider_network/pregnancy_data/dZip.txt', delim_whitespace=True, 
                   dtype={'A': np.int64})

df = pd.read_csv('~/desktop/provider_network/pregnancy_data/dZip.txt', converters={'Zip': lambda x: str(x)})
print (df)
df.dtypes
D = df['Zip'].astype(str).astype(int)
pd.to_numeric(df.Zip)

for c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce') 

del df
dZip.isnull().values.any()
dZip = dZip.dropna()



df.names()

for index, row in df.iterrows():
    print(row['Zip'])

search = SearchEngine(simple_zipcode=True)
zipcode = []
#df = pd.read_csv('LATLONG.csv')
for index, row in df.iterrows():
    zipcode = search.by_zipcode(row["Zip"])
    
zipcode.values() # to list
zipcode.bounds_west

## WORKED AT 12:30 JAN 8th MAYBE CHANGING Simple_Zipcode = True in searchengine
search = SearchEngine(simple_zipcode=True)
#search = SearchEngine(simple_zipcode=False)
zipcode = []
d = []
#df = pd.read_csv('LATLONG.csv')
for index, row in df.iterrows():
    zipcode = search.by_zipcode(row["Zip"])




x = zipcode.values # to list
zipcode.bounds
x.dtypes
from pandas import DataFrame
DataFrame.to_records(x)

zipcode.zipcode


def get_lat_lng(x,y):
    lat = x
    lng = y
    return lat, lng


def convert_to_lat_lng(x):
    z = df.Zip
    res = search.by_zipcode(x)
    lat = res.lat
    lng = res.lng
    return lat,lng

y = convert_to_lat_lng(df.Zip)

def get_state(path):
    with open(path + "USA_downloads.csv", 'r+') as f:
        data = pd.read_csv(f)
        data["state"] = np.vectorize(convert_to_state)(data["latitude"].values, data["longitude"].values)

get_state(path)



def convert_to_state(lat, lon):
    lat, lon = round(lat, 7), round(lon, 7)
    res = search.by_coordinate(lat, lon, radius=1, returns=1)
    state = res.State
    return state

def get_state(path):
    with open(path + "USA_downloads.csv", 'r+') as f:
        data = pd.read_csv(f)
        data["state"] = np.vectorize(convert_to_state)(data["latitude"].values, data["longitude"].values)

import os
path = '~/Desktop/provider_network'

def convert_to_lat_lng(x):
    z = x
    res = search.by_zipcode(z)
    lat, lng = res.lat, res.lng
    zipcode = res.zipcode
    city    = res.city
    state   = res.state
    e_bound = res.bounds_east
    w_bound = res.bounds_west
    s_bound = res.bounds_south
    n_bound = res.bounds_north
    tzone   = res.timezone
    return lat, lng, zipcode, city, state, e_bound, w_bound, s_bound, n_bound, tzone

def get_coord(path):
    with open(path + "tester.csv", 'r+') as f:
        data = pd.read(f)
        data["coordinates"] = np.vectorize(convert_to_lat_lng)(data['Zip'].values, data['zip'].values)
    
    
zipcode.time
dZip = df['Zip']
#abc = convert_to_lat_lng(dZip)

abc = np.vectorize(convert_to_lat_lng)(dZip.values)
type(abc)
tuple(abc)

bcd = np.vectorize(convert_to_lat_lng)(dZip.values)
type(bcd)
bcd = list(bcd)
type(bcd)
bcd = pd.DataFrame(bcd)
bcd = bcd.T

#oboy = np.vectorize(convert_to_lat_lng)(dZip.values)
oboy = list(abc)

xyz = list(abc)
xyz = pd.DataFrame(list(abc))
xyz1 = xyz.T
xyz1.columns = ['lat', 'lng', 'zipcode','city','state', 'e_bound','w_bound','s_bound','n_bound','tzone']

df_cleaned = xyz1.dropna(how='all')
df_cleaned
df_cleaned = df_cleaned.reset_index(drop=True)

import datetime
def _getToday():
    return datetime.datetime.now().strftime('%Y-%m-%d-%H:%M') #:%S')    #("%Y%m%d") #%I:%M:%S %p")
filename = "%s_%s.%s" % ("", _getToday() ,"csv")

df_cleaned.to_csv('/users/lefebvre1217/Desktop/provider_network/zip' + filename, sep=',', index=False)

