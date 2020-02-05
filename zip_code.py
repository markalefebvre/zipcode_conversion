# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 16:45:12 2020

@author: mlefebvre
"""

import pyodbc 
import pandas as pd
import numpy as np
from datetime import datetime 
from uszipcode import SearchEngine


def _getToday():
    return datetime.now().strftime("%Y_%m_%d_%HH%MM%SS")

cnxn = pyodbc.connect(
'Driver={ODBC Driver 13 for SQL Server};'
'Server=tcp:hor-p-adw002.database.windows.net,1433;'
'Database=hor_com_p_201809_38_001;'
'Uid={hqxanalytic};'
'Pwd={2jdc2fjshGRUuLDMsxxO};'
'Encrypt=yes;'
'TrustServerCertificate=no;'
'Connection Timeout=30;'
#'Authentication=ActiveDirectoryPassword'
#, autocommit=True)
)

pos = ("""\
       
    select a.memberid, m.gender, m.birthyear,
    		case when m.zip IS NULL then '99999' else m.zip end as zip,
    		1 as target
    	from (
    	select el.memberid, el.episodestarton,
    	row_number() over (partition by el.memberid order by el.episodestarton desc) as rn
    		from hor_com_p_201809_38_001.dw.episodelevel el
    	where el.episodeacronym = 'KNRPL' 
    		and level = 3
    		and el.TriggerClaimType IN ('In-Patient', 'Out-Patient')
    	) a
    join hor_com_p_201809_38_001.stage.member m
    	on m.memberid = a.memberid
    where a.rn = 1
    """)
        
pos_df = pd.read_sql(pos, cnxn)
pos_org = pos.copy()

pos_df = pos_df[['zip']]
#pos_df = pos_df['zip'].astype(str).astype(int)
for c in pos_df.columns: pd.to_numeric(pos_df[c], errors = 'coerce')

search = SearchEngine(simple_zipcode=True)
zipcode = []

for index, row in pos_df.iterrows():
    zipcode = search.by_zipcode(row["zip"])
    
def convert_to_lat_lng(x):
    z = x
    res = search.by_zipcode(z)
    lat,lng = res.lat,res.lng
    zipcode = res.zipcode
    city = res.city
    state = res.state
    e_bound = res.bounds_east
    w_bound = res.bounds_west
    s_bound = res.bounds_south
    n_bound = res.bounds_north
    t_zone = res.timezone
    return lat,lng,zipcode,city,state,e_bound,w_bound,s_bound,n_bound,t_zone

vec_values = np.vectorize(convert_to_lat_lng)(pos_df['zip'].values)
vec_df = pd.DataFrame(list(vec_values)).T

vec_df.rename(columns={0:'lat',1:'lng',2:'zip',3:'city',4:'state',5:'e_bound',6:'w_bound',7:'s_bound',8:'n_bound',9:'t_zone'},inplace=True)

pos_demo_df = pd.merge(pos_org,vec_df, on ='zip')
pos_org.merge(vec_df, on='zip',how='left')