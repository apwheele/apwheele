'''
Script to save Dallas PD
data as smaller parquet
for use in live dashboard
templates

Andy Wheeler
'''

import pandas as pd
import re

########################
# Grab the data, not sure if "&api_foundry=true" makes any difference
url = 'https://www.dallasopendata.com/api/views/qv6i-rri7/rows.csv?accessType=DOWNLOAD'
res = pd.read_csv(url)

# This is lower memory, not sure if any faster
#import requests
#from io import BytesIO
#res = requests.get(url,stream=True)
#res.raw.decode_content = True
#df = pd.read_csv(BytesIO(res.text.encode('utf-8')))
########################


########################
# Cleaning up the data

# Replacing names
rename_dict = {}
for v in list(res):
    rename_dict[v] = re.sub('[^0-9a-zA-Z]+', '_',v.strip()).lower()

res.rename(columns=rename_dict,inplace=True)

# Only incidents at most prior two years
today = pd.to_datetime('now')
prior = today - pd.DateOffset(years=2) #or pd.Timedelta(days=365*2)
year_end = prior.year
res = res[res['year_of_incident'] >= year_end].copy()

# Limit the columns, begin/end time
keep_col = ['incident_number_w_year','nibrs_crime_category','type_location',
            'date1_of_occurrence','time1_of_occurrence','date2_of_occurrence','time2_of_occurrence',
            'location1']
res = res[keep_col]

# Filtering misc and other low categories
tot_cat = res['nibrs_crime_category'].value_counts()
tot_cat = tot_cat[~tot_cat.index.isin(['MISCELLANEOUS'])]
tot_cat = tot_cat[tot_cat > 500].index.tolist()
res = res[res['nibrs_crime_category'].isin(tot_cat)].copy()

# Creating begin/end times
res['begin'] = pd.to_datetime(res['date1_of_occurrence'].str[:10] + " " + res['time1_of_occurrence'])
res['end'] = pd.to_datetime(res['date2_of_occurrence'].str[:10] + " " + res['time2_of_occurrence'])
res = res[res['begin'] >= prior].copy()

# Parsing Location + Lat/Lon
add_df = res['location1'].str.split("\n",expand=True)
res['address'] = add_df[0]
lat_lon = add_df[2].str[1:-1].str.split(",",expand=True)
res['lat'] = pd.to_numeric(lat_lon[0])
res['lon'] = pd.to_numeric(lat_lon[1])

# Only needed final variables
keep2 = ['incident_number_w_year','nibrs_crime_category','type_location','begin','end','address','lat','lon']
res = res[keep2]
res.columns = ['incident_number','nibrs_cat','location','begin','end','address','lat','lon']
########################

########################
# Save as zip compression

res.to_csv('dallasdata.csv.zip',compression="zip",index=False)
#res.to_parquet('test.parquet.gzip',compression='gzip',engine='pyarrow')

# Then to read it is
#csv1 = 'https://github.com/apwheele/apwheele/blob/master/dallasdata.csv.zip?raw=true'
#res = pd.read_csv(csv1,compression='zip')
# Much snappier than reading directly from Socrata
########################