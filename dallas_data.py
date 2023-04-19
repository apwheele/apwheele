'''
Script to save Dallas PD
data as smaller parquet
for use in live dashboard
templates

Andy Wheeler
'''

import numpy as np
import pandas as pd
import re

########################
# Grab the data, not sure if "&api_foundry=true" makes any difference
url = 'https://www.dallasopendata.com/api/views/qv6i-rri7/rows.csv?accessType=DOWNLOAD'
res = pd.read_csv(url)
res2 = res.copy()

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
prior = today - pd.DateOffset(years=3) #or pd.Timedelta(days=365*2)
year_end = prior.year
res = res[res['year_of_incident'] >= year_end].copy()

# Limit the columns, begin/end time
keep_col = ['incident_number_w_year','nibrs_crime_category','type_location',
            'date1_of_occurrence','time1_of_occurrence','date2_of_occurrence','time2_of_occurrence',
            'location1']
res = res[keep_col]

# Filtering misc and other low categories
#tot_cat = res['nibrs_crime_category'].value_counts()
#tot_cat = tot_cat[~tot_cat.index.isin(['MISCELLANEOUS'])]
#tot_cat = tot_cat[tot_cat > 500].index.tolist()
#res = res[res['nibrs_crime_category'].isin(tot_cat)].copy()

# Mapping NIBRS categories
nibr_num = {'LARCENY/ THEFT OFFENSES': 0, 
            'MOTOR VEHICLE THEFT': 1, 
            'DESTRUCTION/ DAMAGE/ VANDALISM OF PROPERTY': 2, 
            'ASSAULT OFFENSES': 3, 
            'DRUG/ NARCOTIC VIOLATIONS': 4, 
            'BURGLARY/ BREAKING & ENTERING': 5, 
            'ALL OTHER OFFENSES': 6, 
            'TRAFFIC VIOLATION - HAZARDOUS': 7, 
            'ROBBERY': 8, 
            'PUBLIC INTOXICATION': 9, 
            'WEAPON LAW VIOLATIONS': 10, 
            'FRAUD OFFENSES': 11, 
            'DRIVING UNDER THE INFLUENCE': 12, 
            'TRESPASS OF REAL PROPERTY': 13, 
            'FAMILY OFFENSES, NONVIOLENT': 14, 
            'STOLEN PROPERTY OFFENSES': 15, 
            'EMBEZZELMENT': 16, 
            'COUNTERFEITING / FORGERY': 17}

res = res[res['nibrs_crime_category'].isin(nibr_num.keys())].copy()
res['nibrs_crime_category'] = res['nibrs_crime_category'].replace(nibr_num)

# Creating begin/end times
res['begin'] = res['date1_of_occurrence'].str[:10] + " " + res['time1_of_occurrence']
res['end'] = res['date2_of_occurrence'].str[:10] + " " + res['time2_of_occurrence']
res = res[pd.to_datetime(res['begin']) >= prior].copy()

# Parsing Location + Lat/Lon
add_df = res['location1'].str.split("\n",expand=True)
res['address'] = add_df[0]
lat_lon = add_df[2].str[1:-1].str.split(",",expand=True)
res['lat'] = pd.to_numeric(lat_lon[0])
res['lon'] = pd.to_numeric(lat_lon[1])


# Having a reduced set of location categories
loc_map = {'Apartment Complex/Building': 1,
'Apartment Residence': 1,
'Condominium/Townhome Residence': 1,
'Condominium/Townhome Building': 1,
'Bar/NightClub/DanceHall ETC.': 2,
'Restaurant/Food Service/TABC Location': 2,
'Business Office': 3,
'Commercial Property Occupied/Vacant': 3,
'Storage Facility': 3,
'Auto Dealership New/Used': 3,
'Medical Facility': 3,
'Bank/Savings And Loan': 3,
'Government Facility': 3,
'Industrial/Manufacturing': 3,
'Government/Public Building': 3,
'Abandoned/Condemned Structure': 3,
'Farm Facility': 3,
'Gas or Service Station': 4,
'Convenience Store': 4,
'Hotel/Motel/ETC': 5,
'Cyberspace': 6,
'Other': 6,
'Construction Site': 6,
'Airport - Love Field': 6,
'Church/Synagogue/Temple/Mosque': 6,
'Community/ Recreation Center': 6,
'Shelter - Mission/Homeless': 6,
'Airport - All Others': 6,
'Jail/Prison/Penitentiary/Corrections Fac': 6,
'Personal Services': 6,
'Arena/Stadium/Fairgrounds/Coliseum': 6,
'Dock/Wharf/Freight/Modal Terminal': 6,
'Amusement Park': 6,
'Religious Institution': 6,
'Military Installation': 6,
'Outdoor Area Public/Private': 7,
'Park': 7,
'Field/Woods': 7,
'Lake/Waterway/Beach': 7,
'Camp/Campground': 7,
'Trails': 7,
'Rest Area': 7,
'Playground': 7,
'Tribal Lands': 7,
'Parking (Business)': 8,
'Apartment Parking Lot': 8,
'Parking Lot (All Others)': 8,
'Condominium/Townhome Parking': 8,
'Parking Lot (Park)': 8,
'Parking Lot (Apartment)': 8,
'Single Family Residence - Occupied': 1,
'Single Family Residence - Vacant': 1,
'School - Elementary/Secondary': 9,
'Daycare Facility': 9,
'School - College/University': 9,
'School/College': 9,
'School/Daycare': 9,
'Retail Store': 10,
'Grocery/Supermarket': 10,
'Department/Discount Store': 10,
'Shopping Mall': 10,
'Entertainment/Sports Venue': 10,
'Specialty Store (In a Specific Item)': 10,
'Pharmacy': 10,
'Drug Store/Doctors Office/Hospital': 10,
'Financial Institution': 10,
'Liquor Store': 10,
'Rental Storage Facility': 10,
'Gambling Facility/Casino/Race Track': 10,
'ATM Separate from Bank': 10,
'Highway, Street, Alley ETC': 11}

all_loc = set(pd.unique(res['type_location']))
for i in all_loc:
    if i not in loc_map:
        print(i)
        loc_map[i] = 6 # other category

res['type_location'] = res['type_location'].replace(loc_map)

# The final dictionary is
# loc_label = {1: 'Apartment/Residence',
#              2: 'Bar/Restaurant',
#              3: 'Commercial',
#              4: 'Gas/Convenience',
#              5: 'Hotel/Motel',
#              6: 'Other',
#              7: 'Outdoor',
#              8: 'Parking Lot',
#              9: 'School',
#             10: 'Store',
#             11: 'Street'}

# Only needed final variables
keep2 = ['incident_number_w_year','nibrs_crime_category','type_location','begin','end','address','lat','lon']
res = res[keep2]
res.columns = ['incident_number','nibrs_cat','location','begin','end','address','lat','lon']

res['nibrs_cat'] = res['nibrs_cat'].astype(np.ubyte)
res['location'] = res['nibrs_cat'].astype(np.ubyte)
res['lat'] = res['lat'].astype(np.single)
res['lon'] = res['lon'].astype(np.single)
res.reset_index(drop=True,inplace=True)
########################

########################
# Save as zip compression

res.drop(columns=['incident_number'],inplace=True) # saves another 1/2 mb
res.to_csv('dallasdata.csv.zip',compression="zip",index=False)
# This saves a few mb over zipped csv
#res.to_parquet('dallasdata.parquet.gzip',compression='gzip',engine='pyarrow')

# Then to read it is
#csv1 = 'https://github.com/apwheele/apwheele/blob/main/dallasdata.csv.zip?raw=true'
#res = pd.read_csv(csv1,compression='zip')
#par1 = 'https://github.com/apwheele/apwheele/blob/main/dallasdata.parquet.gzip?raw=true'
#res = pd.read_parquet(par1,compression='gzip')
# Much snappier than reading directly from Socrata
########################