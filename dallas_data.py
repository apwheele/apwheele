'''
Script to save Dallas PD
data as smaller zipped csv
for use in live dashboard
templates

Andy Wheeler
'''

from datetime import datetime
import numpy as np
import pandas as pd
import requests
import re

########################
# Grab the data, not sure if "&api_foundry=true" makes any difference
#url = 'https://www.dallasopendata.com/api/views/qv6i-rri7/rows.csv?accessType=DOWNLOAD'
#res = pd.read_csv(url)

# newer Socrata does not have a limit
def query_socrata(base,add_params):
    # Get the total number of items to query
    tot_query = base + add_params + "&$group=&$select=count(*)%20AS%20tot"
    #print(tot_query)
    # the tot query *NEEDS* to be json format
    res_tot = requests.get(tot_query)
    #print(res_tot.text)
    totn = int(res_tot.json()[0]['tot'])
    # with Socrata, can query the whole data
    whole_query = base + add_params + f'&$limit={totn}'
    #print(whole_query)
    res = requests.get(whole_query)
    data = pd.DataFrame(res.json())
    return data

curr_year = datetime.now().year
prior4 = datetime.now().year - 4
res = query_socrata('https://www.dallasopendata.com/resource/qv6i-rri7.json',f'?$where=servyr > {prior4}')

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

# Only incidents at most prior three years
today = pd.to_datetime('now')
prior = today - pd.DateOffset(years=3) #or pd.Timedelta(days=365*3)

# Limit the columns, begin/end time
keep_col = ['incidentnum','nibrs_crime_category','premise',
            'date1','time1','date2_of_occurrence_2','time2',
            'geocoded_column','incident_address']
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
res['begin'] = res['date1'].str[:10] + " " + res['time1']
res['end'] = res['date2_of_occurrence_2'].str[:10] + " " + res['time2']
res = res[pd.to_datetime(res['begin']) >= prior].copy()
# instead of saving as datetime, leave as strings with no seconds

# Parsing Location + Lat/Lon
def parse_info(x):
    d = x[0]
    if 'latitude' in d:
        lat = float(d['latitude'])
    else:
        lat = None
    if 'longitude' in d:
        lon = float(d['longitude'])
    else:
        lon = None
    return [lat,lon]

res = res[~res['geocoded_column'].isna()].reset_index(drop=True)
add_df = res[['geocoded_column']].apply(parse_info,axis=1,result_type='expand')
res['lat'] = add_df[0]
res['lon'] = add_df[1]
res = res[~res['lat'].isna()].reset_index(drop=True)
res.rename({'incident_address':'address'},inplace=True)

# Having a reduced set of location categories
loc_map = {'Highway, Street, Alley ETC': 0,
'Apartment Complex/Building': 1,
'Apartment Residence': 1,
'Condominium/Townhome Residence': 1,
'Condominium/Townhome Building': 1,
'Single Family Residence - Occupied': 1,
'Single Family Residence - Vacant': 1,
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
'Retail Store': 9,
'Grocery/Supermarket': 9,
'Department/Discount Store': 9,
'Shopping Mall': 9,
'Entertainment/Sports Venue': 9,
'Specialty Store (In a Specific Item)': 9,
'Pharmacy': 9,
'Drug Store/Doctors Office/Hospital': 9,
'Financial Institution': 9,
'Liquor Store': 9,
'Rental Storage Facility': 9,
'Gambling Facility/Casino/Race Track': 9,
'ATM Separate from Bank': 9,
'School - Elementary/Secondary': 10,
'Daycare Facility': 10,
'School - College/University': 10,
'School/College': 10,
'School/Daycare': 10}

all_loc = set(pd.unique(res['premise']))
for i in all_loc:
    if i not in loc_map:
        print(i)
        loc_map[i] = 6 # other category

res['premise'] = res['premise'].replace(loc_map)

# The final dictionary is
# loc_label = {0: 'Street',
#              1: 'Apartment/Residence',
#              2: 'Bar/Restaurant',
#              3: 'Commercial',
#              4: 'Gas/Convenience',
#              5: 'Hotel/Motel',
#              6: 'Other',
#              7: 'Outdoor',
#              8: 'Parking Lot',
#              9: 'Store',
#             10: 'School'}

# Only needed final variables
keep2 = ['nibrs_crime_category','premise','begin','end','incident_address','lat','lon']
res = res[keep2]
res.columns = ['nibrs_cat','location','begin','end','address','lat','lon']

# replacing lat/lon for addresses
res.sort_values(by='begin',inplace=True,ignore_index=True)
ll_rep = res[['address','lat','lon']].drop_duplicates(subset='address',keep='last')
res.drop(columns=['lat','lon'],inplace=True)
res = res.merge(ll_rep,how='left',on='address')


# ubyte doesnt do much here for saving as CSV
# but for parquet is useful
res['nibrs_cat'] = res['nibrs_cat'].astype(np.ubyte)
res['location'] = res['location'].astype(np.ubyte)
res['lat'] = res['lat'].astype(np.single)
res['lon'] = res['lon'].astype(np.single)
res.reset_index(drop=True,inplace=True)
########################

########################
# Save as zip compression
res.to_csv('dallasdata.csv.zip',compression="zip",index=False)

# This saves a few mb over zipped csv
#res.to_parquet('dallasdata.parquet.gzip',compression='gzip',engine='pyarrow')

# Then to read it is
#csv1 = 'https://github.com/apwheele/apwheele/blob/main/dallasdata.csv.zip?raw=true'
#res = pd.read_csv(csv1,compression='zip')
# Much snappier than reading directly from Socrata
########################