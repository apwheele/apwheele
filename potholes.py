'''
Script to create pothole
chart, counts per week
from Raleigh open data

Andy Wheeler
'''

from datetime import datetime as dt
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import requests

# Personal plot theme
# https://andrewpwheeler.com/2020/05/05/notes-on-matplotlib-and-seaborn-charts-python/
andy_theme = {'axes.grid': True,
              'grid.linestyle': '--',
              'legend.framealpha': 1,
              'legend.facecolor': 'white',
              'legend.shadow': True,
              'legend.fontsize': 14,
              'legend.title_fontsize': 16,
              'xtick.labelsize': 14,
              'ytick.labelsize': 14,
              'axes.labelsize': 16,
              'axes.titlesize': 20,
              'figure.dpi': 100}
 
matplotlib.rcParams.update(andy_theme)

# Where to query data
# Not this has a limit of 32k, so eventually will need to be changed
ph = r'https://services.arcgis.com/v400IkDOw1ad7Yad/arcgis/rest/services/CityWorks_Pothole_Data/'
ph += r'FeatureServer/0/query?where=1%3D1&outFields=*&resultType=standard&f=json'

response = requests.post(ph)
rj = response.json()

new_di = [r['attributes'].values() for r in rj['features']]
new_col = rj['features'][0]['attributes'].keys()

pdat = pd.DataFrame(new_di,columns=new_col)
pdat['start'] = pd.to_datetime(pdat['actual_start_date']/1000,unit='s')

# Calculating counts per week
def per_week(items,base_date,end_date=None):
    # Converting to weeks since base-date
    bd = pd.to_datetime(base_date)
    diff = items - bd
    # getting rid of missing
    diff = diff[~diff.isna()].copy()
    weekn = pd.DataFrame(np.floor(diff.dt.days/7))
    weekn.columns = ['week']
    weekn['week'] = weekn['week'].astype(int)
    # aggregating counts
    wcounts = pd.DataFrame(weekn['week'].value_counts()).reset_index()
    wcounts.columns = ['week','counts']
    # Expanding out to fill potential 0 weeks
    if not end_date:
        b,e = 0,wcounts['week'].max()
    else:
        ed = pd.to_datetime(end_date)
        diff_end = ed - bd
        end_week = np.floor(diff_end.dt.days/7)
        b,e = 0, end_week
    # Note this will drop the last dangling week
    exp_weeks = pd.DataFrame(range(b,e),columns=['week'])
    mc = exp_weeks.merge(wcounts,on='week',how='left')
    mc['counts'] = mc['counts'].fillna(0).astype(int)
    mc.sort_values(by='week',inplace=True,ignore_index=True)
    # Add in actual date value
    mc['wd'] = bd + pd.to_timedelta(mc['week']*7, unit='D')
    return(mc[['wd','week','counts']])


pc = per_week(pdat['start'],'2018-07-02')

nw = dt.now().strftime("%m/%d/%Y")
fn = f'Data from <https://data.raleighnc.gov/datasets/ral::cityworks-potholes/about>, last update {nw}'

# Making a plot over time
fig, ax = plt.subplots(figsize=(12,6))
ax.plot(pc['wd'], pc['counts'], marker='o', markeredgecolor='w')
ax.set_title('Potholes Filled in Raleigh per week',loc='left')
ax.annotate(fn, (0,0), (0,-30), xycoords='axes fraction', textcoords='offset points', va='top')
plt.savefig('PH_Ral_Week.png', dpi=500, bbox_inches='tight')