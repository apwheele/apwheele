'''
Script to create crashes
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


url = '''https://utility.arcgis.com/usrsvcs/servers/3f503c43a0bd4490be7dec17650bcda9/rest/services/Crash_Reports/FeatureServer/0/query?where=DateOfCrash+>%3D+CAST%28%271%2F1%2F2024%27+AS+date%29&objectIds=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=&returnGeometry=true&returnCentroid=false&returnEnvelope=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&collation=&orderByFields=CAST%28DateOfCrash+AS+VARCHAR%2810%29%29+DESC&groupByFieldsForStatistics=CAST%28DateOfCrash+AS+VARCHAR%2810%29%29&outStatistics=%5B%7BonStatisticField%3A+"CAST%28DateOfCrash+AS+VARCHAR%2810%29%29"%2C%0D%0A++outStatisticFieldName%3A+"TotalCrashes"%2C%0D%0A++statisticType%3A+"count"%2C%0D%0A++%7D%2C%0D%0A%7BonStatisticField%3A+"CAST%28DateOfCrash+AS+VARCHAR%2810%29%29"%2C%0D%0A++outStatisticFieldName%3A+"Date"%2C%0D%0A++statisticType%3A+"min"%2C%0D%0A++%7D%2C%0D%0A%5D&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnTrueCurves=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=json'''

res = requests.get(url)
rd = res.json()
data = pd.DataFrame(x['attributes'] for x in rd['features'])
data['Date'] = pd.to_datetime(data['Date'])

today = pd.to_datetime('now')
lag = today - pd.Timedelta(5,unit='D') # 5 day lag so data in inputted
data = data[data['Date'] <= lag].reset_index(drop=True)

# Calculating counts per week
def per_week(items,date='Date',sumf='TotalCrashes',base_date='2024-01-01',end_date=None):
    # Converting to weeks since base-date
    bd = pd.to_datetime(base_date)
    diff = items[date] - bd
    # getting rid of missing
    diff_df = items[~diff.isna()].copy()
    diff_df['weekn'] = np.floor(diff.dt.days/7)
    diff_df['week'] = diff_df['weekn'].astype(int)
    # aggregating counts
    wcounts = diff_df.groupby('week',as_index=False)[sumf].sum()
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


pc = per_week(data)

nw = dt.now().strftime("%m/%d/%Y")
fn = f'Data from <https://data-ral.opendata.arcgis.com/datasets/ral::reported-crash-locations/about>, last update {nw}'

# Making a plot over time
fig, ax = plt.subplots(figsize=(12,6))
ax.plot(pc['wd'], pc['counts'], color='k', marker='o', markeredgecolor='w', markerfacecolor='k')
ax.set_title('Crashes in Raleigh per week',loc='left')
ax.annotate(fn, (0,0), (0,-30), xycoords='axes fraction', textcoords='offset points', va='top')
plt.savefig('CR_Ral_Week.png', dpi=500, bbox_inches='tight')