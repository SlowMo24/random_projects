#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#%%
import ohsome
import bar_chart_race as bcr
import geopandas
import pandas
import shapely
import pathlib
from tqdm import tqdm

filepath=pathlib.Path(__file__).resolve().parent
#%%

countries=geopandas.read_file(filepath/'ne_110m_admin_0_countries'/'ne_110m_admin_0_countries.shp')

#reduce wrong float precision
countries['geometry']=countries['geometry'].apply(lambda x:shapely.wkt.loads(shapely.wkt.dumps(x, rounding_precision=7)).simplify(0))
countries['geometry']=countries['geometry'].buffer(0)

minx=0
miny=0
maxx=0
maxy=0
for index,row in countries.iterrows():
    b=row.geometry.bounds
    if b[0]<minx:
        minx=b[0]
    if b[1]<miny:
        miny=b[1]
    if b[2]>maxx:
        maxx=b[2]
    if b[3]>maxy:
        maxy=b[3]
        
print((minx,miny,maxx,maxy))

#limit to x countries for testing
#countries=countries.head(2)

n = 1  #chunk row size
list_df = [countries[i:i+n] for i in range(0,countries.shape[0],n)]

client = ohsome.OhsomeClient()
df=pandas.DataFrame(columns = ['date','value','country','population'])

errorCountries=list()
for row in tqdm(list_df):
    try:
        response = client.contributions.count.post(bpolys=row, time="//P1M",  filter="type:node")
        response_df = response.as_dataframe()
        response_df=response_df.reset_index()
        response_df.rename(columns = {'toTimestamp':'date'}, inplace = True)
        response_df=response_df.drop('fromTimestamp',axis=1)
        response_df['country']=row.iloc[0].SOVEREIGNT
        response_df['population']=row.iloc[0].POP_EST
        
        df=df.append(response_df)
    except:
        print(row.iloc[0].SOVEREIGNT)
        errorCountries.append(row.iloc[0].SOVEREIGNT)



with open(filepath/'errorCountries.txt','w')as out:
    out.write('\n'.join(errorCountries))
    
df=df.convert_dtypes()

df=df.groupby(by=['date','country'],as_index=False).sum()

csvout=filepath/'OSM_Contribs.csv'
df.to_csv(csvout)

#%%
df=pandas.read_csv(csvout)

df['date']=pandas.to_datetime(df['date'])

# by population
df_pop=df.copy()

#%%
df_pop['value']=100000*df_pop['value']/df_pop['population']
df_pop=df_pop.drop('population',axis=1)
df_pop=df_pop.pivot(index='date',columns='country', values='value')

df_pop=df_pop.cumsum()

bcr.bar_chart_race(
    df=df_pop,
    filename=str(filepath/'ohsome_country_race_pop.mp4'),
    orientation='h',
    sort='desc',
    n_bars=20,
    fixed_order=False,
    fixed_max=False,
    steps_per_period=10,
    interpolate_period=False,
    label_bars=True,
    bar_size=.95,
    period_label={'x': .99, 'y': .25, 'ha': 'right', 'va': 'center'},
    period_fmt='%B %Y',
    perpendicular_bar_func='median',
    period_length=500,
    figsize=(5, 3),
    dpi=144,
    cmap='dark12',
    title='OSM tagged node edits per country per 100k inhabitants',
    title_size='',
    bar_label_size=7,
    tick_label_size=7,
    shared_fontdict={'family' : 'Helvetica', 'color' : '.1'},
    scale='linear',
    writer=None,
    fig=None,
    bar_kwargs={'alpha': .7},
    filter_column_colors=True)  

#%%
df=df.drop('population',axis=1)

df=df.pivot(index='date',columns='country', values='value')

df=df.cumsum()
bcr.bar_chart_race(
    df=df,
    filename=str(filepath/'ohsome_country_race.mp4'),
    orientation='h',
    sort='desc',
    n_bars=20,
    fixed_order=False,
    fixed_max=False,
    steps_per_period=10,
    interpolate_period=False,
    label_bars=True,
    bar_size=.95,
    period_label={'x': .99, 'y': .25, 'ha': 'right', 'va': 'center'},
    period_fmt='%B %Y',
    period_summary_func=lambda v, r: {'x': .99, 'y': .18,
                                  's': f'Total edits: {v.nlargest(6).sum():,.0f}',
                                  'ha': 'right', 'size': 8, 'family': 'Courier New'},
    perpendicular_bar_func='median',
    period_length=500,
    figsize=(5, 3),
    dpi=144,
    cmap='dark12',
    title='OSM tagged node edits per country',
    title_size='',
    bar_label_size=7,
    tick_label_size=7,
    shared_fontdict={'family' : 'Helvetica', 'color' : '.1'},
    scale='linear',
    writer=None,
    fig=None,
    bar_kwargs={'alpha': .7},
    filter_column_colors=True)  
