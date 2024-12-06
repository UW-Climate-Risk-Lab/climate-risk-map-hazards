#! python
#from pyFWI import *
from FWIFunctions import *
import sys, re, datetime

import pandas as pd
import numpy as np

import xarray as xr

import xclim
import time


'''
Calculate the fire weather index using downscaled CMIP6 data
- Data: NEX-GDDP-CMIP6 https://www.nccs.nasa.gov/services/data-collections/land-based-products/nex-gddp-cmip6
### Input fields
- The input variables required from the original CFWIS paper are Ts, wind speed, relative humidity (RH) at local noon, and 24-hour accumulated precipitation ending at local noon. 
- Since the NEX-GDDP-CMIP6 dataset provides only daily values, proxies were used for these variables, following the methodology outlined in https://gmd.copernicus.org/articles/16/3103/2023/. Specifically, the following daily values were used as proxies for noon conditions:
    - Maximum temperature (°C)
    - Mean wind speed (m/s)
    - Minimum relative humidity (%) [note that in NEX-GDDP-CMIP6, the relative humidity might be daily mean]
    - Total precipitation (mm/day)
- This approach ensures consistency with the available data while approximating the required noon conditions.
'''

## Precipitation; and change its units to mm/day
path = '/data_test'
ds_prec = xr.open_dataset('/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/data_test/pr_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc') ## prec is 
prec = ds_prec.pr

## daily maximum temperature; and change its units to degC. 
ds_tas = xr.open_dataset('/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/data_test/tas_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc') ## prec is 
tas = ds_tas.tas

## Relative humidity
ds_RH = xr.open_dataset('/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/data_test/hurs_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc') ## prec is 
rh = ds_RH.hurs

## Wind speed
ds_ws = xr.open_dataset('/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/data_test/sfcWind_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc') ## prec is 
ws = ds_ws.sfcWind


'''
Parameter set up. Based on https://xclim.readthedocs.io/en/stable/_modules/xclim/indices/fire/_cffwis.html#cffwis_indices, it looks that
- I can calculate the fire season internally in the code and turn off the winter season and restart in spring. Or I can set "season_method = None" where FWI in winter season is also calculated. 
- Overwintering can be used, which means the last DC of the season will be kept in winter, and precipitaiton is accumulated until the start of next fire season. I'm not sure how much this will impact the FWI of the fire season. 
- I can set "dry start = True" for a dry spring start. Note that overwintering overrides the dry start, where overwintering has precipitation accumulated until the next season. But I'm not sure when overwintering = False, how turn on dry start would impact.  

I will need a longer time series to use fire_season and over_wintering.
'''

start_time = time.time()

season_mask = xclim.indicators.atmos.fire_season(
            tas=tas,
            method="WF93",
            freq="YS",
    )
out_fwi = xclim.indicators.atmos.cffwis_indices(
        tas=tas,
        pr=prec,
        hurs=rh,
        sfcWind=ws,
        lat=ws.lat,
        #season_mask=season_mask,
        season_method = None,
        #dry_start = None,
        #initial_start_up = False,
        overwintering=False,
    )

end_time = time.time()

print(f"Execution time: {end_time - start_time:.2f} seconds")

names = ['dc','dmc','ffmc','isi','bui','fwi']
import xarray as xr
start_time = time.time()
da_out = xr.Dataset({name: da for name, da in zip(names, out_fwi)})
da_out.to_netcdf("test_fwi.nc")
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")
