from distributed import Client

import xarray as xr
import xclim

import time

# Depending on your workstation specifications, you may need to adjust these values.
# On a single machine, n_workers=1 is usually better.


HURS_PATH = "/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/input/hurs_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc"
PR_PATH = "/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/input/pr_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc"
TAS_PATH = "/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/input/tas_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc"
SFC_WIND_PATH = "/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/input/sfcWind_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc"

def main():
    client = Client(n_workers=4, threads_per_worker=2, memory_limit="3GB")
    start_time = time.time()
    #ds = xr.open_mfdataset(paths=[HURS_PATH, PR_PATH, TAS_PATH, SFC_WIND_PATH], chunks={"lat": 'auto', "lon": 'auto', "time": -1})
    ds = xr.open_zarr("/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/output_rechunked_4.zarr")
    out_fwi = xclim.indicators.atmos.cffwis_indices(
        tas=ds.tas,
        pr=ds.pr,
        hurs=ds.hurs,
        sfcWind=ds.sfcWind,
        lat=ds.lat,
        season_method = None,
        overwintering=False,
    )

    names = ['dc','dmc','ffmc','isi','bui','fwi']
    da_out = xr.Dataset({name: da for name, da in zip(names, out_fwi)})
    da_out.to_zarr("output.zarr")
    end_time = time.time()
    print(f"Total time: {end_time - start_time:.2f} seconds")



if __name__ == "__main__":
    main()