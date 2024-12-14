import xarray as xr
import xclim

import time
import s3fs
import fsspec
import csv
from typing import List

from src.pipeline import CalcConfig


def calc(ds: xr.Dataset, config: CalcConfig) -> xr.Dataset:

    target_chunks = {
        "time": config.time_chunk,
        "lat": config.lat_chunk,
        "lon": config.lon_chunk,
    }

    # Re-chunk directly as desired
    ds = ds.chunk(target_chunks)
    
    # Select DataArrays for calculation
    tas = ds.tasmax
    pr = ds.pr
    hurs = ds.hurs
    sfcWind = ds.sfcWind
    lat = ds.lat

    # These can be None
    ffmc0 = config.initial_conditions.ffmc
    dmc0 = config.initial_conditions.dmc
    dc0 = config.initial_conditions.dc

    out_fwi = xclim.indicators.atmos.cffwis_indices(
        tas=tas,
        pr=pr,
        hurs=hurs,
        sfcWind=sfcWind,
        lat=lat,
        ffmc0=ffmc0,
        dmc0=dmc0,
        dc0=dc0,
        season_method=None,
        overwintering=False,
    )

    names = ["dc", "dmc", "ffmc", "isi", "bui", "fwi"]
    ds_fwi = xr.Dataset({name: da for name, da in zip(names, out_fwi)})
    return ds_fwi

def load(config: CalcConfig) -> xr.Dataset:
    fs = fsspec.filesystem("s3", anon=True)
    ds = xr.open_mfdataset(
        [fs.open(path, mode="rb") for path in config.input_uris],
        engine="h5netcdf",
        decode_times=True,
        combine="by_coords",
        chunks="auto"
    )
    # Persisting the bounded dataset into cluster memory (if memory allows)
    if config.bbox:
        ds = ds.sel(lat=slice(config.bbox.y_min, config.bbox.y_max),
                    lon=slice(config.bbox.x_min, config.bbox.x_max))

    return ds.persist()


def main(config: CalcConfig):

    config_start_time = time.time()

    ds = load(config)

    # Perform calculation (no additional rechunk step separately, done inside calc)
    ds_fwi = calc(ds, config)

    # Writing results to S3 as Zarr
    try:
        fs = s3fs.S3FileSystem(anon=False)
        # Let to_zarr() handle the computation
        ds_fwi.to_zarr(
            store=s3fs.S3Map(root=config.zarr_output_uri, s3=fs),
            mode="w",
            consolidated=False,
        )

    except Exception as e:
        print(f"Error writing to s3: {str(e)}")
        raise ValueError
    
    try:
        ds_fwi.close()
        ds.close()
        del ds_fwi, ds
    except Exception as e:
        print(f"Trouble cleaning up ds: {str(e)}")

    config_elapsed_time = time.time() - config_start_time
    print(
        f"{config.zarr_output_uri} completed in {config_elapsed_time:.2f} seconds."
    )
