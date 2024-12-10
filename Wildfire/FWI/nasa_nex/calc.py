from distributed import Client
import xarray as xr
import xclim

import time
import os
import shutil
import s3fs
import fsspec
import boto3

from dataclasses import dataclass
import csv


from typing import List
from pipeline import CalcConfig, InitialConditions


def read_csv_from_s3(s3_client, bucket: str, key: str) -> List[List[str]]:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = obj["Body"].read().decode("utf-8").splitlines()
        reader = csv.reader(csv_content)
        return list(reader)
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
        return []


def write_csv_to_s3(s3_client, bucket: str, key: str, rows: List[List[str]]):
    try:
        csv_content = "\n".join([",".join(map(str, row)) for row in rows])
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_content)
    except Exception as e:
        print(f"Error writing CSV to S3: {e}")


def calc(ds: xr.Dataset, config: CalcConfig) -> xr.Dataset:
    
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
    flist = [fs.open(path, mode="rb") for path in config.input_uris]
    ds = xr.open_mfdataset(
        flist, engine="h5netcdf", decode_times=True, combine="by_coords", chunks="auto"
    )
    # Persisting the bounded dataset into cluster memory (if memory allows)
    if config.bbox:
        ds = ds.sel(lat=slice(config.bbox.y_min, config.bbox.y_max),
                    lon=slice(config.bbox.x_min, config.bbox.x_max))
    
    target_chunks = {
        "time": config.time_chunk,
        "lat": config.lat_chunk,
        "lon": config.lon_chunk,
    }

    # Re-chunk directly as desired
    ds = ds.chunk(target_chunks)

    return ds.persist()


def main(s3_client, config: CalcConfig):
    bucket = "uw-crl"
    csv_key = "scratch/dask_results.csv"

    csv_rows = read_csv_from_s3(s3_client, bucket, csv_key)
    if not csv_rows:
        csv_rows.append(
            [   
                "output",
                "n_workers",
                "threads_per_worker",
                "lat_chunk",
                "lon_chunk",
                "load_time",
                "calc_time",
                "write_time",
            ]
        )

    print(f"Running configuration: {config}")

    config_start_time = time.time()

    # Create a single Dask client to be reused
    client = Client(
        n_workers=config.n_workers,
        threads_per_worker=config.threads_per_worker,
        memory_limit="auto",
    )

    # Load data directly from S3 using fsspec and xarray
    # Disable unnecessary decoding to speed up
    start_time = time.time()
    ds = load(config)
    load_time = time.time() - start_time

    # Perform calculation (no additional rechunk step separately, done inside calc)
    try:
        start_time = time.time()
        ds_fwi = calc(ds, config)
        calc_time = time.time() - start_time
    except Exception as e:
        ds_fwi = None
        calc_time = -999
        print(f"{config.zarr_output_uri} calc failed: {e}")

    # Writing results to S3 as Zarr
    if ds_fwi:
        try:
            start_time = time.time()
            fs = s3fs.S3FileSystem(anon=False)
            # Let to_zarr() handle the computation
            ds_fwi.to_zarr(
                store=s3fs.S3Map(root=config.output_uri, s3=fs),
                mode="w",
                consolidated=False,
            )
            write_time = time.time() - start_time

        except Exception as e:
            write_time = -999
            print(f"Error writing to s3: {str(e)}")

    # Close the client
    client.close()

    csv_rows.append(
        [
            config.n_workers,
            config.threads_per_worker,
            config.lat_chunk,
            config.lon_chunk,
            load_time,
            calc_time,
            write_time,
        ]
    )
    write_csv_to_s3(s3_client, bucket, csv_key, csv_rows)

    config_elapsed_time = time.time() - config_start_time
    print(
        f"{config.zarr_output_uri} completed in {config_elapsed_time:.2f} seconds."
    )
