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
from pipeline import RunConfig


@dataclass
class Results:
    config: RunConfig
    load_time: float
    calc_time: float  # seconds
    write_time: float  # seconds


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


def calc(ds: xr.Dataset, config: RunConfig) -> xr.Dataset:
    # The client will be created once in main and passed data via persisted ds.
    # Here we just define calculation steps.
    target_chunks = {
        "time": config.time_chunk,
        "lat": config.lat_chunk,
        "lon": config.lon_chunk,
    }

    # Re-chunk directly as desired
    ds = ds.chunk(target_chunks)

    # Attempt to get tasmax or fallback to tas
    try:
        tas = ds.tasmax
    except AttributeError:
        tas = ds.tas

    out_fwi = xclim.indicators.atmos.cffwis_indices(
        tas=tas,
        pr=ds.pr,
        hurs=ds.hurs,
        sfcWind=ds.sfcWind,
        lat=ds.lat,
        season_method=None,
        overwintering=False,
    )

    names = ["dc", "dmc", "ffmc", "isi", "bui", "fwi"]
    ds_fwi = xr.Dataset({name: da for name, da in zip(names, out_fwi)})
    return ds_fwi

def load(config: RunConfig) -> xr.Dataset:
    fs_r = fsspec.filesystem("s3", anon=True)
    flist = [fs_r.open(path, mode="rb") for path in config.input_uris]
    ds = xr.open_mfdataset(
        flist, engine="h5netcdf", decode_times=True, combine="by_coords", chunks="auto"
    )
    # Persisting the dataset into cluster memory (if memory allows)
    ds = ds.sel(lat=slice(config.bbox["ymin"], config.bbox["ymax"]),
                lon=slice(config.bbox["xmin"], config.bbox["xmax"])).persist()

    return ds


def main(config: RunConfig):
    s3_client = boto3.client("s3")
    bucket = "uw-crl"
    csv_key = "scratch/dask_results.csv"

    csv_rows = read_csv_from_s3(s3_client, bucket, csv_key)
    if not csv_rows:
        csv_rows.append(
            [
                "run_id",
                "run_type",
                "calc_n_workers",
                "threads_per_worker",
                "ec2_type",
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
        n_workers=config.calc_n_workers,
        threads_per_worker=config.threads_per_worker,
        memory_limit="auto",
    )

    # Load data directly from S3 using fsspec and xarray
    # Disable unnecessary decoding to speed up
    start_time = time.time()
    ds = load(config)
    load_elapsed_time = time.time() - start_time

    # Perform calculation (no additional rechunk step separately, done inside calc)
    start_time = time.time()
    try:
        ds_fwi = calc(ds, config)
        calc_elapsed_time = time.time() - start_time
    except Exception as e:
        ds_fwi = None
        calc_elapsed_time = -999
        print(f"Configuration {config.run_id} calc failed: {e}")

    # Writing results to S3 as Zarr
    start_time = time.time()
    write_time = -999
    if ds_fwi:
        try:
            fs = s3fs.S3FileSystem(anon=False)
            # Let to_zarr() handle the computation
            ds_fwi.to_zarr(
                store=s3fs.S3Map(root=config.output_uri, s3=fs),
                mode="w",
                consolidated=False,
            )
            write_time = time.time() - start_time

        except Exception as e:
            print(f"Error writing to s3: {str(e)}")

    # Close the client
    client.close()

    result = Results(
        config=config,
        load_time=load_elapsed_time,
        calc_time=calc_elapsed_time,
        write_time=write_time,
    )

    csv_rows.append(
        [
            result.config.run_id,
            result.config.run_type,
            result.config.calc_n_workers,
            result.config.threads_per_worker,
            result.config.lat_chunk,
            result.config.lon_chunk,
            result.load_time,
            result.calc_time,
            result.write_time,
        ]
    )

    write_csv_to_s3(s3_client, bucket, csv_key, csv_rows)

    # Clean up local zarr store if created (not used in this approach, but just in case)
    if os.path.exists(config.zarr_store):
        shutil.rmtree(config.zarr_store)

    config_elapsed_time = time.time() - config_start_time
    print(
        f"Configuration {config.run_id} completed in {config_elapsed_time:.2f} seconds."
    )
