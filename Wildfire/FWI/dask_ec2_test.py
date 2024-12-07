from distributed import Client

import xarray as xr
import xclim

import time
import os
import shutil
import s3fs
import tempfile
import boto3
import multiprocessing

from dataclasses import dataclass
import csv
import uuid

from typing import List
import argparse

S3 = True
WRITE = True

TEST_S3_PATHS = {
    "one_year": {
        "s3": [
            "s3://uw-climaterisklab/scratch/climate_calc_test/one_year/hurs_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc",
            "s3://uw-climaterisklab/scratch/climate_calc_test/one_year/pr_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc",
            "s3://uw-climaterisklab/scratch/climate_calc_test/one_year/sfcWind_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc",
            "s3://uw-climaterisklab/scratch/climate_calc_test/one_year/tas_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc",
        ]
        ,
        "local": [],
    },
    "hundred_year": {
        "s3": [
            "s3://uw-climaterisklab/scratch/MIROC6_historical_sfcWind_concatenated.nc",
            "s3://uw-climaterisklab/scratch/MIROC6_historical_hurs_concatenated.nc",
            "s3://uw-climaterisklab/scratch/MIROC6_historical_tasmax_concatenated.nc",
            "s3://uw-climaterisklab/scratch/MIROC6_historical_pr_concatenated.nc",
        ],
        "local": [],
    },
}

RUN_TYPE = "one_year"
TIME_CHUNK = -1
LAT_CHUNK = 30
LON_CHUNK = 72
RECHUNK_N_WORKERS = multiprocessing.cpu_count()
CALC_N_WORKERS = multiprocessing.cpu_count()
THREADS = 2


@dataclass
class RunConfig:
    run_id: int
    run_type: str
    rechunk_n_workers: int
    calc_n_workers: int
    threads_per_worker: int
    time_chunk: int
    lat_chunk: int
    lon_chunk: int
    zarr_store: str
    input_uris: list
    output_uri: str
    s3: bool


@dataclass
class Results:
    config: RunConfig
    load_time: float
    rechunk_time: float  # second
    calc_time: float  # seconds
    write_time: float # seconds


def read_csv_from_s3(s3_client, bucket: str, key: str) -> List[List[str]]:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = obj['Body'].read().decode('utf-8').splitlines()
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

def download_s3_files_to_temp(s3_uris: List[str], temp_dir: str) -> List[str]:
    """
    Downloads files from S3 to a temporary local directory.
    
    Parameters:
        s3_uris: List of S3 URIs (s3://bucket/path)
        temp_dir: Path to the local temporary directory
    
    Returns:
        List of local file paths
    """
    s3 = boto3.client("s3")
    local_files = []

    for uri in s3_uris:
        bucket, key = uri.replace("s3://", "").split("/", 1)
        local_path = os.path.join(temp_dir, os.path.basename(key))
        print(f"Downloading {uri} to {local_path}...")
        s3.download_file(bucket, key, local_path)
        local_files.append(local_path)

    return local_files


def load_data(config: RunConfig, temp_dir: str) -> xr.Dataset:
    """
    Downloads data from S3 (if needed), loads it with xarray, and cleans up temporary files.
    """
    if config.s3:
        
        local_files = download_s3_files_to_temp(config.input_uris, temp_dir)
        ds = xr.open_mfdataset(local_files, engine="h5netcdf")
        return ds
    else:
        # Directly load data if not using S3
        ds = xr.open_mfdataset(config.input_uris, engine="h5netcdf")

        return ds


def calc(ds: xr.Dataset, config: RunConfig, tempdir: str) -> xr.Dataset:
    client = Client(
        n_workers=config.calc_n_workers,
        threads_per_worker=config.threads_per_worker,
        memory_limit="auto",
    )
    try:
        target_chunks = {
        "time": config.time_chunk,
        "lat": config.lat_chunk,
        "lon": config.lon_chunk,
        }
        ds = ds.chunk(target_chunks)
        out_fwi = xclim.indicators.atmos.cffwis_indices(
            tas=ds.tas,
            pr=ds.pr,
            hurs=ds.hurs,
            sfcWind=ds.sfcWind,
            lat=ds.lat,
            season_method=None,
            overwintering=False,
        )

        names = ["dc", "dmc", "ffmc", "isi", "bui", "fwi"]
        ds_fwi = xr.Dataset({name: da for name, da in zip(names, out_fwi)})
        ds_final = ds_fwi.compute()
        return ds_final
    finally:
        client.close()

def parse_args():
    parser = argparse.ArgumentParser(description="Run Dask EC2 Test")
    parser.add_argument("--ec2_type", type=str, required=True, help="EC2 instance type")
    return parser.parse_args()

def main(ec2_type: str):
    
    run_id = uuid.uuid4().int
    if S3:
        urls = TEST_S3_PATHS[RUN_TYPE]["s3"]
    else:
        urls = TEST_S3_PATHS[RUN_TYPE]["local"]

    config = RunConfig(
        run_id=run_id,
        run_type=RUN_TYPE,
        calc_n_workers=CALC_N_WORKERS,
        rechunk_n_workers=RECHUNK_N_WORKERS,
        threads_per_worker=THREADS,
        time_chunk=TIME_CHUNK,
        lat_chunk=LAT_CHUNK,
        lon_chunk=LON_CHUNK,
        zarr_store=f"r{run_id}.zarr",
        s3=S3,
        input_uris=urls,
        output_uri=f"s3://uw-climaterisklab/scratch/{run_id}.zarr",
    )

    s3_client = boto3.client("s3")
    bucket = "uw-climaterisklab"
    csv_key = "scratch/dask_results.csv"

    csv_file_exists = True
    csv_rows = read_csv_from_s3(s3_client, bucket, csv_key)
    if not csv_rows:
        csv_file_exists = False
        csv_rows.append([
            "run_id",
            "run_type",
            "rechunk_n_workers",
            "calc_n_workers",
            "threads_per_worker",
            "ec2_type",
            "lat_chunk",
            "lon_chunk",
            "load_time",
            "rechunk_time",
            "calc_time",
            "write_time"
        ])

    print(f"Running configuration: {config}")

    config_start_time = time.time()
    temp_dir = tempfile.mkdtemp()
    start_time = time.time()
    ds = load_data(config, temp_dir)
    load_elapsed_time = time.time() - start_time

    # Then, process the rechunked data
    start_time = time.time()
    try:
        ds = calc(ds, config, temp_dir)
        calc_elapsed_time = time.time() - start_time
    except Exception as e:
        ds = None
        calc_elapsed_time = -999
        print(f"Configuration {config.run_id} calc failed: {e}")
    # The temp_dir is only cleaned up in the RECHUNK block, but should be cleaned up at the end
    # Add at the end of main():
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    
    try:
        start_time = time.time()
        if S3 and (ds is not None) and WRITE:
            fs = s3fs.S3FileSystem(anon=False)
            ds.to_zarr(
                store=s3fs.S3Map(root=config.output_uri, s3=fs),
                mode="w",  # Use 'a' for append
                consolidated=True,  # Consolidate metadata for better performance
            )
            write_time = time.time() - start_time
        else:
            write_time = -999
    except Exception as e:
        write_time = -999
        print(f"Error writing to s3: {str(e)}")

    result = Results(
            config=config,
            load_time=load_elapsed_time,
            rechunk_time=-999, # Rechunk part of calc
            calc_time=calc_elapsed_time,
            write_time=write_time
        )

    csv_rows.append([
        result.config.run_id,
        result.config.run_type,
        result.config.rechunk_n_workers,
        result.config.calc_n_workers,
        result.config.threads_per_worker,
        ec2_type,
        result.config.lat_chunk,
        result.config.lon_chunk,
        result.load_time,
        result.rechunk_time,
        result.calc_time,
        result.write_time
    ])

    write_csv_to_s3(s3_client, bucket, csv_key, csv_rows)

    if os.path.exists(config.zarr_store):
        shutil.rmtree(config.zarr_store)

    config_elapsed_time = time.time() - config_start_time
    print(
            f"Configuration {config.run_id} completed in {config_elapsed_time:.2f} seconds."
        )

if __name__ == "__main__":
    args = parse_args()
    main(ec2_type=args.ec2_type)
