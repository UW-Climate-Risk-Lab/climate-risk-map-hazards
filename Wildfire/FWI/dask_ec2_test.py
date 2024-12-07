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

S3 = True

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


def get_last_completed_run(csv_file: str) -> int:
    if not os.path.exists(csv_file):
        return 0
    try:
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            completed_runs = [int(row["run_id"]) for row in reader]
            return max(completed_runs) if completed_runs else 0
    except:
        return 0



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


def load_data(config: RunConfig) -> xr.Dataset:
    """
    Downloads data from S3 (if needed), loads it with xarray, and cleans up temporary files.
    """
    if config.s3:
        # Step 1: Create a temporary directory
        temp_dir = tempfile.mkdtemp()
        try:
            # Step 2: Download files from S3 to the temporary directory
            local_files = download_s3_files_to_temp(config.input_uris, temp_dir)

            # Step 3: Load data using xarray
            ds = xr.open_mfdataset(local_files, engine="h5netcdf")
        finally:
            # Step 4: Clean up the temporary directory
            print(f"Deleting temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)
            return ds
    else:
        # Directly load data if not using S3
        ds = xr.open_mfdataset(config.input_uris, engine="h5netcdf")

        return ds

def rechunk(ds: xr.Dataset, config: RunConfig):
    client = Client(
        n_workers=config.rechunk_n_workers,
        threads_per_worker=THREADS,
        memory_limit="auto",
    )
    try:
        target_chunks = {
            "time": config.time_chunk,
            "lat": config.lat_chunk,
            "lon": config.lon_chunk,
        }  # Use -1 to make 'time' a single chunk
        rechunked = ds.chunk(target_chunks)

        # Save to Zarr for efficient rechunking
        rechunked.to_zarr(config.zarr_store, mode="w")
    finally:
        client.close()


def calc(config: RunConfig) -> xr.Dataset:
    client = Client(
        n_workers=config.calc_n_workers,
        threads_per_worker=config.threads_per_worker,
        memory_limit="auto",
    )
    try:
        ds = xr.open_zarr(config.zarr_store)
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


def main():

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

    csv_file = "timed_results.csv"
    if not os.path.exists(csv_file):
        with open(csv_file, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "run_id",
                    "run_type",
                    "rechunk_n_workers",
                    "calc_n_workers",
                    "threads_per_worker",
                    "memory_limit",
                    "lat_chunk",
                    "lon_chunk",
                    "load_time",
                    "rechunk_time",
                    "calc_time",
                    "write_time"
                ]
            )

    print(f"Running configuration: {config}")

    start_time = time.time()
    ds = load_data(config)
    load_elapsed_time = time.time() - start_time

    # First, rechunk data and store on disk as zarr
    start_time = time.time()
    try:
        rechunk(ds, config)
        rechunk_elapsed_time = time.time() - start_time
    except Exception as e:
        print(f"Configuration {config.run_id} rechunk failed: {e}")
        rechunk_elapsed_time = -999

    # Then, process the rechunked data
    start_time = time.time()
    try:
        ds = calc(config)
        calc_elapsed_time = time.time() - start_time
        print(
            f"Configuration {config.run_id} completed in {calc_elapsed_time:.2f} seconds."
        )
    except Exception as e:
        ds = None
        calc_elapsed_time = -999
        print(f"Configuration {config.run_id} calc failed: {e}")

    
    try:
        start_time = time.time()
        if S3 & ds:
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
        print(f"Error writing to s3: {str(e)}")

    result = Results(
            config=config,
            load_time=load_elapsed_time,
            rechunk_time=rechunk_elapsed_time,
            calc_time=calc_elapsed_time,
            write_time=write_time
        )

    with open(csv_file, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                result.config.run_id,
                result.config.run_type,
                result.config.rechunk_n_workers,
                result.config.calc_n_workers,
                result.config.threads_per_worker,
                result.config.memory_limit,
                result.config.lat_chunk,
                result.config.lon_chunk,
                result.load_time,
                result.rechunk_time,
                result.calc_time,
                result.write_time
            ]
        )


    if os.path.exists(config.zarr_store):
        shutil.rmtree(config.zarr_store)


if __name__ == "__main__":
    main()
