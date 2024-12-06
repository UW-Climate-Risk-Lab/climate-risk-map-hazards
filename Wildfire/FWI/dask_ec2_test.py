from distributed import Client

import xarray as xr
import xclim

import time
import os
import shutil
import fsspec
import boto3
import s3fs

from dataclasses import dataclass
import csv
import uuid

S3 = True

TEST_S3_PATHS = {
    "one_year": {
        "s3_paths": [
            "s3://uw-climaterisklab/scratch/hurs_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc",
            "s3://uw-climaterisklab/scratch/pr_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc",
            "s3://uw-climaterisklab/scratch/sfcWind_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc",
            "s3://uw-climaterisklab/scratch/tas_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc",
        ],
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
MAX_MEMORY = 80
RECHUNK_N_WORKERS = 2
CALC_N_WORKERS = 4
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


def maximize_memory(n_workers: int, max_memory: int):
    return int(max_memory / n_workers)


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


def load_data(config: RunConfig) -> xr.Dataset:
    if config.s3:
        fs = fsspec.filesystem("s3")
        with fs.open_files(config.input_uris) as fileObj:
            ds = xr.open_mfdataset(fileObj, engine="h5netcdf")
    else:
        ds = xr.open_mfdataset(config.input_uris)
    return ds


def rechunk(ds: xr.Dataset, config: RunConfig):
    client = Client(
        n_workers=config.rechunk_n_workers,
        threads_per_worker=THREADS,
        memory_limit=int(MAX_MEMORY / RECHUNK_N_WORKERS),
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
        memory_limit=int(MAX_MEMORY / config.calc_n_workers),
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
        calc_elapsed_time = -999
        print(f"Configuration {config.run_id} calc failed: {e}")

    
    try:
        start_time = time.time()
        if S3:
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
