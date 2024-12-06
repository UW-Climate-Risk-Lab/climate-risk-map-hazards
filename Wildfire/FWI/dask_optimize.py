from distributed import Client

import xarray as xr
import xclim

import time
import os
import shutil

from dataclasses import dataclass
import csv

# Depending on your workstation specifications, you may need to adjust these values.
# On a single machine, n_workers=1 is usually better.


HURS_PATH = "/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/input/hurs_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc"
PR_PATH = "/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/input/pr_day_CESM2_ssp126_r4i1p1f1_gn_2030_v1.1.nc"
TAS_PATH = "/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/input/tas_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc"
SFC_WIND_PATH = "/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/input/sfcWind_day_CESM2_ssp126_r4i1p1f1_gn_2030.nc"
ZARR_STORE_PREFIX = "/Users/ericcollins/climate-risk-map-hazards/Wildfire/FWI/rechunked"


@dataclass
class RunConfig:
    run_id: int
    n_workers: int
    threads_per_worker: int
    memory_limit: str
    lat_chunk: int
    lon_chunk: int
    zarr_store: str


@dataclass
class Results:
    config: RunConfig
    rechunk_time: float  # second
    calc_time: float  # seconds


def maximize_memory(n_workers: int, max_memory: int):
    return int(max_memory / n_workers)

def get_last_completed_run(csv_file: str) -> int:
    if not os.path.exists(csv_file):
        return 0
        
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            completed_runs = [int(row['run_id']) for row in reader]
            return max(completed_runs) if completed_runs else 0
    except:
        return 0

def rechunk(config: RunConfig):
    with Client(
        n_workers=config.n_workers,
        threads_per_worker=config.threads_per_worker,
        memory_limit=config.memory_limit,
    ):
        ds = xr.open_mfdataset(paths=[HURS_PATH, PR_PATH, TAS_PATH, SFC_WIND_PATH])
        target_chunks = {
            "time": -1,
            "lat": config.lat_chunk,
            "lon": config.lon_chunk,
        }  # Use -1 to make 'time' a single chunk
        rechunked = ds.chunk(target_chunks)

        # Save to Zarr for efficient rechunking
        rechunked.to_zarr(config.zarr_store, mode="w")


def run(config: RunConfig):
    with Client(
        n_workers=config.n_workers,
        threads_per_worker=config.threads_per_worker,
        memory_limit=config.memory_limit,
    ):
        ds = xr.open_zarr(f"{ZARR_STORE_PREFIX}_{config.run_id}.zarr")
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
        da_out = xr.Dataset({name: da for name, da in zip(names, out_fwi)})
        da_out.compute()


def main():
    # Define maximum system constraints
    max_lat = 300  # Maximum latitude range
    max_lon = 720  # Maximum longitude range
    max_memory = 16  # Maximum total memory in GB

    configs = []
    run_id = 0

    # Generate configurations dynamically
    for n_workers in range(4, 9):  # n_workers: 1 to 4 for 8 threads
        memory_limit = int(max_memory / n_workers)
        for threads in range(1, 3):
            for i in range(2, 31):
                if (max_lon % i != 0) or (max_lat % i != 0):
                    continue

                lon_chunk = max_lon / i
                lat_chunk = max_lat / i

                run_id += 1
                configs.append(
                    RunConfig(
                        run_id=run_id,
                        n_workers=n_workers,
                        threads_per_worker=threads,
                        memory_limit=f"{memory_limit}GB",
                        lat_chunk=lat_chunk,
                        lon_chunk=lon_chunk,
                        zarr_store=f"{ZARR_STORE_PREFIX}_{run_id}.zarr",
                    )
                )

    csv_file = "benchmark_results.csv"
    if not os.path.exists(csv_file):
        with open(csv_file, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "run_id",
                    "n_workers",
                    "threads_per_worker",
                    "memory_limit",
                    "lat_chunk",
                    "lon_chunk",
                    "calc_time",
                    "rechunk_time",
                ]
            )

    # Execute each configuration
    for config in configs:
        last_run_id = get_last_completed_run(csv_file)

        if config.run_id <= last_run_id:
            continue

        print(f"Running configuration: {config}")
        
        # First, rechunk data
        start_time = time.time()
        try:
            rechunk(config)
            rechunk_elapsed_time = time.time() - start_time
        except Exception as e:
            print(f"Configuration {config.run_id} rechunk failed: {e}")

        # Then, process the rechunked data
        start_time = time.time()
        try:
            run(config)
            calc_elapsed_time = time.time() - start_time
            result = Results(
                config=config,
                rechunk_time=rechunk_elapsed_time,
                calc_time=calc_elapsed_time,
            )
            with open(csv_file, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    [
                        result.config.run_id,
                        result.config.n_workers,
                        result.config.threads_per_worker,
                        result.config.memory_limit,
                        result.config.lat_chunk,
                        result.config.lon_chunk,
                        result.calc_time,
                        result.rechunk_time,
                    ]
                )

            print(
                f"Configuration {config.run_id} completed in {calc_elapsed_time:.2f} seconds."
            )
        except Exception as e:
            print(f"Configuration {config.run_id} calc failed: {e}")
        if os.path.exists(config.zarr_store):
            shutil.rmtree(config.zarr_store)


if __name__ == "__main__":
    main()
