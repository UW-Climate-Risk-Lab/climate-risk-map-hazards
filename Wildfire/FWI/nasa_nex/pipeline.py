import sys
import boto3
import re
import argparse
import uuid
from dataclasses import dataclass
import run

# Constants
BUCKET = "nex-gddp-cmip6"
PREFIX = "NEX-GDDP-CMIP6"
RUN_TYPE = "one_year"
TIME_CHUNK = -1
LAT_CHUNK = 30
LON_CHUNK = 72
N_WORKERS = 20
THREADS = 4

# Bounding Box
LAT_MIN = 10
LAT_MAX = 50
LON_MIN = 230
LON_MAX = 300

@dataclass
class RunConfig:
    run_id: int
    ec2_type: str
    run_type: str
    rechunk_n_workers: int
    calc_n_workers: int
    threads_per_worker: int
    time_chunk: int
    lat_chunk: int
    lon_chunk: int
    bbox: dict
    zarr_store: str
    input_uris: list
    output_uri: str

def parse_args():
    parser = argparse.ArgumentParser(description="Run Dask EC2 Test")
    parser.add_argument("--ec2_type", type=str, required=True, help="EC2 instance type")
    parser.add_argument("--model", type=str, required=True, help="Climate model")
    parser.add_argument("--scenario", type=str, required=True, help="SSP Scenario or Historical")
    parser.add_argument("--ensemble_member", type=str, required=True, help="Simulation Run e.g 'r4i1p1f1'")
    parser.add_argument("--year", type=str, required=True, help="Year to use")
    return parser.parse_args()

def find_best_file(s3_client, model, scenario, ensemble_member, year, var_candidates):
    """
    Finds the best matching file on S3 based on variable candidates and version priority.
    """
    for variable in var_candidates:
        var_prefix = f"{PREFIX}/{model}/{scenario}/{ensemble_member}/{variable}/"
        response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=var_prefix)
        
        if "Contents" not in response:
            continue
        
        pattern = (
            rf"^{variable}_day_{re.escape(model)}_{re.escape(scenario)}_"
            rf"{re.escape(ensemble_member)}_gn_{year}(_v\d+\.\d+)?\.nc$"
        )
        file_regex = re.compile(pattern)
        
        matching_files = [obj["Key"].split("/")[-1] for obj in response["Contents"] if file_regex.match(obj["Key"].split("/")[-1])]
        
        if not matching_files:
            continue
        
        v1_1_files = [f for f in matching_files if "_v1.1.nc" in f]
        chosen_file = v1_1_files[0] if v1_1_files else matching_files[0]
        
        return f"s3://{BUCKET}/{var_prefix}{chosen_file}"
    return None

def main(ec2_type: str, model: str, scenario: str, year: str, ensemble_member: str):      
    s3_client = boto3.client('s3')
    
    # Variables needed, with fallback for tasmax
    var_list = [
        ["tasmax", "tas"],
        ["hurs"],
        ["sfcWind"],
        ["pr"]
    ]
    
    input_uris = []
    for var_candidates in var_list:
        input_uri = find_best_file(s3_client, model, scenario, ensemble_member, year, var_candidates)
        if input_uri is None:
            print(f"Error: Could not find a valid file for variables: {var_candidates}")
            sys.exit(1)
        input_uris.append(input_uri)
    
    # Construct output Zarr file path
    run_id = uuid.uuid4().int
    output_prefix = f"{PREFIX}/{model}/{scenario}/{ensemble_member}"
    output_uri = f"s3://uw-crl/scratch/{output_prefix}/fwi_day_{model}_{scenario}_{ensemble_member}_gn_{year}.zarr"
    
    # Run Configuration
    config = RunConfig(
        run_id=run_id,
        ec2_type=ec2_type,
        run_type=RUN_TYPE,
        rechunk_n_workers=N_WORKERS,
        calc_n_workers=N_WORKERS,
        threads_per_worker=THREADS,
        time_chunk=TIME_CHUNK,
        lat_chunk=LAT_CHUNK,
        lon_chunk=LON_CHUNK,
        bbox={"xmin": LON_MIN,
                "xmax": LON_MAX,
                "ymin": LAT_MIN,
                "ymax": LAT_MAX},
        zarr_store=output_uri,
        input_uris=input_uris,
        output_uri=output_uri
    )

    print("Starting run with the following configuration:")
    print(config)
    
    # Run calculation
    run.main(config)

if __name__ == "__main__":
    args = parse_args()
    main(
        ec2_type=args.ec2_type,
        model=args.model,
        scenario=args.scenario,
        year=args.year,
        ensemble_member=args.ensemble_member
    )