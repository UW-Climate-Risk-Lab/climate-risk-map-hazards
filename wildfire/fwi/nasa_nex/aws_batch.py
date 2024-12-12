import boto3
import time
import os
from botocore.exceptions import ClientError
import sys

# Configuration from environment variables
AWS_REGION = os.getenv("AWS_REGION")
JOB_QUEUE = os.getenv("JOB_QUEUE")
JOB_DEFINITION = os.getenv("JOB_DEFINITION")
LAT_CHUNK = os.getenv("LAT_CHUNK")
LON_CHUNK = os.getenv("LON_CHUNK")
THREADS = os.getenv("THREADS")
X_MIN = os.getenv("X_MIN")
Y_MIN = os.getenv("Y_MIN")
X_MAX = os.getenv("X_MAX")
Y_MAX = os.getenv("Y_MAX")

MODELS = [
    {
        "model": "ACCESS-CM2",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "ACCESS-ESM1-5",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "BCC-CSM2-MR",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "CanESM5",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "CMCC-CM2-SR5",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "CMCC-ESM2",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "CNRM-CM6-1",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f2",
    },
    {
        "model": "CNRM-ESM2-1",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f2",
    },
    {
        "model": "EC-Earth3-Veg-LR",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "EC-Earth3",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "FGOALS-g3",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r3i1p1f1",
    },
    {
        "model": "GFDL-CM4",
        "scenario": ["historical", "ssp245", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "GFDL-CM4_gr2",
        "scenario": ["historical", "ssp245", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "GFDL-ESM4",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "GISS-E2-1-G",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f2",
    },
    {
        "model": "HadGEM3-GC31-LL",
        "scenario": ["historical", "ssp126", "ssp245", "ssp585"],
        "ensemble_member": "r1i1p1f3",
    },
    {
        "model": "HadGEM3-GC31-MM",
        "scenario": ["historical", "ssp126", "ssp585"],
        "ensemble_member": "r1i1p1f3",
    },
    {
        "model": "INM-CM4-8",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "INM-CM5-0",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "IPSL-CM6A-LR",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "KACE-1-0-G",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "KIOST-ESM",
        "scenario": ["historical", "ssp126", "ssp245", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "MIROC-ES2L",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f2",
    },
    {
        "model": "MIROC6",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "MPI-ESM1-2-HR",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "MPI-ESM1-2-LR",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "MRI-ESM2-0",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "NorESM2-LM",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "NorESM2-MM",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "TaiESM1",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f1",
    },
    {
        "model": "UKESM1-0-LL",
        "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        "ensemble_member": "r1i1p1f2",
    },
]

def validate_env_vars():
    """Validate required environment variables."""
    required_vars = [
        "AWS_REGION", "JOB_QUEUE", "JOB_DEFINITION", 
        "LAT_CHUNK", "LON_CHUNK", "THREADS",
        "X_MIN", "Y_MIN", "X_MAX", "Y_MAX"
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

def submit_batch_job(model, scenario, ensemble_member):
    """Submits a single job to AWS Batch with specified parameters."""
    try:
        job_name = f"pipeline-{model}-{scenario}-{ensemble_member}"
        command = [
            "--model", model,
            "--scenario", scenario,
            "--ensemble_member", ensemble_member,
            "--lat_chunk", str(LAT_CHUNK),
            "--lon_chunk", str(LON_CHUNK),
            "--threads", str(THREADS),
            "--x_min", str(X_MIN),
            "--y_min", str(Y_MIN),
            "--x_max", str(X_MAX),
            "--y_max", str(Y_MAX),
        ]

        response = BATCH_CLIENT.submit_job(
            jobName=job_name,
            jobQueue=JOB_QUEUE,
            jobDefinition=JOB_DEFINITION,
            containerOverrides={
                "command": ["python", "src/pipeline.py"] + command,
                "environment": [
                    {"name": "AWS_REGION", "value": AWS_REGION},
                ],
            },
        )
        print(f"Submitted job: {job_name}, Job ID: {response['jobId']}")
        return response["jobId"]
    except ClientError as e:
        print(f"Failed to submit job {job_name}: {str(e)}")
        return None

def monitor_jobs(job_ids):
    """Monitor job statuses with proper error handling."""
    while job_ids:
        time.sleep(30)
        try:
            # Process jobs in batches of 100 (AWS Batch API limit)
            batch = job_ids[:100]
            job_statuses = BATCH_CLIENT.describe_jobs(jobs=batch)
            
            for job in job_statuses["jobs"]:
                if job["status"] in ["SUCCEEDED", "FAILED"]:
                    job_ids.remove(job["jobId"])
                    print(f"Job {job['jobId']} completed with status: {job['status']}")
                
            print(f"Remaining jobs: {len(job_ids)}")
        except ClientError as e:
            print(f"Error checking job status: {str(e)}")
            time.sleep(60)  # Back off on API errors

def main():
    try:
        validate_env_vars()
        
        # Initialize AWS client
        global BATCH_CLIENT
        BATCH_CLIENT = boto3.client("batch", region_name=AWS_REGION)
        
        jobs = []
        job_ids = []

        # Build all possible jobs
        for model in MODELS:
            for scenario in model["scenario"]:
                jobs.append({
                    "model": model["model"],
                    "scenario": scenario,
                    "ensemble_member": model["ensemble_member"],
                })

        # Submit all jobs
        for job in jobs:
            job_id = submit_batch_job(
                job["model"], 
                job["scenario"], 
                job["ensemble_member"]
            )
            if job_id:
                job_ids.append(job_id)
                time.sleep(0.2)  # Throttle submissions

        print(f"Successfully submitted {len(job_ids)} out of {len(jobs)} jobs")

        if job_ids:
            monitor_jobs(job_ids)
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()