import boto3
import time
import os

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

MODELS = {
    ""
}

# Create the AWS Batch client
batch_client = boto3.client("batch", region_name=AWS_REGION)

def submit_batch_job(model, scenario, ensemble_member):
    """
    Submits a single job to AWS Batch with specified parameters.
    """
    job_name = f"pipeline-{model}-{scenario}-{ensemble_member}"
    command = [
        "--model", model,
        "--scenario", scenario,
        "--ensemble_member", ensemble_member,
        "--lat_chunk", LAT_CHUNK,
        "--lon_chunk", LON_CHUNK,
        "--threads", THREADS,
        "--x_min", X_MIN,
        "--y_min", Y_MIN,
        "--x_max", X_MAX,
        "--y_max", Y_MAX,
    ]

    response = batch_client.submit_job(
        jobName=job_name,
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        containerOverrides={
            "command": ["python", "src/pipeline.py"] + command,
            "environment": [
                {"name": "AWS_REGION", "value": AWS_REGION},
            ]
        }
    )
    print(f"Submitted job: {job_name}, Job ID: {response['jobId']}")
    return response["jobId"]

def main():
    job_ids = []

    # Submit jobs for all model-scenario combinations
    for model in MODELS:
        for scenario in SCENARIOS:
            job_id = submit_batch_job(model, scenario, ENSEMBLE_MEMBER)
            job_ids.append(job_id)
            time.sleep(0.2)  # Throttle submissions slightly to avoid API rate limits

    print(f"Total jobs submitted: {len(job_ids)}")

    # Optional: Monitor job statuses
    print("Monitoring job statuses...")
    while True:
        time.sleep(30)
        job_statuses = batch_client.describe_jobs(jobs=job_ids)
        all_done = all(job['status'] in ['SUCCEEDED', 'FAILED'] for job in job_statuses['jobs'])
        
        if all_done:
            print("All jobs have completed.")
            break
        else:
            print("Some jobs are still running...")

if __name__ == "__main__":
    main()