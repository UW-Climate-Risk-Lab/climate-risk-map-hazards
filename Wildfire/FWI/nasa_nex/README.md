

## Run Pipeline on EC2


'''bash
docker run --network host calc --ec2_type c59xlarge --model CESM2 --scenario ssp126 --ensemble_member r4i1p1f1 --year 2060
'''