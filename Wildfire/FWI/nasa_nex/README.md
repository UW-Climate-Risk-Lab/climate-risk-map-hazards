

## Run Pipeline on EC2


'''bash
docker run --network host pipeline --model MIROC6 --scenario ssp126 --ensemble_member r1i1p1f1 --lat_chunk 30 --lon_chunk 72 --threads 4 --x_min 10 --y_min 50 --x_max 50 --y_max 300
'''