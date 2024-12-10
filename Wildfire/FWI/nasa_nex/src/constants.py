# Constants
INPUT_BUCKET = "nex-gddp-cmip6"
INPUT_PREFIX = "NEX-GDDP-CMIP6"
OUTPUT_BUCKET = "uw-crl"
OUTPUT_PREFIX = "climate-risk-map/backend/climate/scenariomip"

TIME_CHUNK = -1
LAT_CHUNK = 160
LON_CHUNK = 160
N_WORKERS = 16
THREADS = 4

VAR_LIST = [
        ["tasmax"],
        ["hurs"],
        ["sfcWind"],
        ["pr"]
    ]

VALID_YEARS = {
    "historical": [i for i in range(1950, 2015)],
    "ssp": [i for i in range(2015, 2101)]
}