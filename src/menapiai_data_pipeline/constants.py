"""
Constants for MENAPI AI data pipeline.
"""

# Redfin housing data
REDFIN_CITY_URL = (
    "https://redfin-public-data.s3.us-west-2.amazonaws.com/"
    "redfin_market_tracker/city_market_tracker.tsv000.gz"
)

# BLS (Bureau of Labor Statistics) API
BLS_API_BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# BLS LAUS (Local Area Unemployment Statistics) Series ID patterns
# Format: LAUMT + STATE_FIPS + AREA_CODE + MEASURE_CODE
# Measure codes:
#   03 = unemployment rate
#   04 = unemployment
#   05 = employment  
#   06 = labor force

# Metro area FIPS codes (common areas)
BLS_METRO_AREAS = {
    "Portland-Vancouver-Hillsboro, OR-WA": {
        "state_fips": "41",
        "area_code": "38900",
        "full_code": "4138900",
    },
    "Seattle-Tacoma-Bellevue, WA": {
        "state_fips": "53",
        "area_code": "42660", 
        "full_code": "5342660",
    },
    "San Francisco-Oakland-Hayward, CA": {
        "state_fips": "06",
        "area_code": "41860",
        "full_code": "0641860",
    },
    "Los Angeles-Long Beach-Anaheim, CA": {
        "state_fips": "06",
        "area_code": "31080",
        "full_code": "0631080",
    },
    "Phoenix-Mesa-Scottsdale, AZ": {
        "state_fips": "04",
        "area_code": "38060",
        "full_code": "0438060",
    },
}
