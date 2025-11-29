"""
Raw column names from BLS (Bureau of Labor Statistics) API responses.

These constants map to the field names in the BLS API JSON response.
Reference: https://www.bls.gov/developers/api_signature_v2.htm
"""

# Top-level response fields
STATUS = "status"
RESPONSE_TIME = "responseTime"
MESSAGE = "message"
RESULTS = "Results"

# Series data fields
SERIES = "series"
SERIES_ID = "seriesID"
DATA = "data"

# Data point fields
YEAR = "year"
PERIOD = "period"  # Format: M01-M12 for monthly data
PERIOD_NAME = "periodName"  # Human-readable period name (e.g., "January")
VALUE = "value"  # The actual measurement value
FOOTNOTES = "footnotes"

# Series metadata (when catalog=true)
CATALOG = "catalog"
SERIES_TITLE = "series_title"
SERIES_ID_CATALOG = "series_id"
SEASONALLY_ADJUSTED = "seasonally_adjusted"
BEGIN_YEAR = "begin_year"
END_YEAR = "end_year"
BEGIN_PERIOD = "begin_period"
END_PERIOD = "end_period"
