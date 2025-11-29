"""
Canonical column names for employment data across all sources.

These constants define the standard schema for employment data after transformation.
All employment data sources should be normalized to use these column names.
"""

# Core identifiers
REGION_ID = "region_id"  # Unique identifier for geographic region
REGION_NAME = "region_name"  # Human-readable region name
REGION_TYPE = "region_type"  # Type of region (metro, state, county, etc.)

# Temporal fields
PERIOD = "period"  # Date/period for the measurement (YYYY-MM format)
YEAR = "year"  # Year of measurement
MONTH = "month"  # Month of measurement (1-12)
PERIOD_MONTH = "period_month"  # First day-of-month date for point-in-time joins

# Employment metrics
LABOR_FORCE = "labor_force"  # Total number of people in labor force
EMPLOYED = "employed"  # Number of employed people
UNEMPLOYED = "unemployed"  # Number of unemployed people
UNEMPLOYMENT_RATE = "unemployment_rate"  # Unemployment rate (percentage)

# Metadata
DATA_SOURCE = "data_source"  # Source of the data (e.g., "BLS LAUS")
LAST_UPDATED = "last_updated"  # When the data was last updated
