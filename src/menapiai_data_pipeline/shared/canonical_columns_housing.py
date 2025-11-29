"""
Shared canonical housing columns for Redfin ingestion and transformation.
"""

CANONICAL_HOUSING_REGION_ID = "region_id"
CANONICAL_HOUSING_PERIOD_BEGIN = "period_begin"
CANONICAL_HOUSING_PERIOD_END = "period_end"
CANONICAL_HOUSING_MEDIAN_SALE_PRICE = "median_sale_price"
CANONICAL_HOUSING_HOMES_SOLD = "homes_sold"
CANONICAL_HOUSING_INVENTORY = "inventory"
CANONICAL_HOUSING_MEDIAN_DAYS_ON_MARKET = "median_days_on_market"
CANONICAL_HOUSING_PROPERTY_TYPE = "property_type"

# Derived temporal field for monthly alignment
CANONICAL_HOUSING_PERIOD_MONTH = "period_month"  # First day-of-month

# Metadata
CANONICAL_HOUSING_LAST_UPDATED = "last_updated"
