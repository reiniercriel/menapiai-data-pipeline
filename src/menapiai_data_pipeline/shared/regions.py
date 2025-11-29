"""
Shared region mapping utilities.

Provides a minimal mapping from (city, state_full) to CBSA full_code used in BLS LAUS,
so both Housing and Employment share a consistent `region_id`.

Extend `CITY_STATE_TO_CBSA` as needed.
"""
from __future__ import annotations

from typing import Optional, Tuple

# Keys should use full state names (not abbreviations)
CITY_STATE_TO_CBSA: dict[Tuple[str, str], str] = {
    ("Portland", "Oregon"): "4138900",  # Portland-Vancouver-Hillsboro, OR-WA
    ("Seattle", "Washington"): "5342660",  # Seattle-Tacoma-Bellevue, WA
    ("San Francisco", "California"): "0641860",  # San Francisco-Oakland-Hayward, CA
    ("Los Angeles", "California"): "0631080",  # Los Angeles-Long Beach-Anaheim, CA
    ("Phoenix", "Arizona"): "0438060",  # Phoenix-Mesa-Scottsdale, AZ
}


def lookup_cbsa_from_city_state(city: str, state_full: str) -> Optional[str]:
    """Return CBSA full_code for a given city and full state name, if known."""
    key = (city, state_full)
    return CITY_STATE_TO_CBSA.get(key)
