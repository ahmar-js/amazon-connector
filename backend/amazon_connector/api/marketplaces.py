"""Centralized marketplace configuration for backend.

Provides a single source of truth for marketplace codes, IDs and regions.
Used by backend modules to ensure consistency across views and services.
"""
from typing import Dict

# Marketplace code -> marketplace id
MARKETPLACE_IDS: Dict[str, str] = {
    "IT": "APJ6JRA9NG5V4",
    "DE": "A1PA6795UKMFR9",
    "UK": "A1F83G8C2ARO7P",
    "US": "ATVPDKIKX0DER",
    "CA": "A2EUQ1WTGCTBG2",
    "FR": "A13V1IB3VIYZZH",
    "ES": "A1RKKUPIHCS9HS",
}

# Marketplace id -> region (selling partner api regional domain)
# Use 'na' for North America endpoints and 'eu' for European endpoints
MARKETPLACE_REGIONS: Dict[str, str] = {
    "ATVPDKIKX0DER": "na",  # US
    "A2EUQ1WTGCTBG2": "na",  # CA
    "A1PA6795UKMFR9": "eu",  # DE
    "A1F83G8C2ARO7P": "eu",  # UK
    "A13V1IB3VIYZZH": "eu",  # FR
    "A1RKKUPIHCS9HS": "eu",  # ES
    "APJ6JRA9NG5V4": "eu",  # IT
}


def get_marketplace_id(code: str) -> str:
    """Return marketplace id for a given marketplace code (e.g., 'US' -> 'ATVPDKIKX0DER')."""
    return MARKETPLACE_IDS.get(code)


def get_region_from_marketplace_id(marketplace_id: str) -> str:
    """Return region identifier for a given marketplace id ('na' or 'eu').

    Defaults to 'na' when unknown to match legacy behaviour in the codebase.
    """
    return MARKETPLACE_REGIONS.get(marketplace_id, 'na')


def get_available_marketplaces() -> Dict[str, str]:
    """Return a shallow copy of available marketplace code -> id mapping."""
    return dict(MARKETPLACE_IDS)
