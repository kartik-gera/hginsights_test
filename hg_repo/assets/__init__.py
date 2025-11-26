# hg_repo/assets/__init__.py

from hg_repo.assets.raw_customers import raw_customers
from hg_repo.assets.dbt_build import dbt_build
from hg_repo.assets.analytics_freshness_check import analytics_freshness_check


__all__ = [
    "raw_customers",
    "dbt_build",
    "analytics_freshness_check",
]
