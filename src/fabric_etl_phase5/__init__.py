"""Separately installable Phase 5 ingestion extension for the Fabric ETL package."""

from fabric_etl import __version__ as _framework_version

__version__ = "0.6.1"
REQUIRED_FRAMEWORK_VERSION = "0.1.0"

if _framework_version != REQUIRED_FRAMEWORK_VERSION:
    raise ImportError(
        f"fabric-etl-phase5 requires fabric-etl {REQUIRED_FRAMEWORK_VERSION}; "
        f"found {_framework_version}"
    )
