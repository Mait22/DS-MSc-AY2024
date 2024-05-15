from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection, ScheduleDefinition
import os

from . import assets
from .resources.minio_io_manager import MinIOIOManager
from .resources.sftp_io_manager import SFTPManager
from .resources.pg_io_manager import PgIOManager
from .resources.mssql_io_manager import MSSQLIOManager

# Minio resource config 
MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

# DW resource config
DW_PSQL_CONFIG = {
    "host": os.getenv("DW_POSTGRES_HOST"),
    "port": os.getenv("DW_POSTGRES_PORT"),
    "database": os.getenv("DW_POSTGRES_DB"),
    "user": os.getenv("DW_POSTGRES_USER"),
    "password": os.getenv("DW_POSTGRES_PASSWORD"),
}

# SFTP resource config
SFTP_CONFIG = {
    "sftp_user": os.getenv("SFTP_USER"),
    "sftp_pass": os.getenv("SFTP_PASS")
}

# D365 MS SQL 2016 resource config
MSSQL_CONFIG = {
    "mssql_server": os.getenv("MSSQL_SERVER"),
    "mssql_database": os.getenv("MSSQL_DATABASE"),
    "mssql_username": os.getenv("MSSQL_USERNAME"),
    "mssql_password": os.getenv("MSSQL_PASSWORD"),
}

# Export resources with .env specified parameters
resources = {
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "sftp_io_manager": SFTPManager(SFTP_CONFIG),
    "pg_io_manager": PgIOManager(DW_PSQL_CONFIG),
    "mssql_io_manager": MSSQLIOManager(MSSQL_CONFIG),
}

# Autoload assets from assets catalogue
all_assets = load_assets_from_modules([assets])

# Generate "materialize all level" job
xml_job = define_asset_job("xml_job", selection=AssetSelection.all())

# Schedule job
xml_schedule = ScheduleDefinition(
    job=xml_job,
    cron_schedule="0 14 * * *",  # https://crontab.guru/
)

# Export Definitions
defs = Definitions(
    assets=all_assets,
    resources=resources,
    schedules=[xml_schedule],
)