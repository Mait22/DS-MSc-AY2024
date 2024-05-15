from dagster import Definitions, load_assets_from_modules, io_manager 
import os

from . import assets
from .resources.pg_io_manager_lx import PgIOManagerLx
from .resources.pg_io_manager_dw import PgIOManagerDw

# InfraLx connections param
PSQL_CONFIG = {
    "host": os.getenv("LX_POSTGRES_HOST"),
    "port": os.getenv("LX_POSTGRES_PORT"),
    "database": os.getenv("LX_POSTGRES_DB"),
    "user": os.getenv("LX_POSTGRES_USER"),
    "password": os.getenv("LX_POSTGRES_PASSWORD"),
}

# Temp Pg DW connection param
DW_PSQL_CONFIG = {
    "host": os.getenv("DW_POSTGRES_HOST"),
    "port": os.getenv("DW_POSTGRES_PORT"),
    "database": os.getenv("DW_POSTGRES_DB"),
    "user": os.getenv("DW_POSTGRES_USER"),
    "password": os.getenv("DW_POSTGRES_PASSWORD"),
}

resources = {
    "pg_io_manager": PgIOManagerLx(PSQL_CONFIG),
    "pg_io_manager_dw": PgIOManagerDw(DW_PSQL_CONFIG)
}

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources=resources
)