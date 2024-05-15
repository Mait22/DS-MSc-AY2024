from dagster import load_assets_from_modules

from . import bronze

bronze_layer_assets = load_assets_from_modules([bronze])