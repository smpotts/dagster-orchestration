from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    FilesystemIOManager,  # Update the imports at the top of the file to also include this
)

from . import assets

all_assets = load_assets_from_modules([assets])

hackernews_job = define_asset_job("hackernews_job", selection=AssetSelection.all())

hackernews_schedule = ScheduleDefinition(
    job=hackernews_job,
    cron_schedule="0 * * * *",  # every hour
)

io_manager = FilesystemIOManager(
    base_dir="data",  # Path is built relative to where `dagster dev` is run
)

database_io_manager = DuckDBPandasIOManager(database="analytics.hackernews")

# Update your Definitions
defs = Definitions(
    assets=all_assets,
    schedules=[hackernews_schedule],
    resources={
        "io_manager": io_manager,
        "database_io_manager": database_io_manager,  # Define the I/O manager here
    },
)
