from dagster import Definitions, load_assets_from_modules
from .assets import bronze, silver, gold, warehouse
from .resources.abs_io_manager import ABSIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
# from dagstermill import local_output_notebook_io_manager
# from .assets.bronze import bronze_imdb_user_rating_dataset, bronze_movies, bronze_reviews

ABS_CONFIG = {
    "connection_string": "DefaultEndpointsProtocol=https;AccountName=themoviesstorage;AccountKey=;EndpointSuffix=core.windows.net",
    # "container": "",
    # "blob": "",
    "storage_account": "themoviesstorage",
}

MYSQL_CONFIG = {
    "host": "de_mysql",
    "port": 3306,
    "database": "the_movies_db",
    "user": "root",
    "password": "root",
}

PSQL_CONFIG = {
    "host": "de_psql",
    "port": 5432,
    "database": "movies_warehouse",
    "user": "admin",
    "password": "admin123",
}

all_assets = load_assets_from_modules(
    modules=[bronze, silver, gold, warehouse],
)

defs = Definitions(
    assets=all_assets,
    resources={
        "abs_io_manager": ABSIOManager(ABS_CONFIG),
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG)
    }
)
