import pandas as pd
import ast
import numpy as np

from dagster import asset, multi_asset, AssetIn, AssetOut, Output, StaticPartitionsDefinition

LAYER="warehouse_layer"
KEY_PREFIX=["the_movies", "warehouse"]
PSQL_IO_MANAGER = "psql_io_manager"
UPSTREAM_KEY_PREFIX=["the_movies", "gold"]
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1915, 2021)]
)
RATINGS = StaticPartitionsDefinition(
    [str(rate) for rate in np.arange(0,5,0.5)]
)


@asset(
    description="Load the table 'movies' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_movies": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_movies(gold_movies):
    return Output(
        gold_movies,
        metadata={
            "table name": "warehouse_movies",
            "records count": gold_movies.shape[0],
            "columns count": gold_movies.shape[1],
            "columns": gold_movies.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'movies_genres' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_movies_genres": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_movies_genres(gold_movies_genres):
    return Output(
        gold_movies_genres,
        metadata={
            "table name": "warehouse_movies_genres",
            "records count": gold_movies_genres.shape[0],
            "columns count": gold_movies_genres.shape[1],
            "columns": gold_movies_genres.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'genres' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_genres": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_genres(gold_genres):
    return Output(
        gold_genres,
        metadata={
            "table name": "warehouse_genres",
            "records count": gold_genres.shape[0],
            "columns count": gold_genres.shape[1],
            "columns": gold_genres.columns.to_list(),
        }
    )


@asset(
    description="Load the table 'movies_production' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_movies_production": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_movies_production(gold_movies_production):
    return Output(
        gold_movies_production,
        metadata={
            "table name": "warehouse_movies_production",
            "records count": gold_movies_production.shape[0],
            "columns count": gold_movies_production.shape[1],
            "columns": gold_movies_production.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'companies' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_companies": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_companies(gold_companies):
    return Output(
        gold_companies,
        metadata={
            "table name": "warehouse_companies",
            "records count": gold_companies.shape[0],
            "columns count": gold_companies.shape[1],
            "columns": gold_companies.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'languages' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_languages": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_languages(gold_languages):
    return Output(
        gold_languages,
        metadata={
            "table name": "warehouse_languages",
            "records count": gold_languages.shape[0],
            "columns count": gold_languages.shape[1],
            "columns": gold_languages.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'keywords' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_keywords": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_keywords(gold_keywords):
    return Output(
        gold_keywords,
        metadata={
            "table name": "warehouse_keywords",
            "records count": gold_keywords.shape[0],
            "columns count": gold_keywords.shape[1],
            "columns": gold_keywords.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'movies_keywords' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_movies_keywords": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_movies_keywords(gold_movies_keywords):
    return Output(
        gold_movies_keywords,
        metadata={
            "table name": "warehouse_movies_keywords",
            "records count": gold_movies_keywords.shape[0],
            "columns count": gold_movies_keywords.shape[1],
            "columns": gold_movies_keywords.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'movies_cast' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_movies_cast": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_movies_cast(gold_movies_cast):
    return Output(
        gold_movies_cast,
        metadata={
            "table name": "warehouse_movies_cast",
            "records count": gold_movies_cast.shape[0],
            "columns count": gold_movies_cast.shape[1],
            "columns": gold_movies_cast.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'actors' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_actors": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_actors(gold_actors):
    return Output(
        gold_actors,
        metadata={
            "table name": "warehouse_actors",
            "records count": gold_actors.shape[0],
            "columns count": gold_actors.shape[1],
            "columns": gold_actors.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'movies_crew' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_movies_crew": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_movies_crew(gold_movies_crew):
    return Output(
        gold_movies_crew,
        metadata={
            "table name": "warehouse_movies_crew",
            "records count": gold_movies_crew.shape[0],
            "columns count": gold_movies_crew.shape[1],
            "columns": gold_movies_crew.columns.to_list(),
        }
    )

@asset(
    description="Load the table 'staff' to the warehouse.",
    group_name=LAYER,
    io_manager_key=PSQL_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="PostgreSQL",
    ins={
        "gold_staff": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def warehouse_staff(gold_staff):
    return Output(
        gold_staff,
        metadata={
            "table name": "warehouse_staff",
            "records count": gold_staff.shape[0],
            "columns count": gold_staff.shape[1],
            "columns": gold_staff.columns.to_list(),
        }
    )

