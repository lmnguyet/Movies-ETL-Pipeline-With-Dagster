import pandas as pd
import ast
import numpy as np

from dagster import asset, multi_asset, AssetIn, AssetOut, Output, StaticPartitionsDefinition

LAYER="silver_layer"
KEY_PREFIX=["the_movies", "silver"]
ABS_IO_MANAGER = "abs_io_manager"
UPSTREAM_KEY_PREFIX=["the_movies", "bronze"]
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1915, 2021)]
)
RATINGS = StaticPartitionsDefinition(
    [str(rate) for rate in np.arange(0,5,0.5)]
)


@multi_asset(
    group_name=LAYER,
    compute_kind="Pandas",
    ins={
        "bronze_movies_metadata": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
    outs={
        "silver_movies": AssetOut(
            description="Clean table 'movies' to store metadata of each movie.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),
        "silver_genres": AssetOut(
            description="Split table 'genres' to store data of genres.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),
        "silver_languages": AssetOut(
            description="Split table 'languages' to store data of languages.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),
        "silver_companies": AssetOut(
            description="Split table 'companies' to store data of production companies.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),

    }
)
def silver_movies_metadata(bronze_movies_metadata):
    """
        clean the table movies_metadata
    """

    df = bronze_movies_metadata.dropna(subset=["spoken_languages", "genres", "production_companies"])

    df_languages = df[["id", "spoken_languages"]]
    df_genres = df[["id", "genres"]]
    df_companies = df[["id", "production_companies"]]

    df_languages["spoken_languages"] = pd.Series([ast.literal_eval(lang) for lang in df_languages["spoken_languages"]])
    df_genres["genres"] = pd.Series([ast.literal_eval(genre) for genre in df_genres["genres"]])
    df_companies["production_companies"] = pd.Series([ast.literal_eval(comp) for comp in df_companies["production_companies"]])

    df = df.drop(["belongs_to_collection", "poster_path", "production_countries", 
                                      "status", "tagline", "video", "vote_count" 
                                       , "spoken_languages", "genres", "production_companies"
                                      ], axis=1)
    
    df.drop_duplicates(inplace=True)

    df["id"] = pd.to_numeric(df["id"], errors="coerce")


    df.dropna(subset=["id", "imdb_id", "original_language", "release_date"], inplace=True)
    # df.dropna(subset=["imdb_id"], inplace=True)
    # df.dropna(subset=["original_language"], inplace=True)
    # df.dropna(subset=["release_date"], inplace=True)


    df.drop_duplicates(subset=["id"], inplace=True)

    df["id"] = df["id"].astype("int64")
    df["adult"] = df["adult"].astype(bool)
    df["release_date"] = pd.to_datetime(df["release_date"])
    df["budget"] = pd.to_numeric(df["budget"])
    df["revenue"] = df["revenue"].astype("int64")


    df = df.rename(columns={"id": "tmdb_id", 
                            "title": "english_title", 
                            "adult": "age_restricted", 
                            "vote_average": "average_rating"
                            })

    table_names = ["silver_movies", "silver_genres", "silver_companies", "silver_languages"]
    df_list = [df, df_genres, df_companies, df_languages]

    for i in range(len(table_names)):
        yield Output(
            df_list[i],
            output_name=table_names[i],
            metadata={
                "table name": table_names[i],
                "records count": df_list[i].shape[0],
                "columns count": df_list[i].shape[1],
                "columns": df_list[i].columns.to_list(),
            }
        )
    

@asset(
    description="Create id link for movies that have reviews.",
    group_name=LAYER,
    compute_kind="Pandas",
    key_prefix=KEY_PREFIX,
    io_manager_key=ABS_IO_MANAGER,
    ins={
        "bronze_movies": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_id_links(bronze_movies):
    df = bronze_movies
    df["review_url"] = [imdb_id.split("/")[4] for imdb_id in df["review_url"]]

    df = df[["name", "year", "review_url"]]

    df.drop_duplicates(subset=["review_url"], inplace=True)

    df = df.rename(columns={"review_url": "imdb_id"})

    return Output(
        df,
        metadata={
            "table name": "silver_id_links",
            "records count": df.shape[0],
            "columns count": df.shape[1],
            "columns": df.columns.to_list(),
        }
    )

@asset(
    description="Clean table'reviews' for the movies.",
    group_name=LAYER,
    compute_kind="Pandas",
    key_prefix=KEY_PREFIX,
    io_manager_key=ABS_IO_MANAGER,
    ins={
        "bronze_reviews": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
    partitions_def=YEARLY,
)
def silver_reviews(context, bronze_reviews):
    df = pd.DataFrame()
    try:
        partition_str = context.asset_partition_key_for_output()
        partitioned_by = "year"
        df = bronze_reviews[bronze_reviews[partitioned_by]==int(partition_str)]
        # sql_stm += f" where {partitioned_by} = {partition_str}"
        context.log.info(f"Partitioning {partitioned_by} = {partition_str}")
    except Exception as e:
        context.log.error(str(e))

    if len(df) > 0:
        df.dropna(subset=["moviename", "year", "rating"], inplace=True)

        df["rating"] = pd.to_numeric(df["rating"])

    return Output(
        df,
        metadata={
            "table name": "silver_reviews",
            "records count": df.shape[0],
            "columns count": df.shape[1],
            "columns": df.columns.to_list(),
        }
    )

@multi_asset(
    description="Clean table 'credits' for the movies.",
    group_name=LAYER,
    compute_kind="Pandas",
    ins={
        "bronze_credits": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
    outs={
        "silver_crew": AssetOut(
            key_prefix=KEY_PREFIX,
            io_manager_key=ABS_IO_MANAGER,
        ),
        "silver_cast": AssetOut(
            key_prefix=KEY_PREFIX,
            io_manager_key=ABS_IO_MANAGER,
        )
    }
)
def silver_credits(bronze_credits):
    df = bronze_credits
    
    df.drop_duplicates(inplace=True)
    df.drop_duplicates(subset=["id"], inplace=True)
    
    df["crew"] = [ast.literal_eval(crew) for crew in df["crew"]]
    df["cast"] = [ast.literal_eval(cast) for cast in df["cast"]]
    
    df.drop(df[df["crew"].apply(lambda x: len(x)==0) & df["cast"].apply(lambda x: len(x)==0)].index, inplace=True)

    df = df.rename(columns={"id": "tmdb_id"})
    
    df_crew = df[["tmdb_id", "crew"]]
    df_cast = df[["tmdb_id", "cast"]]
    
    dfs = [df_crew, df_cast]
    table_names = ["silver_crew", "silver_cast"]
    for pd_data, table_name in zip(dfs, table_names):
        yield Output(
            pd_data,
            output_name=table_name,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )

@asset(
    description="Clean table 'keywords' for the movies.",
    group_name=LAYER,
    compute_kind="Pandas",
    key_prefix=KEY_PREFIX,
    io_manager_key=ABS_IO_MANAGER,
    ins={
        "bronze_keywords": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_keywords(bronze_keywords):
    df = bronze_keywords
    
    df.drop_duplicates(inplace=True)
    
    df["keywords"] = [ast.literal_eval(kw) for kw in df["keywords"]]
    
    df.drop(df[df["keywords"].apply(lambda x: len(x)==0)].index, inplace=True)

    df = df.rename(columns={"id": "tmdb_id"})

    return Output(
        df,
        metadata={
            "table name": "silver_keywords",
            "records count": df.shape[0],
            "columns count": df.shape[1],
            "columns": df.columns.to_list(),
        }
    )


@asset(
    description="Clean table 'ratings' for the movies.",
    group_name=LAYER,
    compute_kind="Pandas",
    key_prefix=KEY_PREFIX,
    io_manager_key=ABS_IO_MANAGER,
    ins={
        "bronze_ratings": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
    partitions_def=RATINGS,
)
def silver_ratings(context, bronze_ratings):
    df = bronze_ratings
    try:
        partition_str = context.asset_partition_key_for_output()
        #partitioned_by = "year"
        df = df[df['rating'] == float(partition_str)]
        # sql_stm += f" where {partitioned_by} = {partition_str}"
        context.log.info(f"Partitioning rating = {partition_str}")
    except Exception as e:
        context.log.error(str(e))

    if len(df) > 0:
        df["userId"] = df["userId"].astype(str)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

        df["rating"] = df["rating"]*2.0
        
        df = df.rename(columns={"movieId": "tmdb_id", "timestamp": "rating_date", "userId": "user_id"})

    return Output(
        df,
        metadata={
            "table name": "silver_ratings",
            "records count": df.shape[0],
            "columns count": df.shape[1],
            "columns": df.columns.to_list(),
        }
    )
