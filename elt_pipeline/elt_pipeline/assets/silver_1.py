from dagster import file_relative_path, AssetIn, asset, Output
# from dagstermill import define_dagstermill_asset
# from pyspark.sql import SparkSession
import datetime
import polars as pl
import time
import ast
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# path = "/opt/dagster/notebook/silver_movies_metadata.ipynb"

LAYER="silver_layer"
KEY_PREFIX=["the_movies", "silver"]
ABS_IO_MANAGER = "abs_io_manager"
UPSTREAM_KEY_PREFIX=["the_movies", "bronze"]

@asset(
    description="Create table 'movies_keywords' for each movie.",
    key_prefix=KEY_PREFIX,
    group_name=LAYER,
    io_manager_key=ABS_IO_MANAGER,
    compute_kind="Pandas",
    ins={
        "bronze_keywords": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_movies_keywords(context, bronze_keywords):
    table_name = "silver_movies_keywords"
    pd_data = pd.DataFrame(columns=["tmdb_id", "keyword_id"])
    t_start = time.time()
    try:
        i = 0
        while True:
            df = next(bronze_keywords)
            silver_movies_keywords = pd.DataFrame(columns=["tmdb_id", "keyword_id"])
            batch_tstart = time.time()
            for index, row in df.iterrows():
                tmdb_id = row["id"]
                keywords_str = row["keywords"]
                keywords_list = ast.literal_eval(keywords_str)
                for keyword in keywords_list:
                    silver_movies_keywords.loc[len(silver_movies_keywords)] = [tmdb_id, keyword["id"]]
            pd_data = pd.concat([pd_data, silver_movies_keywords], axis=0, ignore_index=True)
            i += 1
            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
    except StopIteration:
        pd_data.sort_values(by=["tmdb_id"], inplace=True)
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))
        
@asset(
    description="Create table 'keywords' for each keyword.",
    key_prefix=KEY_PREFIX,
    group_name=LAYER,
    io_manager_key=ABS_IO_MANAGER,
    compute_kind="Pandas",
    ins={
        "bronze_keywords": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_keywords(context, bronze_keywords):
    table_name = "silver_keywords"
    # silver_keywords = set()

    t_start = time.time()

    pd_data = pd.DataFrame(columns=["keyword_id", "keyword"])
    try:
        i = 0
        while True:
            batch_tstart = time.time()

            df = next(bronze_keywords)

            for index, row in df.iterrows():
                keywords_str = row["keywords"]
                keywords_list = ast.literal_eval(keywords_str)
                for keyword in keywords_list:
                    if pd_data.isin([keyword["id"]]).any().any():
                        continue
                    # silver_keywords.add(keyword)

                    pd_data.loc[len(pd_data)] = [keyword["id"], keyword["name"]] 
            # pd_data = pd.concat([pd_data, silver_movies_keywords], axis=0, ignore_index=True)
            i += 1
            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
    except StopIteration:
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        pd_data.sort_values(by=["keyword_id"], inplace=True)
        context.log.info(f"Check duplicated keywords: {pd_data.duplicated().any()}")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'actors' for each actor.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_credits": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_actors(context, bronze_credits):
    table_name = "silver_actors"

    pd_data = pd.DataFrame(columns=["actor_id", "name", "gender", "profile_path"])

    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_credits)

            for row in df["cast"]:
                cast_list = ast.literal_eval(row)

                for cast in cast_list:
                    if pd_data.isin([cast["id"]]).any().any():
                        continue
                    pd_data.loc[len(pd_data)] = [cast["id"], cast["name"], cast["gender"], cast["profile_path"]]

            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1    
    except StopIteration:
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        pd_data.sort_values(by=["actor_id"], inplace=True)
        context.log.info(f"Check duplicated actors: {pd_data.duplicated(['actor_id']).any()}")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'movies_cast' for each movie.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_credits": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_movies_cast(context, bronze_credits):
    table_name = "silver_movies_cast"

    pd_data = pd.DataFrame(columns=["tmdb_id", "credit_id", "actor_id", "character"])

    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_credits)

            silver_movies_cast = pd.DataFrame(columns=["tmdb_id", "credit_id", "actor_id", "character"])

            for index, row in df.iterrows():
                tmdb_id = row["id"]
                cast_str = row["cast"]
                cast_list = ast.literal_eval(cast_str)

                for cast in cast_list:
                    silver_movies_cast.loc[len(silver_movies_cast)] = [tmdb_id, cast["credit_id"], cast["id"], cast["character"]]

            pd_data = pd.concat([pd_data, silver_movies_cast], axis=0, ignore_index=True)

            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1    
    except StopIteration:
        pd_data.sort_values(by=["tmdb_id"], inplace=True)
        context.log.info(f"Check duplicated rows: {pd_data.duplicated().any()}")
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'staff' for each crew member.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_credits": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_staff(context, bronze_credits):
    table_name = "silver_staff"

    pd_data = pd.DataFrame(columns=["staff_id", "name", "gender", "profile_path"])

    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_credits)

            silver_staff = pd.DataFrame(columns=["staff_id", "name", "gender", "profile_path"])

            for row in df["crew"]:
                staff_list = ast.literal_eval(row)

                for staff in staff_list:
                    if pd_data.isin([staff["id"]]).any().any():
                        continue
                    silver_staff.loc[len(silver_staff)] = [staff["id"], staff["name"], staff["gender"], staff["profile_path"]]

            pd_data = pd.concat([pd_data, silver_staff], axis=0, ignore_index=True)

            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1    
    except StopIteration:
        pd_data.sort_values(by=["staff_id"], inplace=True)
        context.log.info(f"Check duplicated staff: {pd_data.duplicated(['staff_id']).any()}")
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'movies_crew' for each movie.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_credits": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_movies_crew(context, bronze_credits):
    table_name = "silver_movies_crew"

    pd_data = pd.DataFrame(columns=["tmdb_id", "credit_id", "staff_id", "job"])

    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_credits)

            for index, row in df.iterrows():
                tmdb_id = row["id"]
                crew_str = row["crew"]
                crew_list = ast.literal_eval(crew_str)

                for staff in crew_list:
                    pd_data.loc[len(pd_data)] = [tmdb_id, staff["credit_id"], staff["id"], staff["job"]]

            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1    
    except StopIteration:
        pd_data.sort_values(by=["tmdb_id"], inplace=True)
        context.log.info(f"Check duplicated rows: {pd_data.duplicated().any()}")
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'genres' for each genre.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_movies_metadata": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_genres(context, bronze_movies_metadata):
    table_name = "silver_genres"
    
    schema = pa.schema([
        ("genre_id", pa.int64()),
        ("name", pa.string())
    ])
    
    columns = ["genre_id", "name"]
    
    pd_data = pa.Table.from_pandas(pd.DataFrame(columns=columns), schema=schema)

    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_movies_metadata)
            
            for row in df["genres"]:
                genres_list = ast.literal_eval(row)

                for genre in genres_list:
                    if pd_data.filter(pc.field("genre_id") == genre["id"]).num_rows > 0:
                        continue

                    record = pa.Table.from_pandas(
                        pd.DataFrame([[genre["id"], genre["name"]]], columns=columns),
                        schema=schema
                    )
            
                    pd_data = pa.concat_tables([pd_data, record])
    
            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1
    except StopIteration:
        pd_data = pd_data.to_pandas()
        pd_data.sort_values(by=["genre_id"], inplace=True)
        context.log.info(f"Check duplicated genres: {pd_data.duplicated(['genre_id']).any()}")
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'movies_genres' for each movie.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_movies_metadata": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_movies_genres(context, bronze_movies_metadata):
    table_name = "silver_movies_genres"
    
    schema = pa.schema([
        ("tmdb_id", pa.int64()),
        ("genre_id", pa.int64())
    ])
    columns = ["tmdb_id","genre_id"]
    
    pd_data = pa.Table.from_pandas(pd.DataFrame(columns=columns), schema=schema)
    
    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_movies_metadata)

            for index, row in df.iterrows():
                tmdb_id = int(row["id"])
                genres_list = ast.literal_eval(row["genres"])

                for genre in genres_list:
                    record = pa.Table.from_pandas(
                        pd.DataFrame([[tmdb_id, genre["id"]]], columns=columns),
                        schema=schema
                    )
                    pd_data = pa.concat_tables([pd_data, record])

            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1    
    except StopIteration:
        pd_data.sort_values(by=["tmdb_id"], inplace=True)
        context.log.info(f"Check duplicated rows: {pd_data.duplicated().any()}")
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'companies' for each production company.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_movies_metadata": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_companies(context, bronze_movies_metadata):
    table_name = "silver_companies"

    pd_data = pd.DataFrame(columns=["comp_id", "name"])

    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_movies_metadata)

            for row in df["production_companies"]:
                comps_list = ast.literal_eval(row)

                for comp in comps_list:
                    if pd_data.isin([comp["id"]]).any().any():
                        continue
                    pd_data.loc[len(pd_data)] = [comp["id"], comp["name"]]

            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1    
    except StopIteration:
        pd_data.sort_values(by=["comp_id"], inplace=True)
        context.log.info(f"Check duplicated companies: {pd_data.duplicated(['comp_id']).any()}")
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'movies_production' for each movie.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_movies_metadata": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_movies_production(context, bronze_movies_metadata):
    table_name = "silver_movies_production"

    pd_data = pd.DataFrame(columns=["tmdb_id", "comp_id"])

    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_movies_metadata)

            for index, row in df.iterrows():
                tmdb_id = row["id"]
                comps_list = ast.literal_eval(row["production_companies"])

                for comp in comps_list:
                    pd_data.loc[len(pd_data)] = [tmdb_id, comp["id"]]

            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1    
    except StopIteration:
        pd_data.sort_values(by=["tmdb_id"], inplace=True)
        context.log.info(f"Check duplicated rows: {pd_data.duplicated().any()}")
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'languages' for each production company.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_movies_metadata": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_companies(context, bronze_movies_metadata):
    table_name = "silver_languages"

    pd_data = pd.DataFrame(columns=["iso_code", "name"])

    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_movies_metadata)

            for row in df["spoken_languages"]:
                langs_list = ast.literal_eval(row)

                for lang in langs_list:
                    if pd_data.isin([lang["iso_639_1"]]).any().any():
                        continue
                    pd_data.loc[len(pd_data)] = [lang["iso_639_1"], lang["name"]]

            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1    
    except StopIteration:
        pd_data.sort_values(by=["iso_code"], inplace=True)
        context.log.info(f"Check duplicated languages: {pd_data.duplicated(['iso_code']).any()}")
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))

@asset(
    description="Create table 'movies' to store metadata of each movie.",
    io_manager_key=ABS_IO_MANAGER,
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="Pandas",
    ins={
        "bronze_movies_metadata": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def silver_movies(context, bronze_movies_metadata):
    table_name = "silver_movies"

    pd_data = pd.DataFrame(columns=["tmdb_id", "imdb_id", "original_title", "english_title", "original_language", "age_restricted", "runtime",
                                    "overview", "homepage", "release_date", "budget", "revenue", "average_rating"])

    t_start = time.time()

    try:
        i = 1
        while True:
            batch_tstart = time.time()

            df = next(bronze_movies_metadata)

            df.dropna(subset=["imdb_id", "original_language", "production_companies", "release_date", "title", "vote_average"], inplace=True)

            for index, row in df.iterrows():
                tmdb_id = row["id"]
                imdb_id = row["imdb_id"]
                original_title = row["original_title"]
                english_title = row["title"]
                original_language = row["original_language"]
                age_restricted = row["adult"]
                runtime = row["runtime"]
                overview = row["overview"]
                homepage = row["homepage"]
                release_date = row["release_date"]
                budget = row["budget"]
                revenue = row["revenue"]
                average_rating = row["vote_average"]

                pd_data.loc[len(pd_data)] = [tmdb_id, imdb_id, original_title, english_title, original_language, age_restricted, runtime,
                                            overview, homepage, release_date, budget, revenue, average_rating]

            context.log.info(f"Completed batch {i} in: {time.time() - batch_tstart} seconds.")
            i += 1    
    except StopIteration:
        pd_data["release_date"] = pd.to_datetime(pd_data["release_date"], errors="coerce")
        #pd_data.dropna()
        pd_data.sort_values(by=["tmdb_id"], inplace=True)
        context.log.info(f"Check duplicated movies: {pd_data.duplicated(['tmdb_id']).any()}")
        context.log.info(f"Completed transforming in: {time.time() - t_start} seconds.")
        return Output(
            pd_data,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list(),
            }
        )
    except Exception as e:
        context.log.error(str(e))